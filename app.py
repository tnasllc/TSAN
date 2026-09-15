import os, re, time, threading
from collections import defaultdict, deque
from datetime import datetime, timezone

import pandas as pd
import streamlit as st
from alpaca.data.live import StockDataStream, NewsDataStream
from alpaca.data.enums import DataFeed

# -----------------------------
# CONFIG
# -----------------------------
KEY = os.getenv("APCA_API_KEY_ID", "")
SECRET = os.getenv("APCA_API_SECRET_KEY", "")
FEED = os.getenv("ALPACA_FEED", "iex").lower()

USD_MIN = float(os.getenv("USD_MIN", "2"))
USD_MAX = float(os.getenv("USD_MAX", "10"))
MIN_VOL = int(os.getenv("MIN_VOL", "10000000"))
MIN_SCORE = int(os.getenv("MIN_SCORE", "55"))

# Optional FX rates for non-USD exchanges.
# In production replace this with a live FX adapter.
FX_TO_USD = {
    "USD": 1.0,
    "CAD": float(os.getenv("FX_CAD_USD", "0.74")),
    "GBP": float(os.getenv("FX_GBP_USD", "1.35")),
    "EUR": float(os.getenv("FX_EUR_USD", "1.18")),
    "AUD": float(os.getenv("FX_AUD_USD", "0.66")),
    "JPY": float(os.getenv("FX_JPY_USD", "0.0068")),
    "HKD": float(os.getenv("FX_HKD_USD", "0.128")),
    "SGD": float(os.getenv("FX_SGD_USD", "0.78")),
}

MARKETS = {
    "US": {"currency":"USD", "enabled": True, "provider":"Alpaca"},
    "Canada": {"currency":"CAD", "enabled": True, "provider":"Adapter"},
    "UK": {"currency":"GBP", "enabled": True, "provider":"Adapter"},
    "Europe": {"currency":"EUR", "enabled": True, "provider":"Adapter"},
    "Australia": {"currency":"AUD", "enabled": True, "provider":"Adapter"},
    "Japan": {"currency":"JPY", "enabled": True, "provider":"Adapter"},
    "Hong Kong": {"currency":"HKD", "enabled": True, "provider":"Adapter"},
    "Singapore": {"currency":"SGD", "enabled": True, "provider":"Adapter"},
}

state = defaultdict(lambda: {
    "market":"US", "currency":"USD", "local_price":None, "usd_price":None,
    "volume":0, "minute_vols":deque(maxlen=60), "headline":"",
    "news_time":None, "url":"", "score":0, "catalyst":"Other", "risk":"",
    "source":"", "pct":None
})
lock = threading.Lock()

CATALYSTS = {
 "FDA/Regulatory": (30, r"\bfda\b|ema\b|mhra\b|health canada|tga\b|pmda\b|nmpa\b|approval|approved|breakthrough|fast track|pdufa|orphan drug"),
 "Clinical": (27, r"phase [123]|clinical trial|topline|endpoint|patient enrollment|positive data|met primary endpoint"),
 "Contract": (25, r"contract|awarded|purchase order|government award|framework agreement|task order"),
 "Partnership": (21, r"partnership|partnered|collaboration|strategic alliance|license agreement|joint venture"),
 "Earnings/Guidance": (23, r"earnings|revenue|eps\b|guidance|raises outlook|raises forecast|profit|ebitda"),
 "M&A": (30, r"acquire|acquisition|merger|takeover|scheme of arrangement|definitive agreement"),
 "Insider": (20, r"insider|director purchased|ceo purchased|cfo purchased|open-market purchase|substantial shareholder"),
 "Analyst": (16, r"upgrade|price target|initiated.*buy|outperform|overweight|conviction buy"),
 "Financing Positive": (10, r"non-dilutive|grant funding|strategic investment|cornerstone investment"),
}
RISKS = {
 "Offering/Dilution": r"public offering|registered direct|private placement|atm\b|at-the-market|warrant|convertible|rights offering|placement",
 "Reverse split": r"reverse stock split|reverse split|share consolidation",
 "Going concern": r"going concern",
 "Delisting": r"delisting|non-compliance|minimum bid",
}

def classify(text):
    t = (text or "").lower()
    best = ("Other", 5)
    for name, (pts, pat) in CATALYSTS.items():
        if re.search(pat, t, re.I) and pts > best[1]:
            best = (name, pts)
    risks = [name for name, pat in RISKS.items() if re.search(pat, t, re.I)]
    return best[0], best[1], ", ".join(risks)

def to_usd(local_price, currency):
    rate = FX_TO_USD.get(currency, 1.0)
    return None if local_price is None else local_price * rate

def volume_accel(minute_vols):
    mv = list(minute_vols)
    if len(mv) < 8: return 1.0
    recent = sum(mv[-3:]) / 3
    base = max(1, sum(mv[:-3]) / max(1, len(mv)-3))
    return recent / base

def score_row(s):
    score = 0
    if s["usd_price"] is not None and USD_MIN <= s["usd_price"] <= USD_MAX:
        score += 15
    if s["volume"] >= MIN_VOL:
        score += 20
    accel = volume_accel(s["minute_vols"])
    if accel >= 5: score += 25
    elif accel >= 3: score += 20
    elif accel >= 2: score += 12

    cat, pts, risk = classify(s["headline"])
    score += pts

    if s["news_time"]:
        age = (datetime.now(timezone.utc)-s["news_time"]).total_seconds()/60
        if age <= 5: score += 18
        elif age <= 15: score += 14
        elif age <= 30: score += 10
        elif age <= 60: score += 5

    if risk: score -= 15
    s["catalyst"], s["risk"], s["score"] = cat, risk, max(0, min(100, score))

def upsert_market_event(symbol, market, currency, local_price=None, volume_delta=0,
                        headline=None, news_time=None, url=None, source=None):
    """Generic adapter entrypoint for any country/provider."""
    with lock:
        s = state[symbol]
        s["market"] = market
        s["currency"] = currency
        if local_price is not None:
            s["local_price"] = float(local_price)
            s["usd_price"] = to_usd(float(local_price), currency)
        if volume_delta:
            s["volume"] += int(volume_delta)
            s["minute_vols"].append(int(volume_delta))
        if headline is not None:
            s["headline"] = headline
        if news_time is not None:
            s["news_time"] = news_time
        if url is not None:
            s["url"] = url
        if source is not None:
            s["source"] = source
        score_row(s)

# -----------------------------
# U.S. LIVE ADAPTER (Alpaca)
# -----------------------------
async def on_bar(bar):
    upsert_market_event(
        symbol=bar.symbol, market="US", currency="USD",
        local_price=float(bar.close), volume_delta=int(bar.volume),
        source="Alpaca"
    )

async def on_news(news):
    headline = getattr(news, "headline", "") or ""
    symbols = getattr(news, "symbols", []) or []
    created = getattr(news, "created_at", None) or datetime.now(timezone.utc)
    if created.tzinfo is None:
        created = created.replace(tzinfo=timezone.utc)
    url = getattr(news, "url", "") or ""
    for sym in symbols:
        upsert_market_event(
            symbol=sym, market="US", currency="USD",
            headline=headline, news_time=created, url=url, source="Alpaca News"
        )

def run_us_streams():
    if not KEY or not SECRET:
        return
    feed = DataFeed.SIP if FEED == "sip" else DataFeed.IEX
    stock = StockDataStream(KEY, SECRET, feed=feed)
    news = NewsDataStream(KEY, SECRET)
    stock.subscribe_bars(on_bar, "*")
    news.subscribe_news(on_news, "*")
    threading.Thread(target=stock.run, daemon=True).start()
    threading.Thread(target=news.run, daemon=True).start()

# -----------------------------
# GLOBAL ADAPTER PLACEHOLDERS
# -----------------------------
# Plug providers here (examples):
# - Canada: TSX/TSXV provider
# - UK/Europe: LSE / Euronext / Xetra provider
# - Australia: ASX provider
# - Japan: TSE provider
# - Hong Kong: HKEX provider
# - Singapore: SGX provider
#
# Each adapter should call upsert_market_event(...).
#
# Example:
# upsert_market_event("ABC.TO","Canada","CAD",local_price=5.20,volume_delta=250000,
#                     headline="Company wins C$50M contract",
#                     news_time=datetime.now(timezone.utc),source="YourProvider")

# -----------------------------
# DASHBOARD
# -----------------------------
st.set_page_config(page_title="Global Catalyst + Volume Scanner", layout="wide")
st.title("🌍⚡ Global Catalyst + Volume Scanner")
st.caption("Scans catalyst + volume setups across multiple countries. Non-U.S. feeds require exchange/provider adapters.")

if "started" not in st.session_state:
    run_us_streams()
    st.session_state.started = True

st.sidebar.header("Filters")
countries = st.sidebar.multiselect(
    "Markets",
    list(MARKETS.keys()),
    default=list(MARKETS.keys())
)
usd_min = st.sidebar.number_input("Min USD-equivalent price", value=USD_MIN)
usd_max = st.sidebar.number_input("Max USD-equivalent price", value=USD_MAX)
vmin = st.sidebar.number_input("Min session volume", value=MIN_VOL, step=1000000)
smin = st.sidebar.number_input("Min catalyst score", value=MIN_SCORE)

rows=[]
with lock:
    for sym,s in state.items():
        if s["market"] not in countries: continue
        if s["usd_price"] is None: continue
        if not (usd_min <= s["usd_price"] <= usd_max): continue
        if s["volume"] < vmin and s["score"] < smin: continue
        age = None
        if s["news_time"]:
            age = round((datetime.now(timezone.utc)-s["news_time"]).total_seconds()/60,1)
        rows.append({
            "Ticker": sym,
            "Market": s["market"],
            "Local Price": round(s["local_price"],4) if s["local_price"] is not None else None,
            "Currency": s["currency"],
            "USD Price": round(s["usd_price"],2) if s["usd_price"] is not None else None,
            "Volume": s["volume"],
            "Vol Accel": round(volume_accel(s["minute_vols"]),2),
            "Score": s["score"],
            "Catalyst": s["catalyst"],
            "News Age (min)": age,
            "Risk": s["risk"],
            "Headline": s["headline"],
            "Source": s["source"],
            "Link": s["url"]
        })

df=pd.DataFrame(rows)
if len(df):
    df=df.sort_values(["Score","Volume"],ascending=False)
    st.dataframe(
        df, use_container_width=True, hide_index=True,
        column_config={"Link": st.column_config.LinkColumn("News")}
    )
else:
    st.info("Listening for qualifying global price, volume and catalyst events…")

st.markdown("""
### Global coverage architecture
**Live now:** U.S. via Alpaca.  
**Ready for adapters:** Canada, UK, Europe, Australia, Japan, Hong Kong, Singapore.

For each non-U.S. exchange, connect a licensed real-time market-data/news source and send normalized events into `upsert_market_event()`.
The scanner automatically converts local prices into USD-equivalent values for the $2–$10 filter.
""")

time.sleep(5)
st.rerun()
