import os, re, time, threading
from collections import defaultdict, deque
from datetime import datetime, timezone

import pandas as pd
import streamlit as st
from alpaca.data.live import StockDataStream, NewsDataStream
from alpaca.data.enums import DataFeed

st.set_page_config(page_title='Global Catalyst + Volume Scanner', layout='wide')

def get_secret(name, default=''):
    try:
        return str(st.secrets.get(name, os.getenv(name, default)))
    except Exception:
        return os.getenv(name, default)

KEY = get_secret('APCA_API_KEY_ID')
SECRET = get_secret('APCA_API_SECRET_KEY')
FEED = get_secret('ALPACA_FEED', 'iex').lower()
USD_MIN, USD_MAX = 2.0, 10.0

# Optional verified float source. Do not guess float when unavailable.
# Add entries as: 'SOUN': 123456789
VERIFIED_FLOATS = {}

MARKETS = {'US':'USD','Canada':'CAD','UK':'GBP','Europe':'EUR','Australia':'AUD','Japan':'JPY','Hong Kong':'HKD','Singapore':'SGD'}
FX_TO_USD = {'USD':1.0,'CAD':0.74,'GBP':1.35,'EUR':1.18,'AUD':0.66,'JPY':0.0068,'HKD':0.128,'SGD':0.78}

state = defaultdict(lambda: {
    'market':'US','currency':'USD','local_price':None,'usd_price':None,'prev_close':None,
    'volume':0,'minute_vols':deque(maxlen=60),'history_vols':deque(maxlen=20),
    'headline':'','news_time':None,'url':'','score':0,'catalyst':'Other','risk':'','source':'',
    'float':None,'last_event':None
})
lock = threading.Lock()
feed_status = {'started':False,'last_event':None,'error':''}

CATALYSTS = {
 'FDA/Regulatory': (30, r'\bfda\b|ema\b|mhra\b|health canada|tga\b|pmda\b|approval|approved|breakthrough|fast track|pdufa|orphan drug|rmat'),
 'Clinical': (27, r'phase [123]|clinical trial|topline|endpoint|patient enrollment|positive data|met primary endpoint'),
 'Contract': (25, r'contract|awarded|purchase order|government award|framework agreement|task order'),
 'Partnership': (21, r'partnership|partnered|collaboration|strategic alliance|license agreement|joint venture'),
 'Earnings/Guidance': (23, r'earnings|revenue|eps\b|guidance|raises outlook|raises forecast|profit|ebitda'),
 'M&A': (30, r'acquire|acquisition|merger|takeover|definitive agreement'),
 'Insider': (20, r'insider|director purchased|ceo purchased|cfo purchased|open-market purchase'),
 'Analyst': (16, r'upgrade|price target|initiated.*buy|outperform|overweight'),
 'Financing Positive': (10, r'non-dilutive|grant funding|strategic investment')
}
RISKS = {
 'Offering/Dilution': r'public offering|registered direct|private placement|atm\b|at-the-market|warrant|convertible|rights offering',
 'Reverse split': r'reverse stock split|reverse split|share consolidation',
 'Going concern': r'going concern', 'Delisting': r'delisting|non-compliance|minimum bid'
}

def classify(text):
    t=(text or '').lower(); best=('Other',5)
    for name,(pts,pat) in CATALYSTS.items():
        if re.search(pat,t,re.I) and pts>best[1]: best=(name,pts)
    risks=[name for name,pat in RISKS.items() if re.search(pat,t,re.I)]
    return best[0],best[1],', '.join(risks)

def volume_accel(v):
    x=list(v)
    if len(x)<6: return 1.0
    recent=sum(x[-3:])/3; base=max(1,sum(x[:-3])/max(1,len(x)-3))
    return recent/base

def rvol(s):
    # Intraday proxy until a full same-time-of-day historical baseline provider is connected.
    x=list(s['minute_vols'])
    if len(x)<6: return None
    recent=sum(x[-3:])/3; prior=x[:-3]
    base=sum(prior)/len(prior) if prior else 0
    return recent/base if base>0 else None

def pct_change(s):
    if s['local_price'] is None or not s['prev_close']: return None
    return (s['local_price']/s['prev_close']-1)*100

def news_age(s):
    if not s['news_time']: return None
    return max(0,(datetime.now(timezone.utc)-s['news_time']).total_seconds()/60)

def float_rotation(s):
    return (s['volume']/s['float']) if s.get('float') else None

def session_label():
    # Approximate ET session using UTC; DST-safe precision is not needed for display classification.
    try:
        from zoneinfo import ZoneInfo
        et=datetime.now(ZoneInfo('America/New_York'))
        mins=et.hour*60+et.minute
        if 240<=mins<570: return 'Premarket'
        if 570<=mins<960: return 'Regular'
        if 960<=mins<1200: return 'After-hours'
        return 'Closed/Overnight'
    except Exception: return 'Unknown'

def score_row(s):
    score=0
    if s['usd_price'] is not None and 2<=s['usd_price']<=10: score+=15
    vol=s['volume']; rv=rvol(s); accel=volume_accel(s['minute_vols'])
    if vol>=10_000_000: score+=20
    elif vol>=3_000_000: score+=15
    elif vol>=1_000_000: score+=10
    elif vol>=250_000: score+=5
    if rv is not None:
        if rv>=8: score+=22
        elif rv>=5: score+=18
        elif rv>=3: score+=13
        elif rv>=2: score+=8
    if accel>=5: score+=15
    elif accel>=3: score+=10
    elif accel>=2: score+=6
    cat,pts,risk=classify(s['headline']); score+=pts
    age=news_age(s)
    if age is not None:
        if age<=5: score+=18
        elif age<=15: score+=14
        elif age<=30: score+=10
        elif age<=60: score+=5
    rot=float_rotation(s)
    if rot is not None:
        if rot>=1: score+=12
        elif rot>=0.5: score+=8
        elif rot>=0.2: score+=4
    if risk: score-=15
    s['catalyst'],s['risk'],s['score']=cat,risk,max(0,min(100,score))

def signal(s):
    sc=s['score']; rv=rvol(s); age=news_age(s); pc=pct_change(s)
    fresh=age is not None and age<=60 and s['catalyst']!='Other'
    if sc>=85 and (rv or 0)>=5 and (pc is None or pc>=5): return '🔥 BREAKOUT'
    if sc>=70 and ((rv or 0)>=3 or s['volume']>=3_000_000): return '🟢 CONFIRMED'
    if fresh and sc>=45: return '🟡 EARLY ALERT'
    return '👀 WATCH'

def upsert_market_event(symbol, market='US', currency='USD', local_price=None, volume_delta=0, headline=None, news_time=None, url=None, source=None, prev_close=None):
    with lock:
        s=state[symbol]; s['market']=market; s['currency']=currency
        if local_price is not None:
            s['local_price']=float(local_price); s['usd_price']=float(local_price)*FX_TO_USD.get(currency,1.0)
        if prev_close: s['prev_close']=float(prev_close)
        if volume_delta:
            s['volume']+=int(volume_delta); s['minute_vols'].append(int(volume_delta))
        if headline is not None: s['headline']=headline
        if news_time is not None: s['news_time']=news_time
        if url is not None: s['url']=url
        if source is not None: s['source']=source
        if symbol in VERIFIED_FLOATS: s['float']=VERIFIED_FLOATS[symbol]
        s['last_event']=datetime.now(timezone.utc); feed_status['last_event']=s['last_event']
        score_row(s)

async def on_bar(bar):
    upsert_market_event(bar.symbol, local_price=float(bar.close), volume_delta=int(bar.volume), source='Alpaca')

async def on_news(news):
    created=getattr(news,'created_at',None) or datetime.now(timezone.utc)
    if created.tzinfo is None: created=created.replace(tzinfo=timezone.utc)
    for sym in (getattr(news,'symbols',[]) or []):
        upsert_market_event(sym, headline=getattr(news,'headline','') or '', news_time=created, url=getattr(news,'url','') or '', source='Alpaca News')

def run_us_streams():
    if not KEY or not SECRET: return
    try:
        feed=DataFeed.SIP if FEED=='sip' else DataFeed.IEX
        stock=StockDataStream(KEY,SECRET,feed=feed); news=NewsDataStream(KEY,SECRET)
        stock.subscribe_bars(on_bar,'*'); news.subscribe_news(on_news,'*')
        threading.Thread(target=stock.run,daemon=True).start(); threading.Thread(target=news.run,daemon=True).start()
        feed_status['started']=True
    except Exception as e: feed_status['error']=str(e)

st.title('🌍⚡ Global Catalyst + Volume Scanner — Pro')
st.caption('Early catalyst + momentum scanner. U.S. live via Alpaca; international markets require licensed provider adapters.')
if KEY and SECRET: st.success('🟢 ALPACA CREDENTIALS LOADED')
else: st.error('🔴 ALPACA DISCONNECTED — credentials not found')

if 'started' not in st.session_state:
    run_us_streams(); st.session_state.started=True

c1,c2,c3=st.columns(3)
c1.metric('U.S. feed', 'STARTED' if feed_status['started'] else 'NOT STARTED')
c2.metric('Session', session_label())
last=feed_status['last_event']
c3.metric('Last event', last.strftime('%H:%M:%S UTC') if last else 'Waiting…')
if feed_status['error']: st.error(feed_status['error'])

st.sidebar.header('Filters')
countries=st.sidebar.multiselect('Markets',list(MARKETS.keys()),default=['US'])
usd_min=st.sidebar.number_input('Min USD-equivalent price',value=2.0,min_value=0.0,step=0.25)
usd_max=st.sidebar.number_input('Max USD-equivalent price',value=10.0,min_value=0.0,step=0.25)
vmin=st.sidebar.number_input('Minimum volume',value=250000,min_value=0,step=250000)
rmin=st.sidebar.number_input('Minimum RVOL',value=1.0,min_value=0.0,step=0.5)
smin=st.sidebar.number_input('Minimum catalyst score',value=40,min_value=0,max_value=100,step=5)
show_watch=st.sidebar.checkbox('Include WATCH signals',value=False)

rows=[]
with lock:
    for sym,s in state.items():
        if s['market'] not in countries or s['usd_price'] is None or not (usd_min<=s['usd_price']<=usd_max): continue
        rv=rvol(s); sig=signal(s)
        # Fresh catalyst can surface before the volume threshold.
        fresh=(news_age(s) is not None and news_age(s)<=60 and s['catalyst']!='Other')
        if not fresh and s['volume']<vmin: continue
        if rv is not None and rv<rmin and not fresh: continue
        if s['score']<smin: continue
        if sig=='👀 WATCH' and not show_watch: continue
        pc=pct_change(s); rot=float_rotation(s); age=news_age(s)
        rows.append({
            'Signal':sig,'Ticker':sym,'Market':s['market'],'Price':round(s['usd_price'],2),
            '% Change':round(pc,2) if pc is not None else None,'Volume':s['volume'],
            'RVOL':round(rv,2) if rv is not None else None,
            'Float':int(s['float']) if s.get('float') else None,
            'Float Rotation':round(rot,2) if rot is not None else None,
            'Catalyst':s['catalyst'],'News Age (min)':round(age,1) if age is not None else None,
            'Score':s['score'],'Risk':s['risk'],'Headline':s['headline'],'Source':s['source'],'News':s['url']
        })

df=pd.DataFrame(rows)
if len(df):
    df=df.sort_values(['Score','RVOL','Volume'],ascending=False,na_position='last')
    st.dataframe(df,use_container_width=True,hide_index=True,column_config={'News':st.column_config.LinkColumn('News'),'Volume':st.column_config.NumberColumn(format='%d'),'Float':st.column_config.NumberColumn(format='%d')})
else:
    st.info('Listening for qualifying $2–$10 U.S. catalyst and momentum events… Fresh catalysts can appear before the minimum-volume threshold.')

st.markdown('''
### How the new fields work
- **RVOL** is currently an **intraday volume-acceleration proxy** from the live minute stream. A full same-time-of-day historical RVOL baseline will require historical-bar initialization.
- **Float** is shown only when supplied by a **verified fundamentals source**. Alpaca's asset endpoint is a tradable-asset master list and does not provide a reliable float field, so the scanner deliberately shows **N/A** instead of guessing.
- **Float Rotation** = session volume ÷ verified float.
- **🟡 EARLY ALERT** prioritizes fresh catalysts before volume reaches 10M; **🟢 CONFIRMED** requires stronger volume/RVOL confirmation; **🔥 BREAKOUT** requires the strongest combined score.
- Canada, UK, Europe, Australia, Japan, Hong Kong and Singapore remain adapter-ready but are **not live yet**.
''')

time.sleep(5)
st.rerun()
