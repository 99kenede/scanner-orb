import ccxt.async_support as ccxt
import asyncio
import pandas as pd
from datetime import datetime, timezone, time
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from contextlib import asynccontextmanager
import uvicorn
import json
import os
import sys

# ══════════════════════════════════════════════════════════════
# CONFIGURAÇÕES
# ══════════════════════════════════════════════════════════════
ORB_MINUTES = 10
SCAN_INTERVAL = 15

ORB_SESSIONS = {
    '21:00': time(0, 0, 0),
    '10:30': time(13, 30, 0),
}

DEFAULT_SESSION = '21:00'

BREAKOUT_PROP_PAIRS = [
    'BTC/USDT:USDT', 'ETH/USDT:USDT', 'SOL/USDT:USDT',
    'AVAX/USDT:USDT', 'DOGE/USDT:USDT', 'XRP/USDT:USDT',
    'ADA/USDT:USDT', 'LINK/USDT:USDT', 'DOT/USDT:USDT',
    'MATIC/USDT:USDT', 'LTC/USDT:USDT', 'BCH/USDT:USDT',
    'UNI/USDT:USDT', 'ATOM/USDT:USDT', 'ETC/USDT:USDT',
    'FIL/USDT:USDT', 'APT/USDT:USDT', 'ARB/USDT:USDT',
    'OP/USDT:USDT', 'NEAR/USDT:USDT', 'INJ/USDT:USDT',
    'TIA/USDT:USDT', 'SUI/USDT:USDT', 'SEI/USDT:USDT',
    'PEPE/USDT:USDT', 'WIF/USDT:USDT', 'BONK/USDT:USDT',
    'FLOKI/USDT:USDT', 'SHIB/USDT:USDT', 'TRX/USDT:USDT',
    'TON/USDT:USDT', 'NOT/USDT:USDT', 'AAVE/USDT:USDT',
    'MKR/USDT:USDT', 'SNX/USDT:USDT', 'CRV/USDT:USDT',
    'RUNE/USDT:USDT', 'FET/USDT:USDT', 'RENDER/USDT:USDT',
    'TAO/USDT:USDT', 'ORDI/USDT:USDT', 'ENA/USDT:USDT',
    'WLD/USDT:USDT', 'IMX/USDT:USDT', 'GRT/USDT:USDT',
    'ALGO/USDT:USDT', 'FTM/USDT:USDT', 'SAND/USDT:USDT',
    'MANA/USDT:USDT', 'AXS/USDT:USDT', 'GALA/USDT:USDT',
]

candles_cache = {}
connected_clients = []
client_filters = {}

# ══════════════════════════════════════════════════════════════
# FUNÇÕES AUXILIARES
# ══════════════════════════════════════════════════════════════
async def create_exchanges():
    return {
        'mexc': ccxt.mexc({'enableRateLimit': True, 'options': {'defaultType': 'swap'}}),
        'gate': ccxt.gate({'enableRateLimit': True, 'options': {'defaultType': 'swap'}}),
        'okx': ccxt.okx({'enableRateLimit': True, 'options': {'defaultType': 'swap'}}),
        'bybit': ccxt.bybit({'enableRateLimit': True, 'options': {'defaultType': 'swap'}}),
        'binance': ccxt.binance({'enableRateLimit': True, 'options': {'defaultType': 'future'}}),
    }

async def close_exchanges(exchanges):
    for ex in exchanges.values():
        await ex.close()

async def get_best_exchange_for_pair(exchanges, symbol):
    candidates = []
    exchange_order = ['mexc', 'gate', 'okx', 'bybit', 'binance']
    
    for ex_id in exchange_order:
        ex = exchanges[ex_id]
        try:
            ticker = await ex.fetch_ticker(symbol)
            if ticker and ticker.get('quoteVolume'):
                candidates.append({
                    'exchange': ex_id,
                    'ticker': ticker,
                    'volume': float(ticker['quoteVolume'] or 0)
                })
        except Exception as e:
            err_msg = str(e)
            if "451" in err_msg or "403" in err_msg or "restricted" in err_msg.lower():
                print(f"⛔ {symbol} | {ex_id.upper()} bloqueou o IP", file=sys.stderr)
            else:
                print(f"❌ {symbol} | {ex_id.upper()} falhou: {err_msg[:60]}", file=sys.stderr)
            continue
            
    if not candidates:
        print(f"⚠️ {symbol} | NENHUMA exchange funcionou", file=sys.stderr)
        return None, None
        
    candidates.sort(key=lambda x: x['volume'], reverse=True)
    return candidates[0]['exchange'], candidates[0]['ticker']

def get_session_start_utc(session_key):
    session_time = ORB_SESSIONS[session_key]
    now_utc = datetime.now(timezone.utc)
    
    session_start = now_utc.replace(
        hour=session_time.hour,
        minute=session_time.minute,
        second=0,
        microsecond=0
    )
    
    if now_utc < session_start:
        session_start = session_start - pd.Timedelta(days=1)
    
    return int(session_start.timestamp() * 1000), session_start

async def calculate_orb(exchange, symbol, session_key):
    """
    Calcula o ORB buscando TODOS os candles desde o início da sessão.
    ORB = High/Low dos primeiros 10 minutos (2 candles de 5min).
    Preço atual = último candle recebido.
    """
    try:
        session_start_ms, session_start_dt = get_session_start_utc(session_key)
        
        # Calcular quantos minutos se passaram desde o início da sessão
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        minutes_passed = (now_ms - session_start_ms) // 60000
        
        # Precisamos de TODOS os candles desde o início da sessão
        # Cada candle de 5min = precisamos de (minutes_passed / 5) candles
        candles_needed = max(20, int(minutes_passed / 5) + 10)
        
        # Buscar TODOS os candles desde o início da sessão
        ohlcv = await exchange.fetch_ohlcv(symbol, '5m', since=session_start_ms, limit=candles_needed)
        
        if not ohlcv or len(ohlcv) < 2:
            return None
        
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        
        # Os PRIMEIROS 2 candles (10 minutos) são o ORB
        orb_candles = df.head(2)
        
        # Preço atual é o ÚLTIMO candle
        current_price = float(df.iloc[-1]['close'])
        orb_high = float(orb_candles['high'].max())
        orb_low = float(orb_candles['low'].min())
        
        # Debug apenas para BTC
        if symbol == 'BTC/USDT:USDT':
            print(f"🔍 BTC | Sessão: {session_key} | Início: {session_start_dt.strftime('%H:%M UTC')}", file=sys.stderr)
            print(f"📊 BTC | Minutos passados: {minutes_passed} | Candles buscados: {len(ohlcv)}", file=sys.stderr)
            print(f"✅ BTC | ORB High: {orb_high} | Low: {orb_low} | Preço ATUAL: {current_price}", file=sys.stderr)
        
        return {'orb_high': orb_high, 'orb_low': orb_low, 'price': current_price}
    except Exception as e:
        print(f" {symbol} | Erro calculate_orb: {e}", file=sys.stderr)
        return None

async def scan_pair(exchanges, symbol, session_key):
    try:
        ex_id, ticker = await get_best_exchange_for_pair(exchanges, symbol)
        
        if not ticker:
            return None
            
        exchange = exchanges[ex_id]
        orb_data = await calculate_orb(exchange, symbol, session_key)
        
        if not orb_data:
            return None
        
        price = orb_data['price']
        orb_high = orb_data['orb_high']
        orb_low = orb_data['orb_low']
        
        if price > orb_high:
            status = 'ACIMA'
            distance_pct = ((price - orb_high) / orb_high) * 100
        elif price < orb_low:
            status = 'ABAIXO'
            distance_pct = ((orb_low - price) / orb_low) * 100
        else:
            status = 'DENTRO'
            distance_pct = 0
            
        return {
            'symbol': symbol.replace('/USDT:USDT', ''),
            'exchange': ex_id.upper(),
            'price': price,
            'orb_high': orb_high,
            'orb_low': orb_low,
            'status': status,
            'distance_pct': distance_pct,
            'volume_24h': float(ticker.get('quoteVolume', 0) or 0),
        }
    except Exception as e:
        return None

async def run_full_scan(exchanges, session_key):
    try:
        print(f"🚀 [SCAN] Sessão: {session_key} | {len(BREAKOUT_PROP_PAIRS)} pares", file=sys.stderr)
        tasks = [scan_pair(exchanges, symbol, session_key) for symbol in BREAKOUT_PROP_PAIRS]
        results = await asyncio.gather(*tasks)
        
        above = sorted([r for r in results if r and r['status'] == 'ACIMA'], key=lambda x: x['distance_pct'], reverse=True)
        below = sorted([r for r in results if r and r['status'] == 'ABAIXO'], key=lambda x: x['distance_pct'], reverse=True)
        inside = [r for r in results if r and r['status'] == 'DENTRO']
        
        print(f"✅ [SCAN] Acima: {len(above)} | Abaixo: {len(below)} | Dentro: {len(inside)}", file=sys.stderr)
        
        if above:
            top_3 = [f"{p['symbol']} (+{p['distance_pct']:.2f}%)" for p in above[:3]]
            print(f"📊 TOP 3 ACIMA: {', '.join(top_3)}", file=sys.stderr)
        
        return above, below, inside
    except Exception as e:
        print(f"❌ [ERRO SCAN] {e}", file=sys.stderr)
        return [], [], []

# ══════════════════════════════════════════════════════════════
# APP FASTAPI
# ══════════════════════════════════════════════════════════════
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("🚀 Servidor iniciando...", file=sys.stderr)
    print("🛡️ Prioridade: MEXC > GATE > OKX > BYBIT > BINANCE", file=sys.stderr)
    yield

app = FastAPI(lifespan=lifespan)

# ═════════════════════════════════════════════════════════════
# ROTAS HTTP (Polling)
# ══════════════════════════════════════════════════════════════
@app.get("/api/scan")
async def api_scan():
    try:
        print(f"📡 [API SCAN] Requisição recebida", file=sys.stderr)
        exchanges = await create_exchanges()
        above, below, inside = await run_full_scan(exchanges, DEFAULT_SESSION)
        await close_exchanges(exchanges)
        
        result = {
            'above': above,
            'below': below,
            'inside': inside,
            'updated': datetime.now().strftime("%H:%M:%S"),
            'session': DEFAULT_SESSION
        }
        
        print(f"✅ [API SCAN] Enviando {len(above) + len(below) + len(inside)} pares", file=sys.stderr)
        return result
    except Exception as e:
        print(f"❌ [API SCAN ERRO] {e}", file=sys.stderr)
        return {"error": str(e), "above": [], "below": [], "inside": []}

@app.get("/candles/{symbol}")
async def get_candles(symbol: str, timeframe: str = "5m", limit: int = 200):
    cache_key = f"{symbol}_{timeframe}_{limit}"
    if cache_key in candles_cache:
        cached_time, cached_data = candles_cache[cache_key]
        if (datetime.now() - cached_time).seconds < 30:
            return cached_data
    
    try:
        exchanges = await create_exchanges()
        ex_id, _ = await get_best_exchange_for_pair(exchanges, f"{symbol}/USDT:USDT")
        if not ex_id:
            return {"error": "Par não encontrado"}
        exchange = exchanges[ex_id]
        ohlcv = await exchange.fetch_ohlcv(f"{symbol}/USDT:USDT", timeframe, limit=limit)
        await close_exchanges(exchanges)
        if not ohlcv:
            return {"error": "Sem dados"}
        candles = [{'time': int(c[0] / 1000), 'open': float(c[1]), 'high': float(c[2]), 'low': float(c[3]), 'close': float(c[4]), 'volume': float(c[5])} for c in ohlcv]
        result = {'symbol': symbol, 'timeframe': timeframe, 'candles': candles}
        candles_cache[cache_key] = (datetime.now(), result)
        return result
    except Exception as e:
        return {"error": str(e)}

@app.get("/", response_class=HTMLResponse)
async def serve_frontend():
    with open("index.html", "r", encoding="utf-8") as f:
        return f.read()

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_clients.append(websocket)
    client_filters[websocket] = {'session': DEFAULT_SESSION, 'filter': 'all'}
    try:
        while True:
            message = await websocket.receive_text()
            data = json.loads(message)
            if 'session' in data: client_filters[websocket]['session'] = data['session']
            if 'filter' in data: client_filters[websocket]['filter'] = data['filter']
    except WebSocketDisconnect:
        connected_clients.remove(websocket)
        if websocket in client_filters: del client_filters[websocket]

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
