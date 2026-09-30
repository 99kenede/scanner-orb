import ccxt.async_support as ccxt
import asyncio
import pandas as pd
from datetime import datetime, timezone
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
import uvicorn
import json
import sys

app = FastAPI()

# ═══════════════════════════════════════════════════════════════
# CONFIGURAÇÕES
# ═══════════════════════════════════════════════════════════════
ORB_MINUTES = 10
SCAN_INTERVAL = 15  # segundos entre scans completos

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

# Cache global dos resultados do scan
scan_cache = {'above': [], 'below': [], 'inside': [], 'updated': None}

# ═══════════════════════════════════════════════════════════════
# EXCHANGES
# ═══════════════════════════════════════════════════════════════
async def create_exchanges():
    return {
        'binance': ccxt.binance({'enableRateLimit': True, 'options': {'defaultType': 'future'}}),
        'bybit': ccxt.bybit({'enableRateLimit': True, 'options': {'defaultType': 'swap'}}),
        'okx': ccxt.okx({'enableRateLimit': True, 'options': {'defaultType': 'swap'}}),
    }

async def close_exchanges(exchanges):
    for ex in exchanges.values():
        await ex.close()

# ═══════════════════════════════════════════════════════════════
# LÓGICA DO SCANNER
# ═══════════════════════════════════════════════════════════════
async def get_best_exchange_for_pair(exchanges, symbol):
    candidates = []
    for ex_id, ex in exchanges.items():
        try:
            ticker = await ex.fetch_ticker(symbol)
            if ticker and ticker.get('quoteVolume'):
                candidates.append({
                    'exchange': ex_id,
                    'ticker': ticker,
                    'volume': float(ticker['quoteVolume'] or 0)
                })
        except:
            continue
    if not candidates:
        return None, None
    candidates.sort(key=lambda x: x['volume'], reverse=True)
    best = candidates[0]
    return best['exchange'], best['ticker']

async def calculate_orb(exchange, symbol):
    try:
        ohlcv = await exchange.fetch_ohlcv(symbol, '1m', limit=120)
        if not ohlcv or len(ohlcv) < ORB_MINUTES:
            return None
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
        now_utc = datetime.now(timezone.utc)
        today_start = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
        today_candles = df[df['timestamp'] >= today_start]
        if len(today_candles) < ORB_MINUTES:
            return None
        orb_candles = today_candles.head(ORB_MINUTES)
        orb_high = float(orb_candles['high'].max())
        orb_low = float(orb_candles['low'].min())
        current_price = float(df.iloc[-1]['close'])
        return {'orb_high': orb_high, 'orb_low': orb_low, 'price': current_price}
    except:
        return None

async def scan_pair(exchanges, symbol):
    ex_id, ticker = await get_best_exchange_for_pair(exchanges, symbol)
    if not ticker:
        return None
    exchange = exchanges[ex_id]
    orb_data = await calculate_orb(exchange, symbol)
    if not orb_data:
        return None
    price = orb_data['price']
    orb_high = orb_data['orb_high']
    orb_low = orb_data['orb_low']
    status = None
    distance_pct = 0
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
        'exchange': ex_id,
        'price': price,
        'orb_high': orb_high,
        'orb_low': orb_low,
        'status': status,
        'distance_pct': distance_pct,
        'volume_24h': float(ticker.get('quoteVolume', 0) or 0),
    }

async def run_full_scan(exchanges):
    tasks = [scan_pair(exchanges, symbol) for symbol in BREAKOUT_PROP_PAIRS]
    results = await asyncio.gather(*tasks)
    above = sorted([r for r in results if r and r['status'] == 'ACIMA'], key=lambda x: x['distance_pct'], reverse=True)
    below = sorted([r for r in results if r and r['status'] == 'ABAIXO'], key=lambda x: x['distance_pct'], reverse=True)
    inside = [r for r in results if r and r['status'] == 'DENTRO']
    return above, below, inside

# ═══════════════════════════════════════════════════════════════
# ENDPOINTS REST (para buscar candles de um par específico)
# ═══════════════════════════════════════════════════════════════
@app.get("/candles/{symbol}")
async def get_candles(symbol: str, timeframe: str = "5m", limit: int = 200):
    """Busca candles históricos de um par específico"""
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
        candles = []
        for c in ohlcv:
            candles.append({
                'time': int(c[0] / 1000),  # Lightweight Charts usa segundos
                'open': float(c[1]),
                'high': float(c[2]),
                'low': float(c[3]),
                'close': float(c[4]),
                'volume': float(c[5]),
            })
        return {'symbol': symbol, 'timeframe': timeframe, 'candles': candles}
    except Exception as e:
        return {"error": str(e)}

# ═══════════════════════════════════════════════════════════════
# WEBSOCKET (envia lista atualizada em tempo real)
# ═══════════════════════════════════════════════════════════════
connected_clients = []

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_clients.append(websocket)
    try:
        while True:
            await websocket.receive_text()  # Mantém conexão viva
    except WebSocketDisconnect:
        connected_clients.remove(websocket)

async def broadcast_scan_results():
    """Envia resultados do scan para todos os clientes conectados"""
    if not connected_clients:
        return
    message = json.dumps(scan_cache, default=str)
    disconnected = []
    for client in connected_clients:
        try:
            await client.send_text(message)
        except:
            disconnected.append(client)
    for client in disconnected:
        if client in connected_clients:
            connected_clients.remove(client)

# ═══════════════════════════════════════════════════════════════
# SERVE O HTML
# ═══════════════════════════════════════════════════════════════
@app.get("/", response_class=HTMLResponse)
async def serve_frontend():
    with open("index.html", "r", encoding="utf-8") as f:
        return f.read()

# ═══════════════════════════════════════════════════════════════
# LOOP DE BACKGROUND
# ═══════════════════════════════════════════════════════════════
async def scanner_loop():
    """Roda o scanner em loop e atualiza o cache"""
    exchanges = await create_exchanges()
    try:
        while True:
            try:
                above, below, inside = await run_full_scan(exchanges)
                scan_cache['above'] = above
                scan_cache['below'] = below
                scan_cache['inside'] = inside
                scan_cache['updated'] = datetime.now().strftime("%H:%M:%S")
                await broadcast_scan_results()
                print(f"[{scan_cache['updated']}] Scan completo: {len(above)} acima, {len(below)} abaixo, {len(inside)} dentro")
            except Exception as e:
                print(f"Erro no scan: {e}")
            await asyncio.sleep(SCAN_INTERVAL)
    finally:
        await close_exchanges(exchanges)

# ═══════════════════════════════════════════════════════════════
# INICIALIZAÇÃO
# ═══════════════════════════════════════════════════════════════
@app.on_event("startup")
async def startup_event():
    asyncio.create_task(scanner_loop())

if __name__ == "__main__":
    print("🚀 Servidor iniciando... Acesse http://localhost:8000 no navegador")
    uvicorn.run(app, host="0.0.0.0", port=8000)