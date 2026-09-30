async def calculate_orb(exchange, symbol, session_key):
    try:
        session_config = ORB_SESSIONS[session_key]
        session_start_time = session_config['start_utc']
        
        now_utc = datetime.now(timezone.utc)
        
        session_start = now_utc.replace(
            hour=session_start_time.hour, 
            minute=session_start_time.minute, 
            second=0, 
            microsecond=0
        )
        
        if now_utc < session_start:
            session_start = session_start - pd.Timedelta(days=1)
        
        # Calcula quantos minutos se passaram desde o início da sessão
        minutes_since_start = int((now_utc - session_start).total_seconds() / 60)
        
        # Busca candles suficientes para cobrir desde o início da sessão + margem
        candles_needed = minutes_since_start + ORB_MINUTES + 10
        limit = min(candles_needed, 1000)  # Máximo 1000 candles
        
        ohlcv = await exchange.fetch_ohlcv(symbol, '1m', limit=limit)
        if not ohlcv or len(ohlcv) < ORB_MINUTES:
            return None
        
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
        
        # Filtra candles a partir do início da sessão
        session_candles = df[df['timestamp'] >= session_start].head(ORB_MINUTES)
        
        if len(session_candles) < ORB_MINUTES:
            return None
        
        orb_high = float(session_candles['high'].max())
        orb_low = float(session_candles['low'].min())
        current_price = float(df.iloc[-1]['close'])
        
        return {'orb_high': orb_high, 'orb_low': orb_low, 'price': current_price}
    except Exception as e:
        return None
