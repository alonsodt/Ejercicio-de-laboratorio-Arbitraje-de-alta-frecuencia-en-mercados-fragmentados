def simulate_latency(
    df_arbitrage_hits: pd.DataFrame, 
    df_consolidated_tape: pd.DataFrame, 
    latency_microseconds: int
) -> pd.DataFrame:
    """
    Simula el beneficio que realmente se obtiene a un nivel de latencia dado (Delta).

    Si la señal se detecta en T, se busca la cotización real en T + Delta.
    Si la oportunidad desaparece en T + Delta, el beneficio es 0.
    """
    if df_arbitrage_hits.empty or df_consolidated_tape.empty:
        return 0.0

    # 1. Calcular el tiempo de ejecución simulado (T + Delta)
    # T es el 'epoch' original de detección de la señal.
    df_arbitrage_hits['execution_epoch'] = df_arbitrage_hits['epoch'] + latency_microseconds

    # 2. Preparar el DataFrame completo para el lookup (CRÍTICO: debe estar ordenado)
    tape_sorted = df_consolidated_tape.sort_index().reset_index()
    
    # 3. Identificar las columnas de precios y volumen
    bid_price_cols = [col for col in tape_sorted.columns if 'price_bid_0' in col]
    ask_price_cols = [col for col in tape_sorted.columns if 'price_ask_0' in col]

    # 4. Usar merge_asof para el Lookup del Precio en T + Delta
    # merge_asof: encuentra la fila más cercana en el DataFrame de la derecha (tape_sorted)
    # cuyo 'epoch' sea ANTES o IGUAL al 'execution_epoch' de la izquierda (df_arbitrage_hits).
    
    # Seleccionamos las columnas relevantes del tape para el lookup
    cols_lookup = ['epoch'] + bid_price_cols + ask_price_cols + [col for col in tape_sorted.columns if 'vol_' in col]
    
    # Realizar el merge_asof por el tiempo de ejecución simulado
    # El 'direction="backward"' es clave: asegura que miramos el precio MÁS RECIENTE
    # que estaba disponible antes o en el momento de la ejecución.
    df_realized = pd.merge_asof(
        df_arbitrage_hits,
        tape_sorted[cols_lookup],
        left_on='execution_epoch',
        right_on='epoch',
        suffixes=('_T', '_T_Delta'),
        direction='backward'
    )
    
    # 5. Recalcular la Oportunidad en T + Delta
    
    # Calcular el Max Bid y Min Ask reales a tiempo T + Delta
    df_realized['Global_Max_Bid_Real'] = df_realized[bid_price_cols].max(axis=1)
    df_realized['Global_Min_Ask_Real'] = df_realized[ask_price_cols].min(axis=1)

    # 6. Calcular el Beneficio Realizado
    
    # Profit por unidad: Solo si Max Bid > Min Ask. Si no, es 0 (la oportunidad se ha ido).
    df_realized['Realized_Profit_Unit'] = df_realized['Global_Max_Bid_Real'] - df_realized['Global_Min_Ask_Real']
    df_realized['Realized_Profit_Unit'] = df_realized['Realized_Profit_Unit'].clip(lower=0) # Beneficio nunca < 0

    # La cantidad que puedes negociar (Traded_Qty) sigue siendo la MINIMA cantidad
    # que viste en T, ya que tu orden fue enviada por esa cantidad.
    # CRÍTICA: Asumimos que la orden se ejecuta al menos por el volumen que enviamos,
    # siempre que el precio siga siendo favorable.

    # Beneficio Realizado Total:
    df_realized['Realized_Profit'] = df_realized['Realized_Profit_Unit'] * df_realized['Traded_Qty']
    
    # Sumar y devolver el beneficio total para esta latencia
    total_realized_profit = df_realized['Realized_Profit'].sum()
    
    # print(f"   -> Latencia {latency_microseconds}μs: Beneficio Realizado = {total_realized_profit:.2f} €")
    
    return total_realized_profit
