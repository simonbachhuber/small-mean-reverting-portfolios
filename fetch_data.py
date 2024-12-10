import requests
import pandas as pd
from datetime import datetime, timedelta
import time

def fetch_binance_data(symbol, interval, start_time, end_time, limit=1000):
    """
    Fetch historical candlestick (kline) data from Binance API.
    
    Parameters:
    - symbol: Trading pair symbol (e.g., "BTCUSDT").
    - interval: Timeframe for candlesticks (e.g., "1h" for hourly).
    - start_time: Start time in milliseconds (UNIX timestamp).
    - end_time: End time in milliseconds (UNIX timestamp).
    - limit: Number of rows per request (default: 1000).
    
    Returns:
    - Pandas DataFrame with historical price data.
    """
    url = "https://api.binance.com/api/v3/klines"
    params = {
        "symbol": symbol,
        "interval": interval,
        "startTime": start_time,
        "endTime": end_time,
        "limit": limit
    }
    data = []
    while True:
        response = requests.get(url, params=params)
        if response.status_code != 200:
            raise Exception(f"Error fetching data: {response.text}")
        candles = response.json()
        if not candles:
            break
        data.extend(candles)
        params["startTime"] = candles[-1][0] + 1  # Start from the next timestamp
        if len(candles) < limit:
            break
    # Create a DataFrame
    df = pd.DataFrame(data, columns=[
        "timestamp", "open", "high", "low", "close", "volume",
        "close_time", "quote_asset_volume", "trades", "taker_buy_base",
        "taker_buy_quote", "ignore"
    ])
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    df = df[["timestamp", "open", "high", "low", "close", "volume"]]
    return df

def fetch_top_cryptos_hourly_data(symbols, start_date, end_date, interval="1h", output_dir="data/"):
    """
    Fetch hourly data for multiple cryptocurrencies and save to CSV.
    
    Parameters:
    - symbols: List of trading pair symbols (e.g., ["BTCUSDT", "ETHUSDT"]).
    - start_date: Start date as a datetime object.
    - end_date: End date as a datetime object.
    - interval: Timeframe for candlesticks (default: "1h").
    - output_dir: Directory to save CSV files.
    """
    start_time = int(start_date.timestamp() * 1000)
    end_time = int(end_date.timestamp() * 1000)
    requests_per_minute = 4000
    requests_used = 0

    for symbol in symbols:
        print(f"Fetching data for {symbol}...")
        try:
            # Fetch data
            df = fetch_binance_data(symbol, interval, start_time, end_time)
            # Save to CSV
            filename = f"{output_dir}{symbol}_hourly.csv"
            df.to_csv(filename, index=False)
            print(f"Saved data for {symbol} to {filename}.")
            requests_used += len(df) // 1000 + 1
            # Ensure we stay under the rate limit
            if requests_used >= requests_per_minute:
                print("Rate limit reached. Pausing for 60 seconds...")
                time.sleep(60)
                requests_used = 0
        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")

# Example usage
if __name__ == "__main__":
    # Top 50 cryptocurrency pairs on Binance (example symbols)
    top_cryptos = [
        "BTCUSDT",   # Bitcoin
        "ETHUSDT",   # Ethereum
        "BNBUSDT",   # Binance Coin
        "SOLUSDT",   # Solana
        "ADAUSDT",   # Cardano
        "XRPUSDT",   # XRP
        "TONUSDT",   # Toncoin
        "DOGEUSDT",  # Dogecoin
        "DOTUSDT",   # Polkadot
        "AVAXUSDT",  # Avalanche
        "SHIBUSDT",  # Shiba Inu
        "LTCUSDT",   # Litecoin
        "UNIUSDT",   # Uniswap
        "LINKUSDT",  # Chainlink
        "ALGOUSDT",  # Algorand
        "BCHUSDT",   # Bitcoin Cash
        "XLMUSDT",   # Stellar
        "ATOMUSDT",  # Cosmos
        "AXSUSDT",   # Axie Infinity
        "TRXUSDT",   # TRON
        "ETCUSDT",   # Ethereum Classic
        "THETAUSDT", # Theta Network
        "VETUSDT",   # VeChain
        "FILUSDT",   # Filecoin
        "ICPUSDT",   # Internet Computer
        "HBARUSDT",  # Hedera
        "EGLDUSDT",  # Elrond
        "FTMUSDT",   # Fantom
        "MANAUSDT",  # Decentraland
        "SANDUSDT",  # The Sandbox
        "XTZUSDT",   # Tezos
        "GRTUSDT",   # The Graph
        "EOSUSDT",   # EOS
        "FLOWUSDT",  # Flow
        "LRCUSDT",   # Loopring
        "ENJUSDT",   # Enjin Coin
        "KSMUSDT",   # Kusama
        "CHZUSDT",   # Chiliz
        "CRVUSDT",   # Curve DAO Token
        "ZECUSDT",   # Zcash
        "STXUSDT",   # Stacks
        "NEARUSDT",  # NEAR Protocol
        "RUNEUSDT",  # THORChain
        "BATUSDT",   # Basic Attention Token
        "1INCHUSDT"  # 1inch
    ]

    # Date range: Last 5 years
    start_date = datetime(2020, 1, 1)
    end_date = datetime.now()
    # Output directory
    output_directory = "data/"
    
    fetch_top_cryptos_hourly_data(top_cryptos, start_date, end_date, output_dir=output_directory)