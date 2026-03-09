import pandas as pd
import asyncio
from datetime import datetime, timedelta, time, timezone
from zoneinfo import ZoneInfo
import os
import json
import csv

from alpaca.data.live import StockDataStream
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import LimitOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.trading.stream import TradingStream

os.chdir(r"S:\TRADING DESK\Traders Only\Victor\Python\Alpaca")
STATE_FILE = "algo_state.json"

# ================= CONFIG =================
API_KEY = "PKMT57EOOP3X4LTO3TPAXNYVJG"
SECRET_KEY = "3epsMggEbRsAE1cfc2jefRypkuHZaoE67AWjiw4KWkVw"
TRADING_SYMBOL = "TQQQ"

TRADING_WINDOW_START = time(9, 30)
TRADING_WINDOW_END = time(16, 0)
CANDLE_WINDOW_START = time(9, 20)
CANDLE_WINDOW_END = time(16, 0)
EOD_START = time(15, 49)
EOD_END = time(15, 59)

STOP_LOSS = -0.02 #default is -0.02
TAKE_PROFIT1 = 0.035
TAKE_PROFIT2 = 0.05
ORDER_QTY = 2

MAX_INTRADAY_CANDLES = 2000
consecutive_stop_losses = 0
trading_halted = False
prev_close = None
# ================= CLIENTS =================
trading_client = TradingClient(API_KEY, SECRET_KEY, paper=True)
market_stream = StockDataStream(API_KEY, SECRET_KEY)
fill_stream = TradingStream(API_KEY, SECRET_KEY, paper=True)
data_client = StockHistoricalDataClient(API_KEY, SECRET_KEY)

ET = ZoneInfo("America/New_York")
FILL_LOG_FILE = "trade_fills.csv"
POLL_LOG_FILE = "poll_log.csv"

# ================= STATE =================
intraday_df = pd.DataFrame(columns=["close"])

live_price = None
last_5m_close = None

sma50 = None
sma150 = None
std35 = None
raw_std35 = None
std_floor_pct = 0.01

current_position = None
entry_price = None
entry_time = None

entry_std_multiplier = 1.2
last_loss_trading_day = None
trading_days_since_loss = 0
cooldown_until = None

last_signal = None

# Check if trading halted
def check_halt_conditions(prev_close):
    global trading_halted

    # Check 8% daily drop
    if last_5m_close and prev_close:
        daily_pnl = (last_5m_close - prev_close) / prev_close
        if daily_pnl <= -0.08:
            trading_halted = True
            print(f"\n[HALT] Daily drop exceeded 8% — trading halted until restart")
            return

    # Check 3 consecutive stop losses
    if consecutive_stop_losses >= 3:
        trading_halted = True
        print(f"\n[HALT] 3 consecutive stop losses — trading halted until restart")

def fetch_prev_close():
    global prev_close

    try:
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=5)

        request = StockBarsRequest(
            symbol_or_symbols=[TRADING_SYMBOL],
            timeframe=TimeFrame(1, TimeFrameUnit.Day),
            start=start,
            end=end,
            feed="iex"
        )

        bars = data_client.get_stock_bars(request).df

        if isinstance(bars.index, pd.MultiIndex):
            bars = bars.xs(TRADING_SYMBOL, level="symbol")

        today = datetime.now(ET).date()
        last_bar_date = bars.index[-1].tz_convert(ET).date() if bars.index.tz is not None else bars.index[-1].date()

        if last_bar_date == today:
            prev_close = float(bars["close"].iloc[-2])
        else:
            prev_close = float(bars["close"].iloc[-1])

        print(f"Reference price (prev close): {prev_close:.2f}")

    except Exception as e:
        print(f"\n[PREV CLOSE ERROR] {e}")

def filter_trading_hours(bars_df):
    """Keep only candles that fall within regular trading hours (9:30–16:00 ET, Mon–Fri)."""
    idx = bars_df.index
    
    # Convert to ET in-place on the actual index
    if idx.tz is None:
        idx = idx.tz_localize(ET)
    else:
        idx = idx.tz_convert(ET)
    
    mask = (
        (idx.time >= CANDLE_WINDOW_START) &
        (idx.time < CANDLE_WINDOW_END) &
        (idx.dayofweek < 5)
    )
    return bars_df[mask]

# ================= FILL LOGGING =================
def log_trade(side, price, reason, qty=ORDER_QTY):
    """Append a buy/sell event to the CSV fill log."""
    file_exists = os.path.exists(FILL_LOG_FILE)
    now_et = datetime.now(ET).strftime("%Y-%m-%d %H:%M:%S")

    with open(FILL_LOG_FILE, "a", newline="") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["timestamp_et", "symbol", "side", "qty", "price", "reason"])
        writer.writerow([now_et, TRADING_SYMBOL, side.upper(), qty, f"{price:.2f}", reason])

    print(f"\n[TRADE LOG] {now_et} | {side.upper()} {qty}x {TRADING_SYMBOL} @ {price:.2f} | Reason: {reason}")

# ================= POLL LOGGING =================
def log_poll():
    """Append a row to poll_log.csv every 5-min cycle for diagnostics."""
    file_exists = os.path.exists(POLL_LOG_FILE)
    now_et = datetime.now(ET).strftime("%Y-%m-%d %H:%M:%S")

    pnl_pct = None
    if current_position == "long" and entry_price and last_5m_close:
        pnl_pct = round((last_5m_close - entry_price) / entry_price * 100, 4)

    in_window = TRADING_WINDOW_START <= datetime.now(ET).time() <= TRADING_WINDOW_END
    in_eod = EOD_START <= datetime.now(ET).time() <= EOD_END
    cool = cooldown_until.strftime("%Y-%m-%d %H:%M:%S") if cooldown_until and datetime.now(ET) < cooldown_until else "OFF"
    min_std = round(last_5m_close * std_floor_pct, 4) if last_5m_close else None
    floor_active = raw_std35 is not None and min_std is not None and raw_std35 < min_std

    with open(POLL_LOG_FILE, "a", newline="") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow([
                "timestamp_et", "close", "live_price", "sma50", "sma150",
                "sd35_raw", "sd35_used", "sd35_floor_active", "lower_band", "upper_band",
                "position", "entry_price", "pnl_pct", "multiplier",
                "in_window", "in_eod", "cooldown"
            ])

        lb = round(min(sma50, sma150) - (entry_std_multiplier * std35), 4) if sma50 and sma150 and std35 else None
        ub = round(max(sma50, sma150) + (0.6 * std35), 4) if sma50 and sma150 and std35 else None

        writer.writerow([
            now_et,
            round(last_5m_close, 4) if last_5m_close else None,
            round(live_price, 4) if live_price else None,
            round(sma50, 4) if sma50 else None,
            round(sma150, 4) if sma150 else None,
            round(raw_std35, 4) if raw_std35 else None,
            round(std35, 4) if std35 else None,
            floor_active,
            lb, ub,
            current_position,
            entry_price,
            pnl_pct,
            entry_std_multiplier,
            in_window,
            in_eod,
            cool
        ])

# ================= STATE SAVE/LOAD =================
def save_state():
    state = {
        "entry_std_multiplier": entry_std_multiplier,
        "cooldown_until": cooldown_until.isoformat() if cooldown_until else None,
        "last_loss_trading_day": last_loss_trading_day.isoformat() if last_loss_trading_day else None,
        "trading_days_since_loss": trading_days_since_loss,
        "current_position": current_position,
        "entry_price": entry_price,
        "consecutive_stop_losses": consecutive_stop_losses,
        "trading_halted": trading_halted
    }
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)

def load_state():
    global entry_std_multiplier, cooldown_until
    global last_loss_trading_day, trading_days_since_loss
    global current_position, entry_price, consecutive_stop_losses, trading_halted

    if not os.path.exists(STATE_FILE):
        return

    with open(STATE_FILE, "r") as f:
        state = json.load(f)

    entry_std_multiplier = state.get("entry_std_multiplier", 1.2)

    cooldown_str = state.get("cooldown_until")
    cooldown_until = datetime.fromisoformat(cooldown_str) if cooldown_str else None

    loss_day_str = state.get("last_loss_trading_day")
    last_loss_trading_day = datetime.fromisoformat(loss_day_str).date() if loss_day_str else None

    trading_days_since_loss = state.get("trading_days_since_loss", 0)

    current_position = state.get("current_position")
    entry_price = state.get("entry_price")
    consecutive_stop_losses = state.get("consecutive_stop_losses", 0)
    trading_halted = state.get("trading_halted", False)

# ================= PRELOAD INTRADAY =================
def preload_intraday():
    global intraday_df, last_5m_close

    end = datetime.now(timezone.utc)
    start = end - timedelta(days=5)

    request = StockBarsRequest(
        symbol_or_symbols=[TRADING_SYMBOL],
        timeframe=TimeFrame(5, TimeFrameUnit.Minute),
        start=start,
        end=end,
        feed="iex"
    )

    bars = data_client.get_stock_bars(request).df

    if isinstance(bars.index, pd.MultiIndex):
        bars = bars.xs(TRADING_SYMBOL, level="symbol")
    
    bars = filter_trading_hours(bars)
    intraday_df = pd.DataFrame({"close": pd.to_numeric(bars["close"].values)})
    intraday_df = intraday_df.reset_index(drop=True)

    last_5m_close = intraday_df["close"].iloc[-1]

    print(f"Preloaded {len(intraday_df)} candles")

    if len(intraday_df) >= 150:
        compute_intraday_indicators()
        print(f"Indicators ready on startup — SMA50:{sma50:.2f} SMA150:{sma150:.2f} SD35:{std35:.2f}")
    else:
        print(f"Warning: only {len(intraday_df)} candles preloaded, need 150 for indicators")

# ================= POLL 5-MIN HISTORICAL =================
async def poll_5min_bars():
    global intraday_df, last_5m_close

    #don't start indicators or trades without live price
    while live_price is None:
        await asyncio.sleep(1)

    while True:
        try:
            end = datetime.now(timezone.utc)
            start = end - timedelta(days=5)

            request = StockBarsRequest(
                symbol_or_symbols=[TRADING_SYMBOL],
                timeframe=TimeFrame(5, TimeFrameUnit.Minute),
                start=start,
                end=end,
                feed="iex"
            )

            bars = data_client.get_stock_bars(request).df

            if isinstance(bars.index, pd.MultiIndex):
                bars = bars.xs(TRADING_SYMBOL, level="symbol")

            bars = filter_trading_hours(bars)
            new_df = pd.DataFrame({"close": pd.to_numeric(bars["close"].values)})
            intraday_df = new_df.tail(MAX_INTRADAY_CANDLES).reset_index(drop=True)

            last_5m_close = intraday_df["close"].iloc[-1]

            if len(intraday_df) >= 150 and compute_intraday_indicators():
                update_multiplier_state()
                trade_logic()
                save_state()

            log_poll()
            print_status()

        except Exception as e:
            import traceback
            print(f"\nPolling error: {e}")
            traceback.print_exc()

        await asyncio.sleep(5 * 60)

# ================= POSITION SYNC =================
def sync_position():
    global current_position, entry_price

    try:
        position = trading_client.get_open_position(TRADING_SYMBOL)
        current_position = "long"
        entry_price = float(position.avg_entry_price)
        print(f"Synced LONG @ {entry_price:.2f}")
    except Exception:
        current_position = None
        entry_price = None
        print("No open position.")

    save_state()  # always save after sync so state file exists from the start

# ================= INDICATORS =================
def compute_intraday_indicators():
    global sma50, sma150, std35, raw_std35

    if len(intraday_df) < 150:
        return False

    closes = intraday_df["close"]

    sma50 = closes.rolling(50).mean().iloc[-1]
    sma150 = closes.rolling(150).mean().iloc[-1]
    std35 = closes.rolling(35).std().iloc[-1]
    raw_std35 = std35  # save real value before flooring

    min_std = last_5m_close * std_floor_pct
    if std35 < min_std:
        std35 = min_std

    return True

# ================= MULTIPLIER =================
def update_multiplier_state():
    global entry_std_multiplier, trading_days_since_loss, last_loss_trading_day
   
    if last_loss_trading_day is None:
        return

    today = datetime.now(ET).date()

    if today > last_loss_trading_day:
        if not hasattr(update_multiplier_state, "_last_processed_day") or \
                update_multiplier_state._last_processed_day != today:

            update_multiplier_state._last_processed_day = today
            trading_days_since_loss += 1
            last_loss_trading_day = today

            if trading_days_since_loss == 1:
                entry_std_multiplier = 1.3
            elif trading_days_since_loss >= 2:
                entry_std_multiplier = 1.2

            save_state()

# ================= DISPLAY =================
def print_status():
    lp = f"{live_price:.2f}" if live_price is not None else "-"
    lc = f"{last_5m_close:.2f}" if last_5m_close is not None else "-"

    s50 = f"{sma50:.2f}" if pd.notna(sma50) else "-"
    s150 = f"{sma150:.2f}" if pd.notna(sma150) else "-"

    if pd.notna(sma50) and pd.notna(sma150) and pd.notna(std35):
        lower_band = min(sma50, sma150) - (entry_std_multiplier * std35)
        upper_band = max(sma50, sma150) + (0.6 * std35)
        lb = f"{lower_band:.2f}"
        ub = f"{upper_band:.2f}"
    else:
        lb = "-"
        ub = "-"

    now_et = datetime.now(ET).strftime("%H:%M:%S")
    cool_display = cooldown_until.strftime("%H:%M:%S") if cooldown_until and datetime.now(ET) < cooldown_until else "OFF"

    std = f"{raw_std35:.2f}" if raw_std35 is not None and pd.notna(raw_std35) else "-"
    floor_display = f" [SD FLOOR ACTIVE:{std35:.4f}]" if raw_std35 is not None and raw_std35 < last_5m_close * std_floor_pct else ""

    print(
        f"\r[{now_et}] Live:{lp} | Close:{lc} | SMA50:{s50} | SMA150:{s150} | SD35:{std} | "
        f"LB:{lb} | UB:{ub} | Mult:{entry_std_multiplier} | Cool:{cool_display} | "
        f"Days:{trading_days_since_loss}{floor_display}",
        end="", flush=True
    )

# ================= ORDER =================
def conditions_still_met(side, initial_price):
    close_price = live_price if live_price is not None else last_5m_close

    if side == "buy":
        return close_price <= initial_price * 1.005
    elif side == "sell":
        return close_price >= initial_price * 0.995
    return False


def place_limit_order(side, reason):
    global current_position, entry_price, entry_time, last_signal
    last_signal = reason
    initial_price = live_price if live_price is not None else last_5m_close

    import time as time_mod

    while True:
        if trading_halted:
            print(f"\n[RETRY ABORTED] Trading halted")
            return False

        now_dt = datetime.now(ET)
        if not (TRADING_WINDOW_START <= now_dt.time() <= TRADING_WINDOW_END):
            print(f"\n[RETRY ABORTED] Outside trading window")
            return False

        if cooldown_until and now_dt < cooldown_until:
            print(f"\n[RETRY ABORTED] In cooldown")
            return False

        if not conditions_still_met(side, initial_price):
            print(f"\n[RETRY ABORTED] Price moved 0.5% from initial — {reason}")
            return False

        price = live_price if live_price is not None else last_5m_close

        if price is None:
            print(f"\n[ORDER SKIPPED] No price available for {side.upper()} — {reason}")
            return False

        if side == "buy":
            limit = round(price * 1.001, 2)
        else:
            limit = round(price * 0.999, 2)

        order = LimitOrderRequest(
            symbol=TRADING_SYMBOL,
            qty=ORDER_QTY,
            side=OrderSide.BUY if side == "buy" else OrderSide.SELL,
            time_in_force=TimeInForce.FOK,
            limit_price=limit
        )

        try:
            submitted = trading_client.submit_order(order)
            time_mod.sleep(1)
            order_status = trading_client.get_order_by_id(submitted.id)

            if order_status.status.value == "filled":
                filled_price = float(order_status.filled_avg_price)
                log_trade(side, filled_price, reason)

                if side == "buy":
                    current_position = "long"
                    entry_price = filled_price
                    entry_time = datetime.now(ET)
                else:
                    current_position = None
                    entry_price = None
                    entry_time = None

                save_state()
                print(f"\n[FILLED] {side.upper()} {ORDER_QTY}x @ {filled_price:.2f} | {reason}")
                return True

            else:
                print(f"\n[FOK KILLED] {side.upper()} not filled — retrying in 2s | {reason}")
                time_mod.sleep(2)

        except Exception as e:
            print(f"\n[ORDER ERROR] {e} — retrying in 2s")
            time_mod.sleep(2)


# ================= STRATEGY =================
def trade_logic():
    global cooldown_until, last_loss_trading_day, trading_days_since_loss
    global entry_std_multiplier, consecutive_stop_losses, trading_halted

    now_dt = datetime.now(ET)
    now = now_dt.time()

    if trading_halted:
        return

    if not (TRADING_WINDOW_START <= now <= TRADING_WINDOW_END):
        return

    if cooldown_until and now_dt < cooldown_until:
        return

    close_price = last_5m_close

    if current_position is not None and entry_price is not None:
        pnl_pct = (close_price - entry_price) / entry_price

        if pnl_pct <= STOP_LOSS:
            if place_limit_order("sell", "STOP_LOSS"):
                cooldown_until = now_dt + timedelta(minutes=10)
                last_loss_trading_day = now_dt.date()
                trading_days_since_loss = 0
                entry_std_multiplier = 1.4
                consecutive_stop_losses += 1
                check_halt_conditions(prev_close)
                save_state()
            return

        if pnl_pct >= TAKE_PROFIT2:
            if place_limit_order("sell", "TAKE_PROFIT2"):
                consecutive_stop_losses = 0
                save_state()
            return

        if pnl_pct >= TAKE_PROFIT1 and close_price > min(sma50, sma150) and raw_std35 < last_5m_close * 0.01:
            if place_limit_order("sell", "TAKE_PROFIT1"):
                consecutive_stop_losses = 0
                save_state()
            return

        if EOD_START <= now <= EOD_END and pnl_pct > 0.01 and raw_std35 < last_5m_close * 0.008:
            if place_limit_order("sell", "EOD_PROFIT"):
                consecutive_stop_losses = 0
                save_state()
            return

        upper_band = max(sma50, sma150) + (0.6 * std35)
        if close_price > sma50 and close_price > sma150 and close_price > upper_band:
            if place_limit_order("sell", "SD_SELL"):
                consecutive_stop_losses = 0
                save_state()
            return

    if current_position is None:
        lower_ma = min(sma50, sma150)
        dynamic_lower_band = lower_ma - (entry_std_multiplier * std35)

        if close_price < dynamic_lower_band:
            if len(intraday_df) < 2:
                return
            prev_candle_close = intraday_df["close"].iloc[-2]
            if close_price > prev_candle_close:
                place_limit_order("buy", "SD_BUY")
                return
            
# ================= STREAM =================
async def handle_trade(trade):
    global live_price
    print(f"\n[TICK] {trade.price}")  # temp debug line
    live_price = float(trade.price)
    print_status()

# ================= MAIN =================
if __name__ == "__main__":
    load_state()
    sync_position()
    preload_intraday()
    fetch_prev_close()
    
    
    market_stream.subscribe_trades(handle_trade, TRADING_SYMBOL)

    import threading
    stream_thread = threading.Thread(target=market_stream.run, daemon=True)
    stream_thread.start()

    fill_thread = threading.Thread(target=fill_stream.run, daemon=True)
    fill_thread.start()

    asyncio.run(poll_5min_bars())