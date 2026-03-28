#!/usr/bin/env python3
"""
Memecoin Finder & Micro-Profit Sniper Bot
Standalone version for Railway / headless environments.
All configuration via environment variables.
"""

import os
import sys
import time
import datetime
import random
import json
import pickle
import threading
import requests
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# ============================================================================
# 0. Environment Configuration
# ============================================================================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    print("ERROR: TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID must be set.")
    sys.exit(1)

# Optional parameters with defaults
BUY_AMOUNT = float(os.getenv("BUY_AMOUNT", "2.0"))
PROFIT_TARGET_PCT = float(os.getenv("PROFIT_TARGET_PCT", "15.0"))
PROFIT_TARGET_ABS = float(os.getenv("PROFIT_TARGET_ABS", "0.75"))
TIMEOUT_MIN = int(os.getenv("TIMEOUT_MIN", "10"))
STOP_LOSS_PCT = float(os.getenv("STOP_LOSS_PCT", "20.0"))
MAX_POSITIONS = int(os.getenv("MAX_POSITIONS", "10"))
SCORE_THRESHOLD = int(os.getenv("SCORE_THRESHOLD", "50"))
MAX_RUN_HOURS = int(os.getenv("MAX_RUN_HOURS", "48"))   # 0 = infinite

# Filter defaults
AGE_MIN = int(os.getenv("AGE_MIN", "30"))
AGE_MAX = int(os.getenv("AGE_MAX", "300"))
LIQ_MIN = int(os.getenv("LIQ_MIN", "1000"))
LIQ_MAX = int(os.getenv("LIQ_MAX", "20000"))
VOLUME_5M = int(os.getenv("VOLUME_5M", "50"))
REQUIRE_SOCIAL = os.getenv("REQUIRE_SOCIAL", "false").lower() == "true"
CHAIN_SELECTOR = os.getenv("CHAIN_SELECTOR", "ALL")   # SOL, BSC, BASE, ALL

# Data persistence
DATA_PATH = Path("./data")
DATA_PATH.mkdir(exist_ok=True)

# ============================================================================
# 1. Helper: SOL price
# ============================================================================
SOL_USD = 130
SOL_LOCK = threading.Lock()

def get_sol_usd():
    global SOL_USD
    try:
        r = requests.get(
            "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd",
            timeout=5
        )
        if r.status_code == 200:
            with SOL_LOCK:
                SOL_USD = r.json()['solana']['usd']
    except Exception:
        pass
    return SOL_USD

# ============================================================================
# 2. Telegram Notifier (with retry)
# ============================================================================
class TelegramNotifier:
    def __init__(self, token, chat_id):
        self.token = token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{token}/sendMessage"

    def send(self, text, retries=3):
        if not self.token or not self.chat_id:
            return
        for attempt in range(retries):
            try:
                payload = {'chat_id': self.chat_id, 'text': text, 'parse_mode': 'HTML'}
                requests.post(self.base_url, data=payload, timeout=5)
                return
            except Exception as e:
                print(f"Telegram send failed (attempt {attempt+1}/{retries}): {e}")
                time.sleep(2)
        print("Telegram send failed after all retries.")

# ============================================================================
# 3. Detector Class (SOL + BSC + BASE)
# ============================================================================
class Detector:
    def __init__(self, max_mints=25000):
        self.seen_mints = set()
        self.max_mints = max_mints
        self.last_profile_fetch = 0

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json, text/plain, */*",
            "Origin": "https://dexscreener.com",
            "Referer": "https://dexscreener.com/",
        }

        self.pump_fun_url = "https://frontend-api-v3.pump.fun/coins?offset=0&limit=50&sort=created_timestamp&order=DESC"
        self.token_profile_url = "https://api.dexscreener.com/token-profiles/latest/v1"

    def get_new_pools(self, selected_chain):
        new_pools = []
        now = time.time()

        # 1. Pump.fun → SOL only
        if selected_chain in ['SOL', 'ALL']:
            try:
                resp = requests.get(self.pump_fun_url, headers=self.headers, timeout=12)
                if resp.status_code == 200:
                    data = resp.json()
                    with SOL_LOCK:
                        sol_usd = SOL_USD
                    for coin in data:
                        mint = coin.get('mint')
                        if not mint or mint in self.seen_mints:
                            continue
                        ts = coin.get('created_timestamp', now * 1000)
                        created_at = datetime.datetime.fromtimestamp(ts / 1000)
                        price_sol = float(coin.get('price', 0))
                        price_usd = price_sol * sol_usd
                        liquidity_raw = float(coin.get('liquidity', 0) or 5000)
                        liquidity_usd = liquidity_raw * sol_usd if liquidity_raw < 10000 else liquidity_raw
                        age_sec = max((now * 1000 - ts) / 1000, 60)
                        total_vol_usd = float(coin.get('volume', 0)) * sol_usd
                        volume_5m_usd = (total_vol_usd / age_sec) * 300
                        pool = {
                            'mint': mint,
                            'symbol': coin.get('symbol', '???'),
                            'name': coin.get('name', '???'),
                            'price': price_usd,
                            'liquidity': liquidity_usd,
                            'volume_5m': volume_5m_usd,
                            'created_at': created_at,
                            'socials': [coin.get('twitter'), coin.get('telegram')] if coin.get('twitter') or coin.get('telegram') else [],
                            'chain': 'SOL'
                        }
                        self.seen_mints.add(mint)
                        new_pools.append(pool)
            except Exception as e:
                print(f"Pump.fun error: {e}")

        # 2. DexScreener profiles → SOL + BSC + BASE
        if now - self.last_profile_fetch > 90:
            self.last_profile_fetch = now
            try:
                resp = requests.get(self.token_profile_url, headers=self.headers, timeout=10)
                if resp.status_code == 200:
                    data = resp.json()
                    for item in data:
                        mint = item.get('tokenAddress')
                        chain_id = item.get('chainId', '').lower()
                        if not mint or mint in self.seen_mints:
                            continue

                        if selected_chain != 'ALL':
                            if selected_chain == 'SOL' and chain_id != 'solana':
                                continue
                            if selected_chain == 'BSC' and chain_id != 'bsc':
                                continue
                            if selected_chain == 'BASE' and chain_id != 'base':
                                continue

                        pair_data = self._fetch_pair_info(mint, chain_id)
                        if pair_data:
                            pool = {
                                'mint': mint,
                                'symbol': pair_data.get('baseToken', {}).get('symbol', '???'),
                                'name': pair_data.get('baseToken', {}).get('name', '???'),
                                'price': float(pair_data.get('priceUsd', 0)),
                                'liquidity': float(pair_data.get('liquidity', {}).get('usd', 0)),
                                'volume_5m': float(pair_data.get('volume', {}).get('m5', 0)),
                                'created_at': datetime.datetime.fromtimestamp(pair_data.get('pairCreatedAt', now*1000) / 1000),
                                'socials': pair_data.get('info', {}).get('websites', []),
                                'chain': chain_id.upper() if chain_id in ['solana','bsc','base'] else 'SOL'
                            }
                            self.seen_mints.add(mint)
                            new_pools.append(pool)
            except Exception as e:
                print(f"DexScreener profiles error: {e}")

        if len(self.seen_mints) > self.max_mints:
            self.seen_mints = set(list(self.seen_mints)[-self.max_mints:])

        return new_pools

    def _fetch_pair_info(self, mint, chain_id, retries=2):
        chain_map = {'solana': 'solana', 'bsc': 'bsc', 'base': 'base'}
        chain_path = chain_map.get(chain_id, 'solana')
        url = f"https://api.dexscreener.com/token-pairs/v1/{chain_path}/{mint}"
        for attempt in range(retries):
            try:
                resp = requests.get(url, headers=self.headers, timeout=10)
                if resp.status_code == 429:
                    time.sleep(60 * (attempt + 1))
                    continue
                if not resp.text.strip():
                    return None
                data = resp.json()
                if isinstance(data, list) and data:
                    return data[0]
                return None
            except Exception:
                time.sleep(2)
        return None

    def fetch_prices_batch(self, mints, chain):
        if not mints:
            return {}
        chain_map = {'SOL': 'solana', 'BSC': 'bsc', 'BASE': 'base'}
        chain_path = chain_map.get(chain, 'solana')
        base_url = f"https://api.dexscreener.com/tokens/v1/{chain_path}/"
        chunk_size = 30
        all_prices = {}
        for i in range(0, len(mints), chunk_size):
            chunk = mints[i:i+chunk_size]
            url = base_url + ",".join(chunk)
            try:
                resp = requests.get(url, headers=self.headers, timeout=10)
                if resp.status_code == 429:
                    print("Batch fetch rate limited, sleeping 60s")
                    time.sleep(60)
                    continue
                data = resp.json()
                for pair in (data if isinstance(data, list) else []):
                    mint = pair.get('baseToken', {}).get('address')
                    if mint:
                        all_prices[mint] = {
                            'price': float(pair.get('priceUsd', 0)),
                            'liquidity': float(pair.get('liquidity', {}).get('usd', 0)),
                            'volume_5m': float(pair.get('volume', {}).get('m5', 0))
                        }
            except Exception as e:
                print(f"Batch price fetch error for chunk: {e}")
        return all_prices

# ============================================================================
# 4. Filter Engine
# ============================================================================
class FilterEngine:
    def __init__(self, config):
        self.config = config

    def filter_and_score(self, pool):
        try:
            age_seconds = (datetime.datetime.now() - pool['created_at']).total_seconds()
            if age_seconds < self.config['age_min'] or age_seconds > self.config['age_max']:
                return None
        except Exception as e:
            print(f"Age filter error: {e}")
            return None

        if pool['liquidity'] < self.config['liq_min'] or pool['liquidity'] > self.config['liq_max']:
            return None
        if pool.get('volume_5m', 0) < self.config['volume_5m']:
            return None
        if self.config['require_social'] and len(pool['socials']) == 0:
            return None

        score = 0
        liq_score = min(100, pool['liquidity'] / 5000 * 100)
        score += liq_score * 0.3
        vol_score = min(100, pool.get('volume_5m', 0) / 500 * 100)
        score += vol_score * 0.4
        social_score = min(30, len(pool['socials']) * 15)
        score += social_score
        pool['score'] = score
        return pool

# ============================================================================
# 5. Trade Simulator
# ============================================================================
class TradeSimulator:
    def __init__(self, initial_balance=100.0, buy_amount=1.0, profit_target_pct=15.0,
                 profit_target_abs=0.75, timeout_min=10, stop_loss_pct=20.0):
        self.balance = initial_balance
        self.initial_balance = initial_balance
        self.positions = {}
        self.trades = []
        self.buy_amount = buy_amount
        self.profit_target_pct = profit_target_pct
        self.profit_target_abs = profit_target_abs
        self.timeout_min = timeout_min
        self.stop_loss_pct = stop_loss_pct

    def can_buy(self, max_positions=10):
        return self.balance >= self.buy_amount and len(self.positions) < max_positions

    def simulate_buy(self, pool, max_pos=10):
        if not self.can_buy(max_pos):
            return None
        price = pool['price']
        if price <= 0:
            return None
        slippage = random.uniform(0.5, 1.0) / 100
        effective_price = price * (1 + slippage)
        amount_usd = min(self.buy_amount, self.balance)
        fee = amount_usd * 0.005
        amount_after_fee = amount_usd - fee
        quantity = amount_after_fee / effective_price
        self.balance -= amount_usd
        self.positions[pool['mint']] = {
            'symbol': pool['symbol'],
            'buy_price': effective_price,
            'buy_time': datetime.datetime.now(),
            'amount_usd': amount_usd,
            'quantity': quantity,
            'target_price': effective_price * (1 + self.profit_target_pct/100),
            'target_profit_usd': self.profit_target_abs,
            'stop_price': effective_price * (1 - self.stop_loss_pct/100),
            'timeout_at': datetime.datetime.now() + datetime.timedelta(minutes=self.timeout_min),
            'last_price_update': datetime.datetime.now(),
            'chain': pool.get('chain', 'SOL')
        }
        return {
            'symbol': pool['symbol'],
            'price': effective_price,
            'amount': amount_usd,
            'fee': fee,
            'quantity': quantity,
            'balance_after': self.balance
        }

    def update_price(self, mint, current_price):
        if mint in self.positions:
            self.positions[mint]['current_price'] = current_price
            self.positions[mint]['last_price_update'] = datetime.datetime.now()

    def _execute_sell(self, mint, pos, price, reason):
        slippage = random.uniform(0.5, 1.0) / 100
        effective_sell_price = price * (1 - slippage)
        fee = (effective_sell_price * pos['quantity']) * 0.005
        net_proceeds = effective_sell_price * pos['quantity'] - fee
        self.balance += net_proceeds
        profit = net_proceeds - pos['amount_usd']
        trade_record = {
            'timestamp': datetime.datetime.now(),
            'symbol': pos['symbol'],
            'buy_price': pos['buy_price'],
            'sell_price': effective_sell_price,
            'buy_amount_usd': pos['amount_usd'],
            'sell_amount_usd': net_proceeds,
            'profit_usd': profit,
            'reason': reason
        }
        self.trades.append(trade_record)
        del self.positions[mint]
        return {'symbol': pos['symbol'], 'profit': profit, 'reason': reason}

    def check_positions(self):
        executed_sells = []
        now = datetime.datetime.now()
        stale_threshold = datetime.timedelta(minutes=5)

        for mint, pos in list(self.positions.items()):
            if now >= pos['timeout_at']:
                price = pos.get('current_price', pos['buy_price'])
                result = self._execute_sell(mint, pos, price, f"timeout after {self.timeout_min} min")
                executed_sells.append(result)
                continue

            price = pos.get('current_price')
            if price is None or now - pos.get('last_price_update', now) > stale_threshold:
                continue

            current_value = price * pos['quantity']
            profit_usd = current_value - pos['amount_usd']
            profit_pct = (current_value / pos['amount_usd'] - 1) * 100
            reason = None
            if profit_usd >= self.profit_target_abs:
                reason = f"absolute profit ${profit_usd:.2f} >= target ${self.profit_target_abs}"
            elif profit_pct >= self.profit_target_pct:
                reason = f"profit {profit_pct:.1f}% >= target {self.profit_target_pct}%"
            elif price <= pos['stop_price']:
                reason = f"stop loss at {self.stop_loss_pct}%"
            if reason:
                result = self._execute_sell(mint, pos, price, reason)
                executed_sells.append(result)

        return executed_sells

# ============================================================================
# 6. Reporter (logs and saves state)
# ============================================================================
class Reporter:
    def __init__(self, telegram_notifier):
        self.telegram = telegram_notifier
        self.daily_summary = {'date': None, 'trades': 0, 'wins': 0, 'net_profit': 0}
        self.last_save = datetime.datetime.now()
        self.initial_balance = 100.0
        self.start_time = datetime.datetime.now()
        self.warned_live = False
        self.last_summary_date = None

    def log_detection(self, pool):
        msg = f"🔍 New coin detected [{pool['chain']}] {pool['symbol']} ({pool['name']}) | Mint: {pool['mint'][:8]}... | Score: {pool.get('score',0):.1f} | Liq: ${pool['liquidity']:,.0f}"
        print(msg)
        self.telegram.send(msg)

    def log_buy(self, pool, buy_result):
        msg = f"✅ SIMULATED BUY [{pool['chain']}] {pool['symbol']} @ ${buy_result['price']:.8f} | Amount: ${buy_result['amount']:.2f} | Fee: ${buy_result['fee']:.2f} | Balance: ${buy_result['balance_after']:.2f}"
        print(msg)
        self.telegram.send(msg)

    def log_sell(self, symbol, profit, reason):
        msg = f"💰 SIMULATED SELL {symbol} | Profit: ${profit:.2f} | Reason: {reason}"
        print(msg)
        self.telegram.send(msg)

    def log_portfolio(self, balance, positions):
        open_count = len(positions)
        msg = f"📊 Portfolio update | Balance: ${balance:.2f} | Open positions: {open_count}"
        print(msg)
        self.telegram.send(msg)

    def log_error(self, error_msg):
        msg = f"⚠️ Error: {error_msg}"
        print(msg)
        self.telegram.send(msg)

    def save_state(self, simulator, detector):
        state = {
            'simulator': {
                'balance': simulator.balance,
                'positions': simulator.positions,
                'trades': simulator.trades
            },
            'detector': {
                'seen_mints': list(detector.seen_mints)
            },
            'daily_summary': self.daily_summary,
            'start_time': self.start_time.isoformat()
        }
        with open(DATA_PATH / "state.pkl", 'wb') as f:
            pickle.dump(state, f)

    def load_state(self, simulator, detector):
        try:
            with open(DATA_PATH / "state.pkl", 'rb') as f:
                state = pickle.load(f)
            simulator.balance = state['simulator']['balance']
            simulator.positions = state['simulator']['positions']
            simulator.trades = state['simulator']['trades']
            detector.seen_mints = set(state['detector']['seen_mints'])
            self.daily_summary = state['daily_summary']
            if 'start_time' in state:
                self.start_time = datetime.datetime.fromisoformat(state['start_time'])
            print("Loaded previous state.")
        except FileNotFoundError:
            print("No previous state found, starting fresh.")

    def auto_save(self, simulator, detector):
        if (datetime.datetime.now() - self.last_save).total_seconds() > 300:
            self.save_state(simulator, detector)
            self.last_save = datetime.datetime.now()

    def update_daily_summary(self, trade_profit):
        today = datetime.datetime.now().date()
        if self.daily_summary['date'] != today:
            if self.daily_summary['date'] is not None:
                summary_msg = f"📅 Daily summary {self.daily_summary['date']}: Trades: {self.daily_summary['trades']}, Wins: {self.daily_summary['wins']}, Net profit: ${self.daily_summary['net_profit']:.2f}"
                print(summary_msg)
                self.telegram.send(summary_msg)
            self.daily_summary = {'date': today, 'trades': 0, 'wins': 0, 'net_profit': 0}
        self.daily_summary['trades'] += 1
        if trade_profit > 0:
            self.daily_summary['wins'] += 1
        self.daily_summary['net_profit'] += trade_profit

    def plot_equity(self):
        if not self.trades:
            return
        df = pd.DataFrame(self.trades)
        df['cumulative'] = df['profit_usd'].cumsum() + self.initial_balance
        plt.figure(figsize=(12,4))
        plt.plot(df['timestamp'], df['cumulative'], label='Equity')
        plt.axhline(y=self.initial_balance, color='r', linestyle='--', label='Initial')
        plt.xlabel('Time')
        plt.ylabel('Balance ($)')
        plt.title('Equity Curve')
        plt.legend()
        plt.grid(True)
        plt.savefig(DATA_PATH / "equity.png")
        plt.close()
        print("Equity curve saved to data/equity.png")

# ============================================================================
# 7. Keep‑alive heartbeat
# ============================================================================
def keep_alive():
    while True:
        time.sleep(540)
        print(f"[KEEPALIVE] {datetime.datetime.now().strftime('%H:%M:%S')} - runtime still active")
keep_alive_thread = threading.Thread(target=keep_alive, daemon=True)
keep_alive_thread.start()

# ============================================================================
# 8. Main Bot Loop
# ============================================================================
def run_bot():
    notifier = TelegramNotifier(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)
    reporter = Reporter(notifier)
    detector = Detector()
    simulator = TradeSimulator(
        initial_balance=100.0,
        buy_amount=BUY_AMOUNT,
        profit_target_pct=PROFIT_TARGET_PCT,
        profit_target_abs=PROFIT_TARGET_ABS,
        timeout_min=TIMEOUT_MIN,
        stop_loss_pct=STOP_LOSS_PCT
    )
    reporter.initial_balance = simulator.initial_balance
    reporter.trades = simulator.trades  # link for plotting

    reporter.load_state(simulator, detector)

    start_time = reporter.start_time
    last_portfolio_update = datetime.datetime.now()
    last_sol_update = datetime.datetime.now()
    get_sol_usd()

    print("🚀 Bot started (dry‑run mode, virtual $100). Sending Telegram messages...")
    notifier.send("🚀 Bot started. Dry‑run mode with $100 simulated balance.")

    filter_config = {
        'age_min': AGE_MIN,
        'age_max': AGE_MAX,
        'liq_min': LIQ_MIN,
        'liq_max': LIQ_MAX,
        'volume_5m': VOLUME_5M,
        'require_social': REQUIRE_SOCIAL
    }
    filter_engine = FilterEngine(filter_config)

    while True:
        # 48-hour timer (if set)
        if MAX_RUN_HOURS > 0:
            elapsed = datetime.datetime.now() - start_time
            if elapsed.total_seconds() > MAX_RUN_HOURS * 3600:
                final_profit = simulator.balance - simulator.initial_balance
                msg = f"✅ {MAX_RUN_HOURS}‑hour dry‑run complete! Simulated profit: ${final_profit:.2f}."
                print(msg)
                notifier.send(msg)
                break

        # Refresh SOL price every 30 min
        if (datetime.datetime.now() - last_sol_update).total_seconds() > 1800:
            get_sol_usd()
            last_sol_update = datetime.datetime.now()

        # Detection
        try:
            new_pools = detector.get_new_pools(CHAIN_SELECTOR)
        except Exception as e:
            reporter.log_error(f"Detection error: {e}")
            time.sleep(30)
            continue

        for pool in new_pools:
            filtered = filter_engine.filter_and_score(pool)
            if filtered and filtered['score'] >= SCORE_THRESHOLD:
                reporter.log_detection(filtered)
                notifier.send(f"📈 BUY SIGNAL [{filtered['chain']}] {filtered['symbol']} | Score: {filtered['score']:.1f} | Liq: ${filtered['liquidity']:,.0f}")
                if simulator.can_buy(MAX_POSITIONS):
                    buy_result = simulator.simulate_buy(filtered, MAX_POSITIONS)
                    if buy_result:
                        reporter.log_buy(filtered, buy_result)
                    else:
                        reporter.log_error(f"Insufficient balance to buy {filtered['symbol']}")
                else:
                    print(f"Skipping {filtered['symbol']} — max positions ({MAX_POSITIONS}) reached")

        # Update prices for open positions
        active_positions = simulator.positions.items()
        if active_positions:
            positions_by_chain = {}
            for mint, pos in active_positions:
                chain = pos.get('chain', 'SOL')
                positions_by_chain.setdefault(chain, []).append(mint)

            for chain, mints in positions_by_chain.items():
                price_data = detector.fetch_prices_batch(mints, chain)
                for mint, data in price_data.items():
                    simulator.update_price(mint, data['price'])

        # Check for sells
        sells = simulator.check_positions()
        for sell in sells:
            reporter.log_sell(sell['symbol'], sell['profit'], sell['reason'])
            reporter.update_daily_summary(sell['profit'])

        # Periodic portfolio update
        if (datetime.datetime.now() - last_portfolio_update).total_seconds() > 300:
            reporter.log_portfolio(simulator.balance, simulator.positions)
            last_portfolio_update = datetime.datetime.now()

        # Auto save state
        reporter.auto_save(simulator, detector)

        # Status log
        print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] Balance: ${simulator.balance:.2f} | Trades: {len(simulator.trades)} | Open: {len(simulator.positions)}")
        time.sleep(15)

    # Final plot and exit
    reporter.plot_equity()

# ============================================================================
# 9. Start
# ============================================================================
if __name__ == "__main__":
    run_bot()
