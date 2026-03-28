import os
import sys
import time
import datetime
import random
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
TELEGRAM_CHAT_IDS  = [c.strip() for c in os.getenv("TELEGRAM_CHAT_IDS", "").split(",") if c.strip()]

if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_IDS:
    print("ERROR: TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_IDS must be set.")
    print("TELEGRAM_CHAT_IDS should be comma-separated: 123456,789012")
    sys.exit(1)

BUY_AMOUNT           = float(os.getenv("BUY_AMOUNT", "2.0"))
PROFIT_TARGET_PCT    = float(os.getenv("PROFIT_TARGET_PCT", "15.0"))
PROFIT_TARGET_ABS    = float(os.getenv("PROFIT_TARGET_ABS", "0.75"))
TIMEOUT_MIN          = int(os.getenv("TIMEOUT_MIN", "10"))
STOP_LOSS_PCT        = float(os.getenv("STOP_LOSS_PCT", "10.0"))
MAX_POSITIONS        = int(os.getenv("MAX_POSITIONS", "10"))
SCORE_THRESHOLD      = int(os.getenv("SCORE_THRESHOLD", "20"))  # FIX: lowered from 40
MAX_RUN_HOURS        = int(os.getenv("MAX_RUN_HOURS", "48"))

AGE_MIN              = int(os.getenv("AGE_MIN", "10"))     # FIX: lowered from 15
AGE_MAX              = int(os.getenv("AGE_MAX", "900"))    # FIX: raised from 600 → 15 min
LIQ_MIN              = int(os.getenv("LIQ_MIN", "500"))    # FIX: lowered from 1000
LIQ_MAX              = int(os.getenv("LIQ_MAX", "50000"))  # FIX: raised from 20000
VOLUME_5M            = int(os.getenv("VOLUME_5M", "10"))   # FIX: lowered from 50
REQUIRE_SOCIAL       = os.getenv("REQUIRE_SOCIAL", "false").lower() == "true"
CHAIN_SELECTOR       = os.getenv("CHAIN_SELECTOR", "ALL")

DAILY_LOSS_LIMIT     = float(os.getenv("DAILY_LOSS_LIMIT", "-15"))
CONSECUTIVE_LOSS_LIMIT = int(os.getenv("CONSECUTIVE_LOSS_LIMIT", "3"))

DATA_PATH = Path("./data")
DATA_PATH.mkdir(exist_ok=True)

# ============================================================================
# 1. SOL price helper
# ============================================================================
SOL_USD  = 130
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
# 2. Telegram Notifier — multiple chat IDs
# ============================================================================
class TelegramNotifier:
    def __init__(self, token, chat_ids):
        self.token    = token
        self.chat_ids = chat_ids  # list of strings
        self.base_url = f"https://api.telegram.org/bot{token}/sendMessage"

    def send(self, text, retries=3):
        if not self.token or not self.chat_ids:
            return
        for chat_id in self.chat_ids:
            for attempt in range(retries):
                try:
                    requests.post(
                        self.base_url,
                        data={'chat_id': chat_id, 'text': text, 'parse_mode': 'HTML'},
                        timeout=5
                    )
                    break
                except Exception as e:
                    print(f"Telegram failed [{chat_id}] attempt {attempt+1}: {e}")
                    time.sleep(2)

# ============================================================================
# 3. Detector
# ============================================================================
class Detector:
    def __init__(self, max_mints=25000):
        self.seen_mints        = set()
        self.max_mints         = max_mints
        self.last_profile_fetch = 0
        self.last_search_fetch  = 0
        self.pump_fun_urls = [
            "https://frontend-api-v3.pump.fun/coins?offset=0&limit=50&sort=created_timestamp&order=DESC",
            "https://frontend-api.pump.fun/coins?offset=0&limit=50&sort=created_timestamp&order=DESC"
        ]
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept":     "application/json, text/plain, */*",
            "Origin":     "https://dexscreener.com",
            "Referer":    "https://dexscreener.com/",
        }
        self.token_profile_url    = "https://api.dexscreener.com/token-profiles/latest/v1"
        self.dexscreener_search_url = "https://api.dexscreener.com/latest/dex/search?q=pump"

    def get_new_pools(self, selected_chain):
        new_pools   = []
        now         = time.time()
        pf_fetched  = 0
        pf_new      = 0
        pf_skipped  = 0

        # 1. Pump.fun
        if selected_chain in ['SOL', 'ALL']:
            for pf_url in self.pump_fun_urls:
                try:
                    resp = requests.get(pf_url, headers=self.headers, timeout=12)
                    if resp.status_code == 200 and resp.text.strip():
                        data = resp.json()
                        if data:
                            with SOL_LOCK:
                                sol_usd = SOL_USD
                            pf_fetched = len(data)
                            for coin in data:
                                mint = coin.get('mint')
                                if not mint:
                                    continue
                                if mint in self.seen_mints:
                                    pf_skipped += 1
                                    continue
                                liquidity_raw = float(coin.get('liquidity', 0) or 0)
                                if liquidity_raw == 0:
                                    continue
                                ts          = coin.get('created_timestamp', now * 1000)
                                created_at  = datetime.datetime.fromtimestamp(ts / 1000)
                                price_usd   = float(coin.get('price', 0)) * sol_usd
                                liq_usd     = liquidity_raw * sol_usd if liquidity_raw < 10000 else liquidity_raw
                                age_sec     = max((now * 1000 - ts) / 1000, 60)
                                vol_5m      = (float(coin.get('volume', 0)) * sol_usd / age_sec) * 300
                                pool = {
                                    'mint':           mint,
                                    'symbol':         coin.get('symbol', '???'),
                                    'name':           coin.get('name', '???'),
                                    'price':          price_usd,
                                    'liquidity':      liq_usd,
                                    'volume_5m':      vol_5m,
                                    'created_at':     created_at,
                                    'socials':        [x for x in [coin.get('twitter'), coin.get('telegram')] if x],
                                    'chain':          'SOL',
                                    'price_change_5m': 0,
                                    'buys_5m':        0,
                                    'source':         'pumpfun'
                                }
                                self.seen_mints.add(mint)
                                new_pools.append(pool)
                                pf_new += 1
                            break
                except Exception as e:
                    print(f"Pump.fun error: {e}")
                    continue
            # FIX: diagnostic log
            print(f"[DETECT] pump.fun: fetched={pf_fetched} new={pf_new} already_seen={pf_skipped}")

        # 2. DexScreener token profiles (every 60s)
        if now - self.last_profile_fetch > 60:
            self.last_profile_fetch = now
            ds_new = 0
            try:
                resp = requests.get(self.token_profile_url, headers=self.headers, timeout=10)
                if resp.status_code == 200:
                    data = resp.json()
                    chain_label_map = {'solana': 'SOL', 'bsc': 'BSC', 'base': 'BASE'}
                    for item in data:
                        mint     = item.get('tokenAddress')
                        chain_id = item.get('chainId', '').lower()
                        if not mint or mint in self.seen_mints:
                            continue
                        if selected_chain != 'ALL':
                            wanted = {'SOL': 'solana', 'BSC': 'bsc', 'BASE': 'base'}.get(selected_chain)
                            if chain_id != wanted:
                                continue
                        pair_data = self._fetch_pair_info(mint, chain_id)
                        if pair_data:
                            pool = {
                                'mint':            mint,
                                'symbol':          pair_data.get('baseToken', {}).get('symbol', '???'),
                                'name':            pair_data.get('baseToken', {}).get('name', '???'),
                                'price':           float(pair_data.get('priceUsd', 0)),
                                'liquidity':       float(pair_data.get('liquidity', {}).get('usd', 0)),
                                'volume_5m':       float(pair_data.get('volume', {}).get('m5', 0)),
                                'price_change_5m': float(pair_data.get('priceChange', {}).get('m5', 0)),
                                'buys_5m':         pair_data.get('txns', {}).get('m5', {}).get('buys', 0),
                                'created_at':      datetime.datetime.fromtimestamp(pair_data.get('pairCreatedAt', now*1000) / 1000),
                                'socials':         pair_data.get('info', {}).get('websites', []),
                                'chain':           chain_label_map.get(chain_id, 'SOL'),
                                'source':          'dexscreener_profiles'
                            }
                            self.seen_mints.add(mint)
                            new_pools.append(pool)
                            ds_new += 1
            except Exception as e:
                print(f"DexScreener profiles error: {e}")
            print(f"[DETECT] dexscreener profiles: new={ds_new}")

        # 3. DexScreener search (every 60s)
        if now - self.last_search_fetch > 60:
            self.last_search_fetch = now
            sr_new = 0
            try:
                resp = requests.get(self.dexscreener_search_url, headers=self.headers, timeout=10)
                if resp.status_code == 200:
                    chain_label_map = {'solana': 'SOL', 'bsc': 'BSC', 'base': 'BASE'}
                    for p in resp.json().get('pairs', []):
                        chain_id = p.get('chainId', '').lower()
                        chain_ok = (
                            selected_chain == 'ALL' or
                            (selected_chain == 'SOL'  and chain_id == 'solana') or
                            (selected_chain == 'BSC'  and chain_id == 'bsc')    or
                            (selected_chain == 'BASE' and chain_id == 'base')
                        )
                        if not chain_ok:
                            continue
                        mint = p.get('baseToken', {}).get('address')
                        if not mint or mint in self.seen_mints:
                            continue
                        pool = {
                            'mint':            mint,
                            'symbol':          p.get('baseToken', {}).get('symbol', '???'),
                            'name':            p.get('baseToken', {}).get('name', '???'),
                            'price':           float(p.get('priceUsd', 0)),
                            'liquidity':       float(p.get('liquidity', {}).get('usd', 0)),
                            'volume_5m':       float(p.get('volume', {}).get('m5', 0)),
                            'price_change_5m': float(p.get('priceChange', {}).get('m5', 0)),
                            'buys_5m':         p.get('txns', {}).get('m5', {}).get('buys', 0),
                            'created_at':      datetime.datetime.fromtimestamp(p.get('pairCreatedAt', now*1000) / 1000),
                            'socials':         p.get('info', {}).get('websites', []),
                            'chain':           chain_label_map.get(chain_id, 'SOL'),
                            'source':          'dexscreener_search'
                        }
                        self.seen_mints.add(mint)
                        new_pools.append(pool)
                        sr_new += 1
            except Exception as e:
                print(f"DexScreener search error: {e}")
            print(f"[DETECT] dexscreener search: new={sr_new}")

        if len(self.seen_mints) > self.max_mints:
            self.seen_mints = set(list(self.seen_mints)[-self.max_mints:])

        return new_pools

    def _fetch_pair_info(self, mint, chain_id, retries=2):
        chain_path = {'solana': 'solana', 'bsc': 'bsc', 'base': 'base'}.get(chain_id, 'solana')
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
            except Exception:
                time.sleep(2)
        return None

    def fetch_prices_batch(self, mints, chain):
        if not mints:
            return {}
        chain_path = {'SOL': 'solana', 'BSC': 'bsc', 'BASE': 'base'}.get(chain, 'solana')
        all_prices = {}
        for i in range(0, len(mints), 30):
            chunk = mints[i:i+30]
            try:
                resp = requests.get(
                    f"https://api.dexscreener.com/tokens/v1/{chain_path}/{','.join(chunk)}",
                    headers=self.headers, timeout=10
                )
                if resp.status_code == 429:
                    time.sleep(60)
                    continue
                for pair in (resp.json() if isinstance(resp.json(), list) else []):
                    mint = pair.get('baseToken', {}).get('address')
                    if mint:
                        all_prices[mint] = {'price': float(pair.get('priceUsd', 0))}
            except Exception as e:
                print(f"Batch price error: {e}")
        return all_prices

# ============================================================================
# 4. Filter Engine — with diagnostic logging
# ============================================================================
class FilterEngine:
    def __init__(self, config):
        self.config = config

    def filter_and_score(self, pool):
        # Age
        try:
            age = (datetime.datetime.now() - pool['created_at']).total_seconds()
            if age < self.config['age_min'] or age > self.config['age_max']:
                return None, f"age={age:.0f}s out of [{self.config['age_min']},{self.config['age_max']}]"
        except Exception as e:
            return None, f"age_error={e}"

        if pool['liquidity'] < self.config['liq_min'] or pool['liquidity'] > self.config['liq_max']:
            return None, f"liq=${pool['liquidity']:.0f} out of [{self.config['liq_min']},{self.config['liq_max']}]"

        if pool.get('volume_5m', 0) < self.config['volume_5m']:
            return None, f"vol5m=${pool.get('volume_5m',0):.1f} < {self.config['volume_5m']}"

        if self.config['require_social'] and len(pool['socials']) == 0:
            return None, "no_socials"

        # Rug filter
        vol_liq = (pool.get('volume_5m', 0) * 288) / max(pool['liquidity'], 1)
        if vol_liq > 50:
            return None, f"rug_vol_liq={vol_liq:.1f}"

        # Score
        liq_score  = min(100, pool['liquidity'] / 5000 * 100) * 0.3
        vol_score  = min(100, pool.get('volume_5m', 0) / 500 * 100) * 0.4
        soc_score  = min(30,  len(pool['socials']) * 15)
        mom        = pool.get('price_change_5m', 0)
        mom_score  = 20 if mom > 10 else (10 if mom > 5 else (-15 if mom < -5 else 0))
        tx_score   = min(20, pool.get('buys_5m', 0) / 10)
        score      = liq_score + vol_score + soc_score + mom_score + tx_score
        pool['score'] = score
        return pool, "pass"

# ============================================================================
# 5. Trade Simulator
# ============================================================================
class TradeSimulator:
    def __init__(self, notifier, initial_balance=100.0, buy_amount=1.0,
                 profit_target_pct=15.0, profit_target_abs=0.75,
                 timeout_min=10, stop_loss_pct=10.0):
        self.notifier             = notifier
        self.balance              = initial_balance
        self.initial_balance      = initial_balance
        self.positions            = {}
        self.trades               = []
        self.buy_amount           = buy_amount
        self.base_profit_pct      = profit_target_pct
        self.base_profit_abs      = profit_target_abs
        self.base_timeout         = timeout_min
        self.base_stop_pct        = stop_loss_pct

    def _get_targets(self, score):
        if score >= 80:
            return {'profit_pct': 60.0, 'profit_abs': 2.0, 'stop_pct': 15.0, 'timeout': 20}
        elif score >= 65:
            return {'profit_pct': 30.0, 'profit_abs': 1.0, 'stop_pct': 12.0, 'timeout': 15}
        return {'profit_pct': self.base_profit_pct, 'profit_abs': self.base_profit_abs,
                'stop_pct': self.base_stop_pct, 'timeout': self.base_timeout}

    def get_buy_amount(self, score):
        if score >= 80: return self.buy_amount * 2.0
        if score >= 65: return self.buy_amount * 1.5
        return self.buy_amount

    def can_buy(self, max_positions=10, score=0):
        return min(self.get_buy_amount(score), self.balance) >= 0.5 and len(self.positions) < max_positions

    def simulate_buy(self, pool, max_pos=10):
        score = pool.get('score', 50)
        if not self.can_buy(max_pos, score):
            return None
        amount = min(self.get_buy_amount(score), self.balance)
        if amount < 0.5 or pool['price'] <= 0:
            return None
        ep  = pool['price'] * (1 + random.uniform(0.5, 1.0) / 100)
        fee = amount * 0.005
        qty = (amount - fee) / ep
        self.balance -= amount
        t   = self._get_targets(score)
        self.positions[pool['mint']] = {
            'symbol': pool['symbol'], 'buy_price': ep, 'buy_time': datetime.datetime.now(),
            'amount_usd': amount, 'quantity': qty,
            'target_profit_usd': t['profit_abs'],
            'stop_price': ep * (1 - t['stop_pct'] / 100),
            'timeout_at': datetime.datetime.now() + datetime.timedelta(minutes=t['timeout']),
            'last_price_update': datetime.datetime.now(),
            'chain': pool.get('chain', 'SOL'), 'entry_score': score,
            'partial_sold': False, 'targets': t
        }
        return {'symbol': pool['symbol'], 'price': ep, 'amount': amount,
                'fee': fee, 'quantity': qty, 'balance_after': self.balance}

    def update_price(self, mint, price):
        if mint in self.positions:
            self.positions[mint]['current_price'] = price
            self.positions[mint]['last_price_update'] = datetime.datetime.now()

    def update_trailing_stop(self, mint):
        pos = self.positions.get(mint)
        if not pos or pos.get('entry_score', 0) < 65:
            return
        price = pos.get('current_price')
        if not price:
            return
        new_stop = price * (1 - pos['targets']['stop_pct'] / 100)
        if new_stop > pos['stop_price']:
            self.positions[mint]['stop_price'] = new_stop

    def _execute_sell(self, mint, pos, price, reason, partial=False):
        esp     = price * (1 - random.uniform(0.5, 1.0) / 100)
        fee     = esp * pos['quantity'] * 0.005
        proceeds = esp * pos['quantity'] - fee
        self.balance += proceeds
        profit  = proceeds - pos['amount_usd']
        pct     = (proceeds / pos['amount_usd'] - 1) * 100
        self.trades.append({
            'timestamp': datetime.datetime.now(), 'symbol': pos['symbol'], 'mint': mint,
            'buy_price': pos['buy_price'], 'sell_price': esp,
            'buy_amount_usd': pos['amount_usd'], 'sell_amount_usd': proceeds,
            'profit_usd': profit, 'reason': reason,
            'entry_score': pos.get('entry_score', 0), 'chain': pos.get('chain', 'SOL'),
            'partial': partial
        })
        if partial:
            self.positions[mint]['partial_sold'] = True
        else:
            del self.positions[mint]
        emoji = "✅ WIN" if profit > 0 else "❌ LOSS"
        label = "PARTIAL SELL" if partial else "TRADE CLOSED"
        msg = (
            f"{emoji} {label}\n"
            f"🪙 {pos['symbol']} [{pos.get('chain','SOL')}]\n"
            f"📋 CA: <code>{mint}</code>\n"
            f"📥 Buy:  ${pos['buy_price']:.8f}\n"
            f"📤 Sell: ${esp:.8f}\n"
            f"📊 P&L:  ${profit:+.2f} ({pct:+.1f}%)\n"
            f"🎯 Score: {pos.get('entry_score',0):.0f} | Balance: ${self.balance:.2f}\n"
            f"📝 {reason}"
        )
        if partial:
            msg += "\n⚡ 50% closed, holding rest"
        self.notifier.send(msg)
        return {'symbol': pos['symbol'], 'profit': profit, 'reason': reason, 'partial': partial}

    def check_positions(self):
        sells = []
        now   = datetime.datetime.now()
        stale = datetime.timedelta(minutes=5)
        for mint, pos in list(self.positions.items()):
            if now >= pos['timeout_at']:
                sells.append(self._execute_sell(mint, pos,
                    pos.get('current_price', pos['buy_price']),
                    f"timeout after {pos['targets']['timeout']} min"))
                continue
            price = pos.get('current_price')
            if price is None or now - pos.get('last_price_update', now) > stale:
                continue
            pv   = price * pos['quantity']
            pu   = pv - pos['amount_usd']
            ppct = (pv / pos['amount_usd'] - 1) * 100
            # Partial sell
            if pos.get('entry_score', 0) >= 80 and not pos.get('partial_sold') and ppct >= 30:
                hq, hc = pos['quantity']/2, pos['amount_usd']/2
                hp = {**pos, 'quantity': hq, 'amount_usd': hc}
                self.positions[mint].update({'quantity': hq, 'amount_usd': hc,
                    'partial_sold': True, 'stop_price': pos['buy_price']})
                sells.append(self._execute_sell(mint, hp, price, "partial sell at +30%", partial=True))
                continue
            reason = None
            if pu   >= pos['target_profit_usd']:             reason = f"abs profit ${pu:.2f}"
            elif ppct >= pos['targets']['profit_pct']:       reason = f"profit {ppct:.1f}%"
            elif price <= pos['stop_price']:                 reason = f"stop loss {pos['targets']['stop_pct']}%"
            if reason:
                sells.append(self._execute_sell(mint, pos, price, reason))
        return sells

# ============================================================================
# 6. Reporter
# ============================================================================
class Reporter:
    def __init__(self, notifier):
        self.telegram          = notifier
        self.daily_summary     = {'date': None, 'trades': 0, 'wins': 0, 'net_profit': 0}
        self.last_save         = datetime.datetime.now()
        self.initial_balance   = 100.0
        self.start_time        = datetime.datetime.now()
        self.consecutive_losses = 0

    def log_detection(self, pool):
        print(f"🔍 [{pool['chain']}] {pool['symbol']} score={pool.get('score',0):.1f} liq=${pool['liquidity']:,.0f} src={pool.get('source','?')}")

    def log_buy(self, pool, r):
        msg = (f"✅ BUY [{pool['chain']}] {pool['symbol']}\n"
               f"📋 CA: <code>{pool['mint']}</code>\n"
               f"Price: ${r['price']:.8f} | Amount: ${r['amount']:.2f} | Balance: ${r['balance_after']:.2f}")
        print(msg)
        self.telegram.send(msg)

    def log_portfolio(self, balance, positions):
        msg = f"📊 Balance: ${balance:.2f} | Open: {len(positions)}"
        print(msg)
        self.telegram.send(msg)

    def log_error(self, msg):
        print(f"⚠️ {msg}")
        self.telegram.send(f"⚠️ {msg}")

    def save_state(self, sim, det):
        pickle.dump({
            'simulator': {'balance': sim.balance, 'positions': sim.positions, 'trades': sim.trades},
            'detector':  {'seen_mints': list(det.seen_mints)},
            'daily_summary': self.daily_summary,
            'start_time': self.start_time.isoformat(),
            'consecutive_losses': self.consecutive_losses
        }, open(DATA_PATH / "state.pkl", 'wb'))

    def load_state(self, sim, det):
        try:
            s = pickle.load(open(DATA_PATH / "state.pkl", 'rb'))
            sim.balance, sim.positions, sim.trades = s['simulator']['balance'], s['simulator']['positions'], s['simulator']['trades']
            det.seen_mints    = set(s['detector']['seen_mints'])
            self.daily_summary = s['daily_summary']
            self.start_time    = datetime.datetime.fromisoformat(s.get('start_time', self.start_time.isoformat()))
            self.consecutive_losses = s.get('consecutive_losses', 0)
            print("Loaded previous state.")
        except FileNotFoundError:
            print("Starting fresh.")

    def auto_save(self, sim, det):
        if (datetime.datetime.now() - self.last_save).total_seconds() > 300:
            self.save_state(sim, det)
            self.last_save = datetime.datetime.now()

    def update_daily_summary(self, profit):
        today = datetime.datetime.now().date()
        if self.daily_summary['date'] != today:
            if self.daily_summary['date']:
                msg = f"📅 {self.daily_summary['date']}: {self.daily_summary['trades']} trades, {self.daily_summary['wins']} wins, ${self.daily_summary['net_profit']:.2f}"
                print(msg); self.telegram.send(msg)
            self.daily_summary = {'date': today, 'trades': 0, 'wins': 0, 'net_profit': 0}
        self.daily_summary['trades'] += 1
        if profit > 0:
            self.daily_summary['wins'] += 1
            self.consecutive_losses = 0
        else:
            self.consecutive_losses += 1
        self.daily_summary['net_profit'] += profit

    def plot_equity(self, trades):
        full = [t for t in trades if not t.get('partial')]
        if not full: return
        df = pd.DataFrame(full)
        df['cumulative'] = df['profit_usd'].cumsum() + self.initial_balance
        plt.figure(figsize=(12,4))
        plt.plot(df['timestamp'], df['cumulative'], label='Equity')
        plt.axhline(y=self.initial_balance, color='r', linestyle='--', label='Initial')
        plt.title('Equity Curve'); plt.xlabel('Time'); plt.ylabel('Balance ($)')
        plt.legend(); plt.grid(True)
        plt.savefig(DATA_PATH / "equity.png"); plt.close()
        print("Equity curve saved.")

# ============================================================================
# 7. Keep-alive
# ============================================================================
def keep_alive():
    while True:
        time.sleep(540)
        print(f"[KEEPALIVE] {datetime.datetime.now().strftime('%H:%M:%S')}")
threading.Thread(target=keep_alive, daemon=True).start()

# ============================================================================
# 8. Main Loop
# ============================================================================
def run_bot():
    notifier  = TelegramNotifier(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_IDS)
    reporter  = Reporter(notifier)
    detector  = Detector()
    simulator = TradeSimulator(notifier, initial_balance=100.0, buy_amount=BUY_AMOUNT,
                               profit_target_pct=PROFIT_TARGET_PCT, profit_target_abs=PROFIT_TARGET_ABS,
                               timeout_min=TIMEOUT_MIN, stop_loss_pct=STOP_LOSS_PCT)
    reporter.initial_balance = simulator.initial_balance
    reporter.load_state(simulator, detector)

    start_time          = reporter.start_time
    last_portfolio_upd  = datetime.datetime.now()
    last_sol_upd        = datetime.datetime.now()
    buy_pause_until     = None
    get_sol_usd()

    print(f"🚀 Bot started | Notifying {len(TELEGRAM_CHAT_IDS)} chat(s)")
    notifier.send(f"🚀 Bot started. Dry-run $100. Notifying {len(TELEGRAM_CHAT_IDS)} recipient(s).")

    filter_engine = FilterEngine({
        'age_min': AGE_MIN, 'age_max': AGE_MAX,
        'liq_min': LIQ_MIN, 'liq_max': LIQ_MAX,
        'volume_5m': VOLUME_5M, 'require_social': REQUIRE_SOCIAL
    })

    while True:
        if MAX_RUN_HOURS > 0:
            elapsed = (datetime.datetime.now() - start_time).total_seconds()
            if elapsed > MAX_RUN_HOURS * 3600:
                msg = f"✅ {MAX_RUN_HOURS}h complete! P&L: ${simulator.balance - simulator.initial_balance:.2f}"
                print(msg); notifier.send(msg); break

        if (datetime.datetime.now() - last_sol_upd).total_seconds() > 1800:
            get_sol_usd(); last_sol_upd = datetime.datetime.now()

        try:
            new_pools = detector.get_new_pools(CHAIN_SELECTOR)
        except Exception as e:
            reporter.log_error(f"Detection error: {e}"); time.sleep(30); continue

        # FIX: log filter results per cycle
        passed = rejected = 0
        reject_reasons = {}

        daily_loss = reporter.daily_summary.get('net_profit', 0)
        can_buy_now = (
            daily_loss >= DAILY_LOSS_LIMIT and
            (buy_pause_until is None or datetime.datetime.now() >= buy_pause_until)
        )

        if buy_pause_until and datetime.datetime.now() >= buy_pause_until:
            buy_pause_until = None; print("▶️ Buy pause lifted")

        if reporter.consecutive_losses >= CONSECUTIVE_LOSS_LIMIT and buy_pause_until is None:
            print(f"⛔ {reporter.consecutive_losses} losses — pausing 30 min")
            buy_pause_until = datetime.datetime.now() + datetime.timedelta(minutes=30)
            reporter.consecutive_losses = 0
            reporter.save_state(simulator, detector)
            can_buy_now = False

        for pool in new_pools:
            filtered, reason = filter_engine.filter_and_score(pool)
            if not filtered:
                rejected += 1
                reject_reasons[reason] = reject_reasons.get(reason, 0) + 1
                continue
            passed += 1
            if filtered['score'] < SCORE_THRESHOLD:
                reject_reasons[f"score={filtered['score']:.0f}<{SCORE_THRESHOLD}"] = \
                    reject_reasons.get(f"score={filtered['score']:.0f}<{SCORE_THRESHOLD}", 0) + 1
                continue
            reporter.log_detection(filtered)
            notifier.send(
                f"📈 BUY SIGNAL [{filtered['chain']}] {filtered['symbol']} ({filtered['name']})\n"
                f"📋 CA: <code>{filtered['mint']}</code>\n"
                f"Score: {filtered['score']:.1f} | Liq: ${filtered['liquidity']:,.0f} | Src: {filtered.get('source','?')}"
            )
            if can_buy_now and simulator.can_buy(MAX_POSITIONS, filtered.get('score', 0)):
                r = simulator.simulate_buy(filtered, MAX_POSITIONS)
                if r: reporter.log_buy(filtered, r)
            elif not can_buy_now:
                print(f"Skipping buy — paused/limit")
            else:
                print(f"Skipping {filtered['symbol']} — max positions reached")

        # FIX: always print filter summary so you know what's happening
        if new_pools:
            print(f"[FILTER] {len(new_pools)} candidates → {passed} passed filter → reject reasons: {reject_reasons}")
        else:
            print(f"[FILTER] 0 new candidates this cycle")

        # Price updates
        for chain, mints in {pos.get('chain','SOL'): [] for pos in simulator.positions.values()}.items():
            mints = [m for m, p in simulator.positions.items() if p.get('chain','SOL') == chain]
            for mint, data in detector.fetch_prices_batch(mints, chain).items():
                simulator.update_price(mint, data['price'])
                simulator.update_trailing_stop(mint)

        for sell in simulator.check_positions():
            if not sell.get('partial'):
                reporter.update_daily_summary(sell['profit'])

        if (datetime.datetime.now() - last_portfolio_upd).total_seconds() > 300:
            reporter.log_portfolio(simulator.balance, simulator.positions)
            last_portfolio_upd = datetime.datetime.now()

        reporter.auto_save(simulator, detector)

        full  = [t for t in simulator.trades if not t.get('partial')]
        wins  = sum(1 for t in full if t['profit_usd'] > 0)
        wr    = (wins / len(full) * 100) if full else 0
        print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] "
              f"Balance: ${simulator.balance:.2f} | Trades: {len(full)} | WR: {wr:.0f}% | Open: {len(simulator.positions)}")
        time.sleep(15)

    reporter.plot_equity(simulator.trades)

if __name__ == "__main__":
    run_bot()
