import os
import sys
import time
import datetime
import random
import pickle
import logging
import threading
import requests
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from collections import defaultdict
from dataclasses import dataclass

# ============================================================================
# 0. Configuration & Logging
# ============================================================================
DATA_PATH = Path("./data")
DATA_PATH.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler(DATA_PATH / "bot.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class BotConfig:
    # Existing
    buy_amount: float = float(os.getenv("BUY_AMOUNT", "2.0"))
    profit_target_pct: float = float(os.getenv("PROFIT_TARGET_PCT", "15.0"))
    profit_target_abs: float = float(os.getenv("PROFIT_TARGET_ABS", "0.75"))
    timeout_min: int = int(os.getenv("TIMEOUT_MIN", "10"))
    stop_loss_pct: float = float(os.getenv("STOP_LOSS_PCT", "10.0"))
    max_positions: int = int(os.getenv("MAX_POSITIONS", "10"))
    score_threshold: int = int(os.getenv("SCORE_THRESHOLD", "20"))
    max_run_hours: int = int(os.getenv("MAX_RUN_HOURS", "48"))
    age_min: int = int(os.getenv("AGE_MIN", "10"))
    age_max: int = int(os.getenv("AGE_MAX", "900"))
    liq_min: int = int(os.getenv("LIQ_MIN", "500"))
    liq_max: int = int(os.getenv("LIQ_MAX", "50000"))
    volume_5m: int = int(os.getenv("VOLUME_5M", "10"))
    require_social: bool = os.getenv("REQUIRE_SOCIAL", "false").lower() == "true"
    chain_selector: str = os.getenv("CHAIN_SELECTOR", "ALL")
    daily_loss_limit: float = float(os.getenv("DAILY_LOSS_LIMIT", "-15"))
    consecutive_loss_limit: int = int(os.getenv("CONSECUTIVE_LOSS_LIMIT", "3"))
    min_buys_5m: int = int(os.getenv("MIN_BUYS_5M", "5"))          # raised from 0
    min_buy_usd: float = float(os.getenv("MIN_BUY_USD", "5.0"))    # NEW

    # Momentum scanner
    age_momentum_min: int = int(os.getenv("AGE_MOMENTUM_MIN", "900"))
    age_momentum_max: int = int(os.getenv("AGE_MOMENTUM_MAX", "43200"))
    volume_spike_mult: float = float(os.getenv("VOLUME_SPIKE_MULT", "2.0"))
    momentum_score_threshold: int = int(os.getenv("MOMENTUM_SCORE_THRESHOLD", "35"))
    min_buy_ratio: float = float(os.getenv("MIN_BUY_RATIO", "0.65"))
    birdeye_api_key: str = os.getenv("BIRDEYE_API_KEY", "")       # from Railway

config = BotConfig()

# ============================================================================
# 1. Price helpers (SOL + BNB)
# ============================================================================
SOL_USD = 130
SOL_LOCK = threading.Lock()
BNB_USD = 600
BNB_LOCK = threading.Lock()

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

def get_bnb_usd():
    global BNB_USD
    try:
        r = requests.get(
            "https://api.coingecko.com/api/v3/simple/price?ids=binancecoin&vs_currencies=usd",
            timeout=5
        )
        if r.status_code == 200:
            with BNB_LOCK:
                BNB_USD = r.json()['binancecoin']['usd']
    except Exception:
        pass
    return BNB_USD

# ============================================================================
# 2. Telegram Notifier
# ============================================================================
class TelegramNotifier:
    def __init__(self, token, chat_ids):
        self.token = token
        self.chat_ids = chat_ids
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
                    logger.warning(f"Telegram failed [{chat_id}] attempt {attempt+1}: {e}")
                    time.sleep(2)

# ============================================================================
# 3. Detector (New Launches + Momentum)
# ============================================================================
class Detector:
    def __init__(self, max_mints=25000):
        self.seen_mints = set()                    # global fallback
        self.seen_mints_per_chain = defaultdict(set)  # per-chain precision
        self.max_mints = max_mints
        self.last_profile_fetch = 0
        self.last_search_fetch = {}                # per-chain
        self.last_fourmeme_fetch = 0
        self.last_clanker_fetch = 0

        self.pump_fun_urls = [
            "https://frontend-api-v3.pump.fun/coins?offset=0&limit=50&sort=created_timestamp&order=DESC",
            "https://frontend-api.pump.fun/coins?offset=0&limit=50&sort=created_timestamp&order=DESC"
        ]
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json, text/plain, */*",
            "Origin": "https://dexscreener.com",
            "Referer": "https://dexscreener.com/",
        }
        self.token_profile_url = "https://api.dexscreener.com/token-profiles/latest/v1"
        self.search_urls = {
            'SOL': "https://api.dexscreener.com/latest/dex/search?q=pump",
            'BSC': "https://api.dexscreener.com/latest/dex/search?q=four.meme",
            'BASE': "https://api.dexscreener.com/latest/dex/search?q=zora+base"
        }
        self.fourmeme_url = "https://four.meme/meme-api/v1/meme/query?page=1&pageSize=50&sort=createTime&order=desc&status=1"
        self.clanker_search_url = "https://api.dexscreener.com/latest/dex/search?q=clanker"

    # ---- Exponential backoff with custom headers ----
    def _get_with_backoff(self, url, max_retries=4, extra_headers=None):
        wait = 2
        for attempt in range(max_retries):
            try:
                headers = self.headers.copy()
                if extra_headers:
                    headers.update(extra_headers)
                resp = requests.get(url, headers=headers, timeout=10)
                if resp.status_code == 429:
                    logger.warning(f"Rate limited on {url}, waiting {wait}s")
                    time.sleep(wait)
                    wait = min(wait * 2, 60)
                    continue
                if resp.status_code == 200:
                    return resp
                logger.warning(f"HTTP {resp.status_code} on {url}")
                return None
            except Exception as e:
                logger.warning(f"Request error {url}: {e}, waiting {wait}s")
                time.sleep(wait)
                wait = min(wait * 2, 60)
        return None

    # ---- Social normalizer ----
    def _normalize_socials(self, raw):
        result = []
        if not raw:
            return result
        for x in raw:
            if isinstance(x, str) and x:
                result.append(x)
            elif isinstance(x, dict):
                url = x.get('url') or x.get('handle') or x.get('value')
                if url:
                    result.append(url)
        return result

    # ==================== NEW LAUNCH SCANNER ====================
    def get_new_pools(self, selected_chain):
        new_pools = []
        now = time.time()

        # 1. Pump.fun (SOL only)
        if selected_chain in ['SOL', 'ALL']:
            for pf_url in self.pump_fun_urls:
                resp = self._get_with_backoff(pf_url)
                if not resp or not resp.text.strip():
                    continue
                try:
                    data = resp.json()
                    with SOL_LOCK:
                        sol_usd = SOL_USD
                    for coin in data:
                        mint = coin.get('mint')
                        if not mint or mint in self.seen_mints_per_chain['SOL']:
                            continue
                        liq_raw = float(coin.get('liquidity', 0) or 0)
                        if liq_raw == 0:
                            continue
                        ts = coin.get('created_timestamp', now * 1000)
                        age_sec = max((now * 1000 - ts) / 1000, 60)
                        price_usd = float(coin.get('price', 0)) * sol_usd
                        liq_usd = liq_raw * sol_usd if liq_raw < 10000 else liq_raw
                        vol_5m = (float(coin.get('volume', 0)) * sol_usd / age_sec) * 300

                        pool = {
                            'mint': mint, 'symbol': coin.get('symbol', '???'), 'name': coin.get('name', '???'),
                            'price': price_usd, 'liquidity': liq_usd, 'volume_5m': vol_5m,
                            'created_at': datetime.datetime.fromtimestamp(ts / 1000),
                            'socials': self._normalize_socials([coin.get('twitter'), coin.get('telegram')]),
                            'chain': 'SOL', 'price_change_5m': 0, 'buys_5m': 0,
                            'source': 'pumpfun'
                        }
                        self.seen_mints_per_chain['SOL'].add(mint)
                        new_pools.append(pool)
                    logger.info(f"[NEW] pump.fun → {len(data)} fetched, {len(new_pools)} new")
                    break
                except Exception as e:
                    logger.error(f"Pump.fun error: {e}")

        # 2. Four.meme (BSC-focused launcher)
        if selected_chain in ['BSC', 'ALL'] and now - self.last_fourmeme_fetch > 30:
            self.last_fourmeme_fetch = now
            resp = self._get_with_backoff(self.fourmeme_url)
            if resp and resp.status_code == 200:
                try:
                    data = resp.json().get('data', []) if isinstance(resp.json(), dict) else []
                    for item in data:
                        mint = item.get('address') or item.get('mint')
                        if not mint or mint in self.seen_mints_per_chain['BSC']:
                            continue
                        pool = {
                            'mint': mint,
                            'symbol': item.get('symbol', '???'),
                            'name': item.get('name', '???'),
                            'price': float(item.get('priceUsd', 0) or 0),
                            'liquidity': float(item.get('liquidity', 0) or 0),
                            'volume_5m': float(item.get('volume5m', 0) or 0),
                            'price_change_5m': float(item.get('priceChange5m', 0) or 0),
                            'buys_5m': int(item.get('buys5m', 0) or 0),
                            'created_at': datetime.datetime.fromtimestamp(item.get('createTime', now) / 1000),
                            'socials': self._normalize_socials(item.get('socials', [])),
                            'chain': 'BSC',
                            'source': 'fourmeme'
                        }
                        self.seen_mints_per_chain['BSC'].add(mint)
                        new_pools.append(pool)
                except Exception as e:
                    logger.error(f"Four.meme error: {e}")

        # 3. Clanker (BASE) via DexScreener search
        if selected_chain in ['BASE', 'ALL'] and now - self.last_clanker_fetch > 60:
            self.last_clanker_fetch = now
            resp = self._get_with_backoff(self.clanker_search_url)
            if resp and resp.status_code == 200:
                for p in resp.json().get('pairs', []):
                    if p.get('chainId') != 'base':
                        continue
                    mint = p.get('baseToken', {}).get('address')
                    if not mint or mint in self.seen_mints_per_chain['BASE']:
                        continue
                    pool = {
                        'mint': mint,
                        'symbol': p.get('baseToken', {}).get('symbol', '???'),
                        'name': p.get('baseToken', {}).get('name', '???'),
                        'price': float(p.get('priceUsd', 0)),
                        'liquidity': float(p.get('liquidity', {}).get('usd', 0)),
                        'volume_5m': float(p.get('volume', {}).get('m5', 0)),
                        'price_change_5m': float(p.get('priceChange', {}).get('m5', 0)),
                        'buys_5m': p.get('txns', {}).get('m5', {}).get('buys', 0),
                        'created_at': datetime.datetime.fromtimestamp(p.get('pairCreatedAt', now*1000) / 1000),
                        'socials': self._normalize_socials(p.get('info', {}).get('websites', [])),
                        'chain': 'BASE',
                        'source': 'clanker'
                    }
                    self.seen_mints_per_chain['BASE'].add(mint)
                    new_pools.append(pool)

        # 4. DexScreener token profiles (every 60s)
        if now - self.last_profile_fetch > 60:
            self.last_profile_fetch = now
            ds_new = 0
            try:
                resp = self._get_with_backoff(self.token_profile_url)
                if resp and resp.status_code == 200:
                    data = resp.json()
                    chain_label_map = {'solana': 'SOL', 'bsc': 'BSC', 'base': 'BASE'}
                    for item in data:
                        mint = item.get('tokenAddress')
                        chain_id = item.get('chainId', '').lower()
                        if not mint or mint in self.seen_mints_per_chain[chain_label_map.get(chain_id, 'SOL')]:
                            continue
                        if selected_chain != 'ALL':
                            wanted = {'SOL': 'solana', 'BSC': 'bsc', 'BASE': 'base'}.get(selected_chain)
                            if chain_id != wanted:
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
                                'price_change_5m': float(pair_data.get('priceChange', {}).get('m5', 0)),
                                'buys_5m': pair_data.get('txns', {}).get('m5', {}).get('buys', 0),
                                'created_at': datetime.datetime.fromtimestamp(pair_data.get('pairCreatedAt', now*1000) / 1000),
                                'socials': self._normalize_socials(pair_data.get('info', {}).get('websites', [])),
                                'chain': chain_label_map.get(chain_id, 'SOL'),
                                'source': 'dexscreener_profiles'
                            }
                            self.seen_mints_per_chain[pool['chain']].add(mint)
                            new_pools.append(pool)
                            ds_new += 1
            except Exception as e:
                logger.error(f"DexScreener profiles error: {e}")
            logger.info(f"[NEW] dexscreener profiles: new={ds_new}")

        # 5. Chain-specific DexScreener search (per chain)
        chains_to_search = (['SOL', 'BSC', 'BASE'] if selected_chain == 'ALL' else [selected_chain])
        chain_label_map = {'solana': 'SOL', 'bsc': 'BSC', 'base': 'BASE'}

        for sch in chains_to_search:
            if now - self.last_search_fetch.get(sch, 0) < 60:
                continue
            self.last_search_fetch[sch] = now
            url = self.search_urls.get(sch)
            if not url:
                continue
            try:
                resp = self._get_with_backoff(url)
                if not resp or resp.status_code != 200:
                    continue
                for p in resp.json().get('pairs', []):
                    chain_id = p.get('chainId', '').lower()
                    chain_matches = (
                        (sch == 'SOL' and chain_id == 'solana') or
                        (sch == 'BSC' and chain_id == 'bsc') or
                        (sch == 'BASE' and chain_id == 'base')
                    )
                    if not chain_matches:
                        continue
                    mint = p.get('baseToken', {}).get('address')
                    if not mint or mint in self.seen_mints_per_chain[sch]:
                        continue
                    pool = {
                        'mint': mint,
                        'symbol': p.get('baseToken', {}).get('symbol', '???'),
                        'name': p.get('baseToken', {}).get('name', '???'),
                        'price': float(p.get('priceUsd', 0)),
                        'liquidity': float(p.get('liquidity', {}).get('usd', 0)),
                        'volume_5m': float(p.get('volume', {}).get('m5', 0)),
                        'price_change_5m': float(p.get('priceChange', {}).get('m5', 0)),
                        'buys_5m': p.get('txns', {}).get('m5', {}).get('buys', 0),
                        'created_at': datetime.datetime.fromtimestamp(p.get('pairCreatedAt', now*1000) / 1000),
                        'socials': self._normalize_socials(p.get('info', {}).get('websites', [])),
                        'chain': sch,
                        'source': f'dexscreener_search_{sch.lower()}'
                    }
                    self.seen_mints_per_chain[sch].add(mint)
                    new_pools.append(pool)
            except Exception as e:
                logger.error(f"DexScreener search {sch} error: {e}")

        # Cleanup: maintain global seen set for backward compatibility
        for chain in self.seen_mints_per_chain:
            self.seen_mints.update(self.seen_mints_per_chain[chain])
        if len(self.seen_mints) > self.max_mints:
            self.seen_mints = set(list(self.seen_mints)[-self.max_mints:])
        return new_pools

    # ==================== MOMENTUM SCANNER ====================
    def get_momentum_pools(self, selected_chain, config: BotConfig):
        momentum_pools = []
        now = time.time()

        # 1. Birdeye trending (SOL only - uses free tier API key)
        if config.birdeye_api_key and selected_chain in ['SOL', 'ALL']:
            try:
                url = "https://public-api.birdeye.so/defi/token_trending?sort_by=rank&sort_type=desc&offset=0&limit=50"
                extra_headers = {"X-API-KEY": config.birdeye_api_key, "x-chain": "solana"}
                resp = self._get_with_backoff(url, extra_headers=extra_headers)
                if resp and resp.status_code == 200:
                    data = resp.json().get('data', {}).get('items', [])
                    logger.info(f"[BIRDEYE] Retrieved {len(data)} trending tokens")
                    for item in data:
                        mint = item.get('address')
                        if not mint or mint in self.seen_mints_per_chain['SOL']:
                            continue
                        age_sec = now - (item.get('pairCreatedAt', now * 1000) / 1000)
                        if not (config.age_momentum_min <= age_sec <= config.age_momentum_max):
                            continue
                        vol_5m = float(item.get('volume', {}).get('m5', 0) or 0)
                        liq = float(item.get('liquidity', 0) or 0)
                        if vol_5m < config.volume_5m * config.volume_spike_mult or liq < config.liq_min:
                            continue
                        buys = int(item.get('txns', {}).get('m5', {}).get('buys', 0) or 0)
                        sells = int(item.get('txns', {}).get('m5', {}).get('sells', 0) or 0)
                        buy_ratio = buys / (buys + sells + 1) if (buys + sells) > 0 else 0
                        if buy_ratio < config.min_buy_ratio:
                            continue
                        pool = {
                            'mint': mint,
                            'symbol': item.get('symbol', '???'),
                            'name': item.get('name', '???'),
                            'price': float(item.get('price', 0)),
                            'liquidity': liq,
                            'volume_5m': vol_5m,
                            'price_change_5m': float(item.get('priceChange', {}).get('m5', 0) or 0),
                            'buys_5m': buys,
                            'created_at': datetime.datetime.fromtimestamp(item.get('pairCreatedAt', now*1000) / 1000),
                            'socials': self._normalize_socials(item.get('info', {}).get('websites', [])),
                            'chain': 'SOL',
                            'source': 'birdeye_momentum',
                            'buy_ratio': buy_ratio
                        }
                        self.seen_mints_per_chain['SOL'].add(mint)
                        momentum_pools.append(pool)
                elif resp and resp.status_code == 401:
                    logger.error("Invalid Birdeye API key. Check BIRDEYE_API_KEY environment variable.")
                elif resp and resp.status_code == 429:
                    logger.warning("Birdeye rate limit reached, waiting 60s")
                    time.sleep(60)
            except Exception as e:
                logger.warning(f"Birdeye momentum error: {e}")

        # 2. DexScreener fallback (per‑chain)
        try:
            chains_to_scan = ['SOL'] if selected_chain == 'SOL' else \
                             ['BSC'] if selected_chain == 'BSC' else \
                             ['BASE'] if selected_chain == 'BASE' else ['SOL', 'BSC', 'BASE']
            for sch in chains_to_scan:
                url = self.search_urls.get(sch)
                if not url:
                    continue
                resp = self._get_with_backoff(url)
                if not resp or resp.status_code != 200:
                    continue
                for p in resp.json().get('pairs', []):
                    chain_id = p.get('chainId', '').lower()
                    if selected_chain != 'ALL' and chain_id != {'SOL':'solana','BSC':'bsc','BASE':'base'}.get(sch):
                        continue
                    mint = p.get('baseToken', {}).get('address')
                    if not mint or mint in self.seen_mints_per_chain[sch]:
                        continue
                    age_sec = now - (p.get('pairCreatedAt', now * 1000) / 1000)
                    if not (config.age_momentum_min <= age_sec <= config.age_momentum_max):
                        continue
                    vol_5m = float(p.get('volume', {}).get('m5', 0))
                    liq = float(p.get('liquidity', {}).get('usd', 0))
                    buys = p.get('txns', {}).get('m5', {}).get('buys', 0)
                    sells = p.get('txns', {}).get('m5', {}).get('sells', 0)
                    buy_ratio = buys / (buys + sells + 1) if (buys + sells) > 0 else 0
                    if vol_5m < config.volume_5m * config.volume_spike_mult or buy_ratio < config.min_buy_ratio or liq < config.liq_min:
                        continue
                    chain_label_map = {'solana': 'SOL', 'bsc': 'BSC', 'base': 'BASE'}
                    pool = {
                        'mint': mint,
                        'symbol': p.get('baseToken', {}).get('symbol', '???'),
                        'name': p.get('baseToken', {}).get('name', '???'),
                        'price': float(p.get('priceUsd', 0)),
                        'liquidity': liq,
                        'volume_5m': vol_5m,
                        'price_change_5m': float(p.get('priceChange', {}).get('m5', 0) or 0),
                        'buys_5m': buys,
                        'created_at': datetime.datetime.fromtimestamp(p.get('pairCreatedAt', now*1000) / 1000),
                        'socials': self._normalize_socials(p.get('info', {}).get('websites', [])),
                        'chain': chain_label_map.get(chain_id, sch),
                        'source': 'dexscreener_momentum',
                        'buy_ratio': buy_ratio
                    }
                    self.seen_mints_per_chain[sch].add(mint)
                    momentum_pools.append(pool)
        except Exception as e:
            logger.error(f"DexScreener momentum error: {e}")

        logger.info(f"[MOMENTUM] Found {len(momentum_pools)} momentum candidates")
        return momentum_pools

    # ---- Helper methods ----
    def _fetch_pair_info(self, mint, chain_id):
        chain_path = {'solana': 'solana', 'bsc': 'bsc', 'base': 'base'}.get(chain_id, 'solana')
        url = f"https://api.dexscreener.com/token-pairs/v1/{chain_path}/{mint}"
        resp = self._get_with_backoff(url, max_retries=3)
        if not resp or not resp.text.strip():
            return None
        data = resp.json()
        return data[0] if isinstance(data, list) and data else None

    def fetch_prices_batch(self, mints, chain):
        if not mints:
            return {}
        chain_path = {'SOL': 'solana', 'BSC': 'bsc', 'BASE': 'base'}.get(chain, 'solana')
        all_prices = {}
        for i in range(0, len(mints), 30):
            chunk = mints[i:i+30]
            try:
                url = f"https://api.dexscreener.com/tokens/v1/{chain_path}/{','.join(chunk)}"
                resp = self._get_with_backoff(url)
                if not resp or resp.status_code == 429:
                    time.sleep(60)
                    continue
                data = resp.json()
                for pair in (data if isinstance(data, list) else []):
                    mint = pair.get('baseToken', {}).get('address')
                    if mint:
                        all_prices[mint] = {'price': float(pair.get('priceUsd', 0))}
            except Exception as e:
                logger.error(f"Batch price error: {e}")
        return all_prices

# ============================================================================
# 4. Filter Engine (New Launch + Momentum)
# ============================================================================
class FilterEngine:
    def __init__(self, config: BotConfig):
        self.config = config

    def filter_and_score(self, pool):
        # Age filter – skip for real-time sources (pumpfun/fourmeme)
        source = pool.get('source', '')
        if source not in ('pumpfun', 'fourmeme'):
            try:
                age = (datetime.datetime.now() - pool['created_at']).total_seconds()
                if age < self.config.age_min or age > self.config.age_max:
                    return None, f"age={age:.0f}s out of [{self.config.age_min},{self.config.age_max}]"
            except Exception as e:
                return None, f"age_error={e}"

        if pool['liquidity'] < self.config.liq_min or pool['liquidity'] > self.config.liq_max:
            return None, f"liq=${pool['liquidity']:.0f} out of [{self.config.liq_min},{self.config.liq_max}]"

        if pool.get('volume_5m', 0) < self.config.volume_5m:
            return None, f"vol5m=${pool.get('volume_5m',0):.1f} < {self.config.volume_5m}"

        if self.config.require_social and len(pool['socials']) == 0:
            return None, "no_socials"

        # Min buys_5m filter
        if self.config.min_buys_5m > 0 and pool.get('buys_5m', 0) < self.config.min_buys_5m:
            return None, f"buys_5m={pool.get('buys_5m',0)} < {self.config.min_buys_5m}"

        # Dynamic rug filter: allow higher vol/liq for very new coins
        try:
            age_sec = (datetime.datetime.now() - pool['created_at']).total_seconds()
        except:
            age_sec = 300
        max_vol_liq = 80 if age_sec < 300 else 50
        vol_liq = (pool.get('volume_5m', 0) * 288) / max(pool['liquidity'], 1)
        if vol_liq > max_vol_liq:
            return None, f"rug_vol_liq={vol_liq:.1f}"

        # Score
        liq_score = min(100, pool['liquidity'] / 5000 * 100) * 0.3
        vol_score = min(100, pool.get('volume_5m', 0) / 500 * 100) * 0.4
        soc_score = min(30, len(pool['socials']) * 15)
        mom = pool.get('price_change_5m', 0)
        mom_score = 20 if mom > 10 else (10 if mom > 5 else (-15 if mom < -5 else 0))
        tx_score = min(20, pool.get('buys_5m', 0) / 10)
        score = liq_score + vol_score + soc_score + mom_score + tx_score
        pool['score'] = score
        return pool, "pass"

    def filter_and_score_momentum(self, pool):
        filtered, reason = self.filter_and_score(pool)
        if not filtered:
            return None, reason

        # 2026 momentum bonus
        bonus = 0
        if pool.get('volume_5m', 0) >= 1000: bonus += 20
        if pool.get('price_change_5m', 0) > 8: bonus += 15
        if pool.get('buy_ratio', 0) >= 0.75: bonus += 25
        filtered['score'] = filtered.get('score', 0) + bonus

        if filtered['score'] < self.config.momentum_score_threshold:
            return None, f"momentum_score={filtered['score']:.0f}"
        return filtered, "momentum_pass"

# ============================================================================
# 5. Trade Simulator
# ============================================================================
class TradeSimulator:
    def __init__(self, notifier, config: BotConfig):
        self.notifier = notifier
        self.balance = 100.0
        self.initial_balance = 100.0
        self.positions = {}
        self.trades = []
        self.config = config

    def _get_targets(self, score, is_momentum=False):
        if is_momentum:
            return {'profit_pct': 35.0, 'profit_abs': 1.5, 'stop_pct': 12.0, 'timeout': 25}
        if score >= 80:
            return {'profit_pct': 60.0, 'profit_abs': 2.0, 'stop_pct': 15.0, 'timeout': 20}
        elif score >= 65:
            return {'profit_pct': 30.0, 'profit_abs': 1.0, 'stop_pct': 12.0, 'timeout': 15}
        else:
            return {'profit_pct': self.config.profit_target_pct,
                    'profit_abs': self.config.profit_target_abs,
                    'stop_pct': self.config.stop_loss_pct,
                    'timeout': self.config.timeout_min}

    def get_buy_amount(self, score):
        if score >= 80:
            return self.config.buy_amount * 2.0
        elif score >= 65:
            return self.config.buy_amount * 1.5
        else:
            return self.config.buy_amount

    def can_buy(self, max_positions=10, score=0):
        amount = min(self.get_buy_amount(score), self.balance)
        return amount >= self.config.min_buy_usd and len(self.positions) < max_positions

    def simulate_buy(self, pool, max_pos=10):
        score = pool.get('score', 50)
        if not self.can_buy(max_pos, score):
            return None
        amount = min(self.get_buy_amount(score), self.balance)
        if amount < 0.5 or pool['price'] <= 0:
            return None
        ep = pool['price'] * (1 + random.uniform(0.5, 1.0) / 100)
        fee = amount * 0.005
        qty = (amount - fee) / ep
        self.balance -= amount

        is_momentum = pool.get('source', '').endswith('_momentum')
        t = self._get_targets(score, is_momentum)

        self.positions[pool['mint']] = {
            'symbol': pool['symbol'],
            'buy_price': ep,
            'buy_time': datetime.datetime.now(),
            'amount_usd': amount,
            'quantity': qty,
            'target_profit_usd': t['profit_abs'],
            'stop_price': ep * (1 - t['stop_pct'] / 100),
            'timeout_at': datetime.datetime.now() + datetime.timedelta(minutes=t['timeout']),
            'last_price_update': datetime.datetime.now(),
            'chain': pool.get('chain', 'SOL'),
            'entry_score': score,
            'partial_sold': False,
            'targets': t,
            'is_momentum': is_momentum
        }
        return {
            'symbol': pool['symbol'],
            'price': ep,
            'amount': amount,
            'fee': fee,
            'quantity': qty,
            'balance_after': self.balance
        }

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
        esp = price * (1 - random.uniform(0.5, 1.0) / 100)
        fee = esp * pos['quantity'] * 0.005
        proceeds = esp * pos['quantity'] - fee
        self.balance += proceeds
        profit = proceeds - pos['amount_usd']
        pct = (proceeds / pos['amount_usd'] - 1) * 100
        self.trades.append({
            'timestamp': datetime.datetime.now(),
            'symbol': pos['symbol'],
            'mint': mint,
            'buy_price': pos['buy_price'],
            'sell_price': esp,
            'buy_amount_usd': pos['amount_usd'],
            'sell_amount_usd': proceeds,
            'profit_usd': profit,
            'reason': reason,
            'entry_score': pos.get('entry_score', 0),
            'chain': pos.get('chain', 'SOL'),
            'partial': partial,
            'is_momentum': pos.get('is_momentum', False)
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
        now = datetime.datetime.now()
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
            pv = price * pos['quantity']
            pu = pv - pos['amount_usd']
            ppct = (pv / pos['amount_usd'] - 1) * 100
            # Partial sell for high-score coins
            if pos.get('entry_score', 0) >= 80 and not pos.get('partial_sold') and ppct >= 30:
                hq, hc = pos['quantity']/2, pos['amount_usd']/2
                hp = {**pos, 'quantity': hq, 'amount_usd': hc}
                self.positions[mint].update({'quantity': hq, 'amount_usd': hc,
                    'partial_sold': True, 'stop_price': pos['buy_price']})
                sells.append(self._execute_sell(mint, hp, price, "partial sell at +30%", partial=True))
                continue
            reason = None
            if pu >= pos['target_profit_usd']:
                reason = f"abs profit ${pu:.2f}"
            elif ppct >= pos['targets']['profit_pct']:
                reason = f"profit {ppct:.1f}%"
            elif price <= pos['stop_price']:
                reason = f"stop loss {pos['targets']['stop_pct']}%"
            if reason:
                sells.append(self._execute_sell(mint, pos, price, reason))
        return sells

# ============================================================================
# 6. Reporter
# ============================================================================
class Reporter:
    def __init__(self, notifier):
        self.telegram = notifier
        self.daily_summary = {'date': None, 'trades': 0, 'wins': 0, 'net_profit': 0}
        self.last_save = datetime.datetime.now()
        self.initial_balance = 100.0
        self.start_time = datetime.datetime.now()
        self.consecutive_losses = 0

    def log_detection(self, pool):
        logger.info(f"🔍 [{pool['chain']}] {pool['symbol']} score={pool.get('score',0):.1f} liq=${pool['liquidity']:,.0f} src={pool.get('source','?')}")

    def log_buy(self, pool, r):
        msg = (f"✅ BUY [{pool['chain']}] {pool['symbol']}\n"
               f"📋 CA: <code>{pool['mint']}</code>\n"
               f"Price: ${r['price']:.8f} | Amount: ${r['amount']:.2f} | Balance: ${r['balance_after']:.2f}")
        logger.info(msg)
        self.telegram.send(msg)

    def log_portfolio(self, balance, positions):
        msg = f"📊 Balance: ${balance:.2f} | Open: {len(positions)}"
        logger.info(msg)
        self.telegram.send(msg)

    def log_error(self, msg):
        logger.error(msg)
        self.telegram.send(f"⚠️ {msg}")

    def save_state(self, sim, det):
        pickle.dump({
            'simulator': {'balance': sim.balance, 'positions': sim.positions, 'trades': sim.trades},
            'detector': {'seen_mints': list(det.seen_mints), 'seen_mints_per_chain': {k: list(v) for k, v in det.seen_mints_per_chain.items()}},
            'daily_summary': self.daily_summary,
            'start_time': self.start_time.isoformat(),
            'consecutive_losses': self.consecutive_losses
        }, open(DATA_PATH / "state.pkl", 'wb'))

    def load_state(self, sim, det):
        try:
            s = pickle.load(open(DATA_PATH / "state.pkl", 'rb'))
            sim.balance, sim.positions, sim.trades = s['simulator']['balance'], s['simulator']['positions'], s['simulator']['trades']
            det.seen_mints = set(s['detector']['seen_mints'])
            if 'seen_mints_per_chain' in s['detector']:
                det.seen_mints_per_chain = {k: set(v) for k, v in s['detector']['seen_mints_per_chain'].items()}
            self.daily_summary = s['daily_summary']
            self.start_time = datetime.datetime.fromisoformat(s.get('start_time', self.start_time.isoformat()))
            self.consecutive_losses = s.get('consecutive_losses', 0)
            logger.info("Loaded previous state.")
        except FileNotFoundError:
            logger.info("Starting fresh.")

    def auto_save(self, sim, det):
        if (datetime.datetime.now() - self.last_save).total_seconds() > 300:
            self.save_state(sim, det)
            self.last_save = datetime.datetime.now()

    def update_daily_summary(self, profit):
        today = datetime.datetime.now().date()
        if self.daily_summary['date'] != today:
            if self.daily_summary['date']:
                msg = f"📅 {self.daily_summary['date']}: {self.daily_summary['trades']} trades, {self.daily_summary['wins']} wins, ${self.daily_summary['net_profit']:.2f}"
                logger.info(msg)
                self.telegram.send(msg)
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
        if not full:
            return
        df = pd.DataFrame(full)
        df['cumulative'] = df['profit_usd'].cumsum() + self.initial_balance
        plt.figure(figsize=(12,4))
        plt.plot(df['timestamp'], df['cumulative'], label='Equity')
        plt.axhline(y=self.initial_balance, color='r', linestyle='--', label='Initial')
        plt.title('Equity Curve'); plt.xlabel('Time'); plt.ylabel('Balance ($)')
        plt.legend(); plt.grid(True)
        plt.savefig(DATA_PATH / "equity.png"); plt.close()
        logger.info("Equity curve saved.")

# ============================================================================
# 7. Keep-alive + health ping (optional)
# ============================================================================
def keep_alive():
    while True:
        time.sleep(300)  # every 5 min
        logger.info("[HEALTH] Bot is alive")
        # Uncomment to send Telegram health ping (requires notifier reference)
        # if notifier:
        #     notifier.send("✅ Bot is alive")
threading.Thread(target=keep_alive, daemon=True).start()

# ============================================================================
# 8. Main Bot Loop
# ============================================================================
def run_bot():
    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_IDS = [c.strip() for c in os.getenv("TELEGRAM_CHAT_IDS", "").split(",") if c.strip()]
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_IDS:
        logger.error("TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_IDS must be set.")
        sys.exit(1)

    notifier = TelegramNotifier(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_IDS)
    reporter = Reporter(notifier)
    detector = Detector()
    simulator = TradeSimulator(notifier, config)
    reporter.initial_balance = simulator.initial_balance
    reporter.load_state(simulator, detector)

    start_time = reporter.start_time
    last_portfolio_upd = datetime.datetime.now()
    last_price_upd = datetime.datetime.now()
    buy_pause_until = None
    get_sol_usd()
    get_bnb_usd()

    logger.info(f"🚀 Bot started | Notifying {len(TELEGRAM_CHAT_IDS)} chat(s)")
    notifier.send(f"🚀 Bot started. Dry-run $100. Notifying {len(TELEGRAM_CHAT_IDS)} recipient(s).")

    filter_engine = FilterEngine(config)

    while True:
        if config.max_run_hours > 0:
            elapsed = (datetime.datetime.now() - start_time).total_seconds()
            if elapsed > config.max_run_hours * 3600:
                msg = f"✅ {config.max_run_hours}h complete! P&L: ${simulator.balance - simulator.initial_balance:.2f}"
                logger.info(msg); notifier.send(msg); break

        # Refresh SOL and BNB prices every 30 min
        if (datetime.datetime.now() - last_price_upd).total_seconds() > 1800:
            get_sol_usd()
            get_bnb_usd()
            last_price_upd = datetime.datetime.now()

        # Detection
        try:
            new_pools = detector.get_new_pools(config.chain_selector)
            momentum_pools = detector.get_momentum_pools(config.chain_selector, config)
            all_candidates = new_pools + momentum_pools
        except Exception as e:
            reporter.log_error(f"Detection error: {e}")
            time.sleep(30)
            continue

        # Filter & buy logic
        passed = rejected = 0
        reject_reasons = {}

        daily_loss = reporter.daily_summary.get('net_profit', 0)
        can_buy_now = (
            daily_loss >= config.daily_loss_limit and
            (buy_pause_until is None or datetime.datetime.now() >= buy_pause_until)
        )

        if buy_pause_until and datetime.datetime.now() >= buy_pause_until:
            buy_pause_until = None
            can_buy_now = True
            logger.info("▶️ Buy pause lifted")

        if reporter.consecutive_losses >= config.consecutive_loss_limit and buy_pause_until is None:
            logger.info(f"⛔ {reporter.consecutive_losses} losses — pausing 30 min")
            buy_pause_until = datetime.datetime.now() + datetime.timedelta(minutes=30)
            reporter.consecutive_losses = 0
            reporter.save_state(simulator, detector)
            can_buy_now = False

        for pool in all_candidates:
            if pool.get('source', '').endswith('_momentum'):
                filtered, reason = filter_engine.filter_and_score_momentum(pool)
            else:
                filtered, reason = filter_engine.filter_and_score(pool)

            if not filtered:
                rejected += 1
                reject_reasons[reason] = reject_reasons.get(reason, 0) + 1
                continue
            passed += 1

            # Per‑source threshold adjustment
            if filtered.get('source') == 'pumpfun':
                threshold = max(15, config.score_threshold - 20)
            else:
                threshold = config.score_threshold
            if filtered['score'] < threshold:
                reject_reasons[f"score={filtered['score']:.0f}<{threshold}"] = \
                    reject_reasons.get(f"score={filtered['score']:.0f}<{threshold}", 0) + 1
                continue

            reporter.log_detection(filtered)
            notifier.send(
                f"📈 BUY SIGNAL [{filtered['chain']}] {filtered['symbol']} ({filtered['name']})\n"
                f"📋 CA: <code>{filtered['mint']}</code>\n"
                f"Score: {filtered['score']:.1f} | Liq: ${filtered['liquidity']:,.0f} | Src: {filtered.get('source','?')}"
            )
            if can_buy_now and simulator.can_buy(config.max_positions, filtered.get('score', 0)):
                r = simulator.simulate_buy(filtered, config.max_positions)
                if r:
                    reporter.log_buy(filtered, r)
            elif not can_buy_now:
                logger.info("Skipping buy — paused/limit")
            else:
                logger.info(f"Skipping {filtered['symbol']} — max positions reached")

        if all_candidates:
            logger.info(f"[FILTER] {len(all_candidates)} candidates → {passed} passed filter → reject reasons: {reject_reasons}")
        else:
            logger.info("[FILTER] 0 new candidates this cycle")

        # Price updates
        if simulator.positions:
            chain_mints = defaultdict(list)
            for mint, pos in simulator.positions.items():
                chain_mints[pos.get('chain', 'SOL')].append(mint)
            for chain, mints in chain_mints.items():
                prices = detector.fetch_prices_batch(mints, chain)
                for mint, data in prices.items():
                    simulator.update_price(mint, data['price'])
                    simulator.update_trailing_stop(mint)

        # Sells
        for sell in simulator.check_positions():
            if not sell.get('partial'):
                reporter.update_daily_summary(sell['profit'])

        # Periodic portfolio update
        if (datetime.datetime.now() - last_portfolio_upd).total_seconds() > 300:
            reporter.log_portfolio(simulator.balance, simulator.positions)
            last_portfolio_upd = datetime.datetime.now()

        reporter.auto_save(simulator, detector)

        full = [t for t in simulator.trades if not t.get('partial')]
        wins = sum(1 for t in full if t['profit_usd'] > 0)
        wr = (wins / len(full) * 100) if full else 0
        logger.info(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] "
                    f"Balance: ${simulator.balance:.2f} | Trades: {len(full)} | WR: {wr:.0f}% | Open: {len(simulator.positions)}")
        time.sleep(15)

    reporter.plot_equity(simulator.trades)

# ============================================================================
# 9. Auto‑restart wrapper
# ============================================================================
if __name__ == "__main__":
    while True:
        try:
            run_bot()
        except Exception as e:
            logger.critical(f"Bot crashed: {e}", exc_info=True)
            time.sleep(10)
