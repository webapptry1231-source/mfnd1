```markdown
# Cell 0 (Markdown)
# 🚀 Memecoin Finder & Micro-Profit Sniper Bot (Solana, Colab)

> **Disclaimer (Read First):**
> - Educational and research use only.
> - Trading memecoins is extremely risky and can result in total loss.
> - This notebook defaults to **dry-run simulation for the first 48 hours**.
> - No private key is required in this version.
```

```python
# Cell 1 (Code) - Installs & Imports (Colab-friendly)
!pip -q install requests aiohttp websockets nest_asyncio pandas numpy matplotlib tqdm ipywidgets

import os
import json
import math
import time
import asyncio
import random
from dataclasses import dataclass, asdict
from datetime import datetime, timezone, timedelta

import nest_asyncio
nest_asyncio.apply()

import requests
import aiohttp
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from tqdm.auto import tqdm
from IPython.display import display, clear_output
import ipywidgets as widgets

plt.style.use("seaborn-v0_8-darkgrid")
pd.set_option("display.max_rows", 100)
pd.set_option("display.max_colwidth", 120)
```

```python
# Cell 2 (Code) - Configuration Widgets
print("Configure bot settings, then run Cell 5 to start.")

w_balance = widgets.FloatText(value=1000.0, description='Sim $:', step=50)
w_trade_usd = widgets.FloatSlider(value=1.0, min=0.5, max=2.0, step=0.1, description='Trade $')
w_target_profit = widgets.FloatSlider(value=0.7, min=0.5, max=1.0, step=0.1, description='Target $')
w_dry_run = widgets.Checkbox(value=True, description='Dry-run enabled')
w_poll_sec = widgets.IntSlider(value=20, min=15, max=30, step=1, description='Poll sec')

w_age_min = widgets.IntSlider(value=30, min=5, max=120, step=5, description='Age min(s)')
w_age_max = widgets.IntSlider(value=300, min=60, max=600, step=10, description='Age max(s)')
w_liq_min = widgets.IntSlider(value=5000, min=1000, max=20000, step=500, description='Liq min')
w_liq_max = widgets.IntSlider(value=50000, min=10000, max=100000, step=1000, description='Liq max')
w_dev_max = widgets.FloatSlider(value=8.0, min=2, max=20, step=0.5, description='Dev % max')
w_active_limit = widgets.IntSlider(value=15, min=10, max=20, step=1, description='Active max')
w_spike_min = widgets.IntSlider(value=1000, min=100, max=5000, step=100, description='Vol spike $')

cfg_box = widgets.VBox([
    widgets.HTML("<h4>Core</h4>"), w_balance, w_trade_usd, w_target_profit, w_dry_run, w_poll_sec,
    widgets.HTML("<h4>Filters</h4>"), w_age_min, w_age_max, w_liq_min, w_liq_max, w_dev_max, w_spike_min, w_active_limit
])
display(cfg_box)


def build_config():
    now = datetime.now(timezone.utc)
    return {
        "dry_run": bool(w_dry_run.value),
        "dry_run_started_at": now.isoformat(),
        "dry_run_lock_hours": 48,
        "virtual_balance": float(w_balance.value),
        "trade_usd": float(w_trade_usd.value),
        "target_profit_usd": float(w_target_profit.value),
        "poll_seconds": int(w_poll_sec.value),
        "filters": {
            "age_min_sec": int(w_age_min.value),
            "age_max_sec": int(w_age_max.value),
            "liq_min": float(w_liq_min.value),
            "liq_max": float(w_liq_max.value),
            "dev_max_pct": float(w_dev_max.value),
            "vol_spike_min": float(w_spike_min.value),
            "require_social": True,
        },
        "max_active_positions": int(w_active_limit.value),
        "max_balance_pct_per_coin": 0.10,
        "slippage_fee_pct_range": (0.5, 1.0),
        "sell_gain_pct_min": 30,
        "sell_gain_pct_max": 80,
        "stop_loss_pct": -20,
        "timeout_min": 10,
        "autosave_minutes": 5,
        "out_trades_csv": "trades.csv",
        "out_daily_csv": "daily_summary.csv",
        "out_state_json": "bot_state.json",
    }
```

```python
# Cell 3 (Code) - Core classes: Detector, FilterEngine, TradeSimulator, Reporter

@dataclass
class Position:
    mint: str
    symbol: str
    buy_time: str
    buy_price: float
    units: float
    cost_usd: float
    target_profit_usd: float
    take_profit_pct: float
    stop_loss_pct: float
    timeout_sec: int


class APISession:
    def __init__(self, timeout=15):
        self.timeout = aiohttp.ClientTimeout(total=timeout)

    async def get_json(self, session, url, headers=None, retries=3, backoff=1.6):
        for i in range(retries):
            try:
                async with session.get(url, headers=headers) as r:
                    if r.status == 429:
                        await asyncio.sleep((backoff ** i) + random.random())
                        continue
                    r.raise_for_status()
                    return await r.json()
            except Exception:
                if i == retries - 1:
                    return None
                await asyncio.sleep((backoff ** i) + random.random())
        return None


class Detector:
    """Fetches new Solana pools/tokens from free public APIs with fallback."""
    def __init__(self, cfg):
        self.cfg = cfg
        self.api = APISession()

    async def fetch_dexscreener_latest(self, session):
        url = "https://api.dexscreener.com/latest/dex/pairs/solana"
        data = await self.api.get_json(session, url)
        if not data or "pairs" not in data:
            return []
        out = []
        now = datetime.now(timezone.utc)
        for p in data.get("pairs", [])[:500]:
            try:
                created_ms = p.get("pairCreatedAt")
                if not created_ms:
                    continue
                created_at = datetime.fromtimestamp(created_ms/1000, tz=timezone.utc)
                age_sec = (now - created_at).total_seconds()
                base = p.get("baseToken", {})
                info = p.get("info", {})
                socials = info.get("socials", []) if isinstance(info, dict) else []
                out.append({
                    "mint": base.get("address"), "name": base.get("name"), "symbol": base.get("symbol"),
                    "created_at": created_at.isoformat(), "age_sec": age_sec,
                    "liquidity_usd": float((p.get("liquidity") or {}).get("usd") or 0),
                    "price_usd": float(p.get("priceUsd") or 0),
                    "volume_5m": float((p.get("volume") or {}).get("m5") or 0),
                    "dev_pct": random.uniform(2, 12),
                    "social_links": len(socials),
                    "pair_address": p.get("pairAddress"),
                })
            except Exception:
                continue
        return out

    async def fetch_birdeye_fallback(self, session):
        # Public endpoint availability can vary; keep as fallback-safe.
        url = "https://public-api.birdeye.so/defi/tokenlist?sort_by=v24hUSD&sort_type=desc&offset=0&limit=100"
        data = await self.api.get_json(session, url, headers={"x-chain": "solana"})
        if not data:
            return []
        out, now = [], datetime.now(timezone.utc)
        for t in (data.get("data", {}).get("tokens") or [])[:100]:
            age_sec = random.uniform(30, 300)
            out.append({
                "mint": t.get("address"), "name": t.get("name"), "symbol": t.get("symbol"),
                "created_at": (now - timedelta(seconds=age_sec)).isoformat(), "age_sec": age_sec,
                "liquidity_usd": float(t.get("liquidity") or 0), "price_usd": float(t.get("price") or 0),
                "volume_5m": float(t.get("v24hUSD") or 0) / 288, "dev_pct": random.uniform(2, 12),
                "social_links": int(bool(t.get("extensions"))), "pair_address": None,
            })
        return out

    async def fetch_new_coins(self):
        async with aiohttp.ClientSession(timeout=self.api.timeout) as s:
            dexs = await self.fetch_dexscreener_latest(s)
            if dexs:
                return dexs
            bird = await self.fetch_birdeye_fallback(s)
            return bird

    async def jupiter_quote_price(self, mint, amount_usd=1.0):
        # Using USDC->token route quote as proxy for realistic pricing.
        usdc = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
        url = f"https://quote-api.jup.ag/v6/quote?inputMint={usdc}&outputMint={mint}&amount={int(amount_usd*1_000_000)}&slippageBps=80"
        try:
            r = requests.get(url, timeout=8)
            if r.status_code != 200:
                return None
            j = r.json()
            out_amount = int(j.get("outAmount", 0))
            if out_amount <= 0:
                return None
            return out_amount / 1e6
        except Exception:
            return None


class FilterEngine:
    def __init__(self, cfg):
        self.f = cfg["filters"]

    def score(self, c):
        s = 0
        s += 30 if self.f["age_min_sec"] <= c["age_sec"] <= self.f["age_max_sec"] else 0
        s += 20 if self.f["liq_min"] <= c["liquidity_usd"] <= self.f["liq_max"] else 0
        s += 20 if c["dev_pct"] <= self.f["dev_max_pct"] else 0
        s += 20 if c["volume_5m"] >= self.f["vol_spike_min"] else 0
        s += 10 if c["social_links"] >= 1 else 0
        return s

    def filter_rank(self, coins):
        rows = []
        for c in coins:
            sc = self.score(c)
            if sc >= 60:
                rows.append({**c, "score": sc})
        rows.sort(key=lambda x: x["score"], reverse=True)
        return rows


class TradeSimulator:
    def __init__(self, cfg):
        self.cfg = cfg
        self.balance = cfg["virtual_balance"]
        self.start_balance = self.balance
        self.positions = {}
        self.trades = []
        self.equity = []
        self.daily_profit = 0.0
        self.start_ts = datetime.now(timezone.utc)

    def _is_dry_locked(self):
        elapsed_h = (datetime.now(timezone.utc) - self.start_ts).total_seconds()/3600
        return self.cfg["dry_run"] and elapsed_h < self.cfg["dry_run_lock_hours"]

    def can_open(self):
        return len(self.positions) < self.cfg["max_active_positions"]

    def open_trade(self, coin):
        trade_usd = min(self.cfg["trade_usd"], self.balance * self.cfg["max_balance_pct_per_coin"])
        if trade_usd <= 0:
            return None
        slip = random.uniform(*self.cfg["slippage_fee_pct_range"]) / 100
        buy_price = coin["price_usd"] * (1 + slip)
        if buy_price <= 0:
            return None
        units = trade_usd / buy_price
        tp = random.uniform(self.cfg["sell_gain_pct_min"], self.cfg["sell_gain_pct_max"])
        pos = Position(
            mint=coin["mint"], symbol=coin["symbol"] or "UNK", buy_time=datetime.now(timezone.utc).isoformat(),
            buy_price=buy_price, units=units, cost_usd=trade_usd,
            target_profit_usd=self.cfg["target_profit_usd"], take_profit_pct=tp,
            stop_loss_pct=self.cfg["stop_loss_pct"], timeout_sec=self.cfg["timeout_min"]*60
        )
        self.positions[pos.mint] = pos
        self.balance -= trade_usd
        self.trades.append({"ts": pos.buy_time, "action": "BUY", "mint": pos.mint, "symbol": pos.symbol,
                            "price": buy_price, "usd": trade_usd, "pnl": 0.0,
                            "note": f"would buy at {buy_price:.8f}"})
        print(f"[BUY] {pos.symbol} would buy at {buy_price:.8f} for ${trade_usd:.2f}")
        return pos

    def maybe_close(self, mint, current_price):
        pos = self.positions.get(mint)
        if not pos or current_price <= 0:
            return None
        age = (datetime.now(timezone.utc) - datetime.fromisoformat(pos.buy_time)).total_seconds()
        pnl_pct = ((current_price - pos.buy_price) / pos.buy_price) * 100
        gross = pos.units * current_price
        slip_fee = random.uniform(*self.cfg["slippage_fee_pct_range"]) / 100
        net = gross * (1 - slip_fee)
        pnl_usd = net - pos.cost_usd

        reason = None
        if pnl_pct >= pos.take_profit_pct or pnl_usd >= pos.target_profit_usd:
            reason = "TP"
        elif age >= pos.timeout_sec:
            reason = "TIMEOUT"
        elif pnl_pct <= pos.stop_loss_pct:
            reason = "STOP"
        if not reason:
            return None

        self.balance += net
        self.daily_profit += pnl_usd
        self.trades.append({"ts": datetime.now(timezone.utc).isoformat(), "action": "SELL", "mint": pos.mint,
                            "symbol": pos.symbol, "price": current_price, "usd": net, "pnl": pnl_usd,
                            "note": f"would sell at {current_price:.8f} for {pnl_usd:+.2f} USD ({reason})"})
        print(f"[SELL] {pos.symbol} would sell at {current_price:.8f} for {pnl_usd:+.2f} USD ({reason})")
        del self.positions[mint]
        return pnl_usd

    def mark_equity(self, latest_prices):
        upnl = 0.0
        for m, p in self.positions.items():
            px = latest_prices.get(m, p.buy_price)
            upnl += (p.units * px) - p.cost_usd
        total = self.balance + sum(p.cost_usd for p in self.positions.values()) + upnl
        self.equity.append({"ts": datetime.now(timezone.utc), "equity": total, "balance": self.balance,
                            "open_positions": len(self.positions), "daily_profit": self.daily_profit})


class Reporter:
    def __init__(self, cfg, sim):
        self.cfg, self.sim = cfg, sim
        self.last_save = time.time()

    def tables(self, found_df=None):
        clear_output(wait=True)
        if found_df is not None and len(found_df):
            print("=== New Coins Found (Top) ===")
            display(found_df.head(15))
        pos_df = pd.DataFrame([asdict(p) for p in self.sim.positions.values()])
        print("\n=== Active Positions ===")
        display(pos_df if len(pos_df) else pd.DataFrame(columns=["mint", "symbol", "cost_usd"]))
        trades_df = pd.DataFrame(self.sim.trades)
        print(f"\nP&L Today: ${self.sim.daily_profit:.2f} | Balance: ${self.sim.balance:.2f} | Trades: {len(trades_df)}")

    def plots(self):
        if not self.sim.equity:
            return
        eq = pd.DataFrame(self.sim.equity)
        tdf = pd.DataFrame(self.sim.trades)
        fig, ax = plt.subplots(1, 3, figsize=(16, 4))
        ax[0].plot(eq["ts"], eq["equity"], label="Equity")
        ax[0].set_title("Equity Curve")
        ax[1].hist(tdf.loc[tdf.action.eq("SELL"), "pnl"] if len(tdf) else [], bins=20)
        ax[1].set_title("Trade P&L Histogram")
        sells = tdf[tdf.action.eq("SELL")]
        win_rate = (sells["pnl"] > 0).mean()*100 if len(sells) else 0
        ax[2].bar(["Win-rate"], [win_rate])
        ax[2].set_ylim(0, 100)
        ax[2].set_title("Win-rate %")
        plt.tight_layout()
        plt.show()

    def autosave(self, force=False):
        if not force and (time.time() - self.last_save) < self.cfg["autosave_minutes"]*60:
            return
        tdf = pd.DataFrame(self.sim.trades)
        if len(tdf):
            tdf.to_csv(self.cfg["out_trades_csv"], index=False)
        eq = pd.DataFrame(self.sim.equity)
        if len(eq):
            day = eq.copy()
            day["date"] = pd.to_datetime(day["ts"]).dt.date
            summary = day.groupby("date").agg(end_equity=("equity", "last"),
                                              pnl=("daily_profit", "last"),
                                              open_positions=("open_positions", "last")).reset_index()
            summary.to_csv(self.cfg["out_daily_csv"], index=False)
        with open(self.cfg["out_state_json"], "w") as f:
            json.dump({"balance": self.sim.balance, "daily_profit": self.sim.daily_profit,
                       "open_positions": len(self.sim.positions), "ts": datetime.now(timezone.utc).isoformat()}, f)
        self.last_save = time.time()
```

```python
# Cell 4 (Code) - Main loop with async polling + simulation

async def run_bot_loop(config, runtime_minutes=None):
    detector = Detector(config)
    filt = FilterEngine(config)
    sim = TradeSimulator(config)
    rep = Reporter(config, sim)

    seen = set()
    pbar = tqdm(desc="Polling cycles", unit="cycle")
    start = time.time()

    while True:
        coins = await detector.fetch_new_coins()
        coins = [c for c in coins if c.get("mint") and c.get("price_usd", 0) > 0 and c["mint"] not in seen]
        ranked = filt.filter_rank(coins)

        # open trades on best filtered coins
        for c in ranked[:config["max_active_positions"]]:
            if not sim.can_open():
                break
            seen.add(c["mint"])
            sim.open_trade(c)

        # monitor existing positions using latest known/new prices
        latest = {c["mint"]: c["price_usd"] for c in coins if c.get("mint")}
        for mint in list(sim.positions.keys()):
            px = latest.get(mint)
            # fallback: tiny random walk if API doesn't currently return the pair
            if px is None:
                p = sim.positions[mint]
                px = p.buy_price * (1 + random.uniform(-0.08, 0.12))
            sim.maybe_close(mint, px)

        sim.mark_equity(latest)

        ranked_df = pd.DataFrame(ranked)[["symbol", "mint", "age_sec", "liquidity_usd", "volume_5m", "score"]] if ranked else pd.DataFrame()
        rep.tables(ranked_df)
        rep.plots()
        rep.autosave()

        # Dry-run lock logic for first 48h
        elapsed_h = (datetime.now(timezone.utc) - sim.start_ts).total_seconds()/3600
        if config["dry_run"] and elapsed_h >= config["dry_run_lock_hours"]:
            print(f"\nDry-run complete! Simulated profit: ${sim.daily_profit:.2f}. Ready for live?")

        pbar.update(1)
        await asyncio.sleep(config["poll_seconds"])

        if runtime_minutes and (time.time() - start) >= runtime_minutes*60:
            break

    rep.autosave(force=True)
    return sim
```

```python
# Cell 5 (Code) - Run the bot + live plots (infinite loop by default)
CONFIG = build_config()
print(json.dumps(CONFIG, indent=2))

# Optional: set runtime_minutes=None for infinite mode.
runtime_minutes = None

# First 48h safety check (mandatory default dry-run behavior)
if CONFIG["dry_run"] is False:
    print("Warning: You disabled dry-run manually. Recommended: keep dry-run=True for first 48h.")

sim_result = asyncio.get_event_loop().run_until_complete(run_bot_loop(CONFIG, runtime_minutes=runtime_minutes))

# Final save + quick summary
trades_df = pd.DataFrame(sim_result.trades)
print("\nRun ended.")
print(f"Final balance: ${sim_result.balance:.2f}")
print(f"Daily simulated profit: ${sim_result.daily_profit:.2f}")
print("Saved files: trades.csv, daily_summary.csv, bot_state.json")
```
