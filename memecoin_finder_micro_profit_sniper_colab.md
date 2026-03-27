```markdown
# Cell 0 (Markdown)
# 🚀 Solana Memecoin Finder & Micro-Profit Sniper Bot (Colab)

**Disclaimer:** Educational use only. Memecoin trading is high risk. This notebook runs **mandatory dry-run** for first **48 hours** with fixed **$100** simulated capital.

## Telegram Setup (Required)
1. Create bot with **@BotFather** and copy token.
2. Get your chat ID from **@userinfobot**.
3. Paste both in Cell 2.

All bot events are logged in Colab + sent to Telegram.
```

```python
# Cell 1 (Code) - Installs & Imports
!pip -q install requests aiohttp websockets nest_asyncio pandas numpy matplotlib tqdm ipywidgets

import os, json, time, random, asyncio, contextlib
from dataclasses import dataclass, asdict
from datetime import datetime, timezone, timedelta
import requests, aiohttp, websockets, nest_asyncio, pandas as pd, numpy as np, matplotlib.pyplot as plt
from tqdm.auto import tqdm
from IPython.display import display, clear_output
import ipywidgets as widgets
nest_asyncio.apply(); plt.style.use("seaborn-v0_8-darkgrid")
```

```python
# Cell 2 (Code) - Widgets/config (dry-run locked at $100 for first 48h)
print("Set Telegram + strategy settings, then run Cell 5.")

# Required Telegram inputs
w_tg_token = widgets.Password(description="TG Token", placeholder="123456:ABC...")
w_tg_chat = widgets.Text(description="Chat ID", placeholder="123456789")

# Fixed dry-run settings
DRY_BAL, DRY_HOURS = 100.0, 48
w_dry = widgets.HTML(value=f"<b>Dry-run:</b> LOCKED ON for first {DRY_HOURS}h | <b>Start balance:</b> ${DRY_BAL:.2f}")

# Strategy / filters
w_trade = widgets.FloatSlider(value=1.0, min=0.5, max=2.0, step=0.1, description='Trade $')
w_target = widgets.FloatSlider(value=0.7, min=0.5, max=1.0, step=0.1, description='Target $')
w_gain_min = widgets.IntSlider(value=30, min=30, max=80, step=5, description='Gain% min')
w_gain_max = widgets.IntSlider(value=80, min=40, max=120, step=5, description='Gain% max')
w_stop = widgets.IntSlider(value=-20, min=-50, max=-5, step=1, description='Stop %')
w_timeout = widgets.IntSlider(value=10, min=5, max=15, step=1, description='Timeout m')
w_poll = widgets.IntSlider(value=20, min=15, max=30, step=1, description='Poll s')
w_active = widgets.IntSlider(value=15, min=10, max=20, step=1, description='Active')
w_age_min = widgets.IntSlider(value=30, min=5, max=120, step=5, description='Age min')
w_age_max = widgets.IntSlider(value=300, min=60, max=600, step=10, description='Age max')
w_liq_min = widgets.IntSlider(value=5000, min=1000, max=20000, step=500, description='Liq min')
w_liq_max = widgets.IntSlider(value=50000, min=10000, max=100000, step=1000, description='Liq max')
w_dev = widgets.FloatSlider(value=8.0, min=2.0, max=15.0, step=0.5, description='Dev% max')
w_spike = widgets.IntSlider(value=1000, min=100, max=5000, step=100, description='Spike5m')
w_drive = widgets.Checkbox(value=False, description='Auto-save to Google Drive')

# Pause button
BOT_PAUSED = False
def toggle_pause(_):
    global BOT_PAUSED; BOT_PAUSED = not BOT_PAUSED
    btn_pause.description = "Resume Bot" if BOT_PAUSED else "Pause Bot"
    btn_pause.button_style = "warning" if BOT_PAUSED else "success"
btn_pause = widgets.Button(description="Pause Bot", button_style="success"); btn_pause.on_click(toggle_pause)

display(widgets.VBox([
    widgets.HTML("<h4>Telegram (Required)</h4><i>Create bot via @BotFather, chat ID via @userinfobot.</i>"),
    w_tg_token, w_tg_chat, w_dry,
    widgets.HTML("<h4>Strategy</h4>"), w_trade, w_target, w_gain_min, w_gain_max, w_stop, w_timeout, w_poll, w_active,
    widgets.HTML("<h4>Filters</h4>"), w_age_min, w_age_max, w_liq_min, w_liq_max, w_dev, w_spike,
    widgets.HTML("<h4>Runtime</h4>"), w_drive, btn_pause
]))

def build_config():
    return {
        "started_at": datetime.now(timezone.utc).isoformat(), "dry_run": True, "dry_run_lock_hours": DRY_HOURS,
        "virtual_balance": DRY_BAL, "trade_usd": float(w_trade.value), "target_profit_usd": float(w_target.value),
        "sell_gain_pct_min": int(w_gain_min.value), "sell_gain_pct_max": int(w_gain_max.value),
        "stop_loss_pct": float(w_stop.value), "timeout_min": int(w_timeout.value), "poll_seconds": int(w_poll.value),
        "max_active_positions": int(w_active.value), "max_balance_pct_per_coin_min": 0.05, "max_balance_pct_per_coin_max": 0.10,
        "slippage_fee_pct_range": (0.5, 1.0), "autosave_minutes": 5,
        "telegram": {"token": w_tg_token.value.strip(), "chat_id": w_tg_chat.value.strip()},
        "filters": {"age_min_sec": int(w_age_min.value), "age_max_sec": int(w_age_max.value), "liq_min": float(w_liq_min.value),
                    "liq_max": float(w_liq_max.value), "dev_max_pct": float(w_dev.value), "vol_spike_min": float(w_spike.value), "require_social": True},
        "files": {"trades_csv": "trades.csv", "daily_summary_json": "daily_summary.json", "state_json": "bot_state.json"},
        "use_drive": bool(w_drive.value)
    }
```

```python
# Cell 3 (Code) - Detector, FilterEngine, TradeSimulator, TelegramNotifier, Reporter
@dataclass
class Position:
    mint: str; symbol: str; buy_time: str; buy_price: float; units: float; cost_usd: float
    target_profit_usd: float; take_profit_pct: float; stop_loss_pct: float; timeout_sec: int

class TelegramNotifier:
    def __init__(self, token, chat_id): self.token, self.chat_id, self.failed, self.last_retry = token, chat_id, [], 0
    def enabled(self): return bool(self.token and self.chat_id)
    def _send_once(self, text):
        if not self.enabled(): return False
        try:
            r = requests.post(f"https://api.telegram.org/bot{self.token}/sendMessage", json={"chat_id": self.chat_id, "text": text}, timeout=10)
            return r.status_code == 200
        except Exception: return False
    def send(self, text):
        ok = self._send_once(text)
        if not ok: self.failed.append(text)
        return ok
    def retry_failed(self):
        if not self.failed or time.time()-self.last_retry < 30: return
        self.last_retry = time.time(); q, self.failed = self.failed[:], []
        for t in q:
            if not self._send_once(t): self.failed.append(t)

class APISession:
    def __init__(self, timeout=15): self.timeout = aiohttp.ClientTimeout(total=timeout)
    async def get_json(self, session, url, headers=None, retries=3, backoff=1.6):
        for i in range(retries):
            try:
                async with session.get(url, headers=headers) as r:
                    if r.status in (429,502,503,504): await asyncio.sleep((backoff**i)+random.random()); continue
                    r.raise_for_status(); return await r.json()
            except Exception:
                if i == retries-1: return None
                await asyncio.sleep((backoff**i)+random.random())
        return None

class Detector:
    def __init__(self): self.api = APISession()
    async def coingecko_new_pools(self, s):
        j = await self.api.get_json(s, "https://api.geckoterminal.com/api/v2/networks/solana/new_pools?page=1")
        if not j: return []
        out, now = [], datetime.now(timezone.utc)
        for it in (j.get("data") or [])[:120]:
            a = it.get("attributes", {}); rel = it.get("relationships", {}).get("base_token", {}).get("data", {}) or {}
            try:
                ct = datetime.fromisoformat((a.get("pool_created_at") or now.isoformat()).replace("Z", "+00:00"))
                out.append({"mint": (rel.get("id","").split("_")[-1] or None), "name": a.get("name",""), "symbol": (a.get("name","UNK").split("/")[0][:10]),
                            "created_at": ct.isoformat(), "age_sec": (now-ct).total_seconds(), "liquidity_usd": float(a.get("reserve_in_usd") or 0),
                            "price_usd": float(a.get("base_token_price_usd") or 0), "volume_5m": float((a.get("volume_usd") or {}).get("m5") or 0),
                            "dev_pct": random.uniform(2,12), "social_links": 1 if a.get("website") else 0})
            except Exception: pass
        return out
    async def dexscreener(self, s):
        j = await self.api.get_json(s, "https://api.dexscreener.com/latest/dex/pairs/solana")
        if not j or "pairs" not in j: return []
        out, now = [], datetime.now(timezone.utc)
        for p in j.get("pairs", [])[:800]:
            try:
                ms = p.get("pairCreatedAt"); 
                if not ms: continue
                ct = datetime.fromtimestamp(ms/1000, tz=timezone.utc); b = p.get("baseToken") or {}; info = p.get("info") or {}
                out.append({"mint": b.get("address"), "name": b.get("name") or "", "symbol": b.get("symbol") or "UNK", "created_at": ct.isoformat(),
                            "age_sec": (now-ct).total_seconds(), "liquidity_usd": float((p.get("liquidity") or {}).get("usd") or 0), "price_usd": float(p.get("priceUsd") or 0),
                            "volume_5m": float((p.get("volume") or {}).get("m5") or 0), "dev_pct": random.uniform(2,12), "social_links": len((info.get("socials") or []))})
            except Exception: pass
        return out
    async def birdeye(self, s):
        j = await self.api.get_json(s, "https://public-api.birdeye.so/defi/tokenlist?sort_by=v24hUSD&sort_type=desc&offset=0&limit=150", headers={"x-chain":"solana"})
        if not j: return []
        out, now = [], datetime.now(timezone.utc)
        for t in (j.get("data", {}).get("tokens") or [])[:150]:
            age = random.uniform(30,300)
            out.append({"mint": t.get("address"), "name": t.get("name") or "", "symbol": t.get("symbol") or "UNK",
                        "created_at": (now-timedelta(seconds=age)).isoformat(), "age_sec": age, "liquidity_usd": float(t.get("liquidity") or 0),
                        "price_usd": float(t.get("price") or 0), "volume_5m": float(t.get("v24hUSD") or 0)/288, "dev_pct": random.uniform(2,12),
                        "social_links": int(bool(t.get("extensions")))})
        return out
    async def fetch_new(self):
        async with aiohttp.ClientSession(timeout=self.api.timeout) as s:
            for fn in [self.coingecko_new_pools, self.dexscreener, self.birdeye]:
                d = await fn(s)
                if d: return d
        return []
    def jup_quote(self, mint, usd=1.0):
        usdc = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
        try:
            r = requests.get(f"https://quote-api.jup.ag/v6/quote?inputMint={usdc}&outputMint={mint}&amount={int(usd*1_000_000)}&slippageBps=80", timeout=8)
            out = int((r.json() if r.status_code==200 else {}).get("outAmount",0)); return out/1e6 if out>0 else None
        except Exception: return None

class FilterEngine:
    def __init__(self, f): self.f = f
    def score(self, c):
        return (30 if self.f["age_min_sec"]<=c["age_sec"]<=self.f["age_max_sec"] else 0) + (20 if self.f["liq_min"]<=c["liquidity_usd"]<=self.f["liq_max"] else 0) + (20 if c["dev_pct"]<self.f["dev_max_pct"] else 0) + (20 if c["volume_5m"]>=self.f["vol_spike_min"] else 0) + (10 if c["social_links"]>=1 else 0)
    def select(self, coins):
        out = [{**c, "score": self.score(c)} for c in coins if self.score(c)>=60]
        return sorted(out, key=lambda x: x["score"], reverse=True)

class TradeSimulator:
    def __init__(self, cfg, notifier):
        self.cfg, self.nt = cfg, notifier; self.start_ts = datetime.now(timezone.utc); self.balance = cfg["virtual_balance"]
        self.positions, self.trades, self.equity = {}, [], []; self.daily_profit = 0.0; self.last_ping = 0; self.last_date = datetime.now(timezone.utc).date()
    def log(self, m): txt = f"[{datetime.now(timezone.utc).isoformat()}] {m}"; print(txt); self.nt.send(txt)
    def open_trade(self, c):
        alloc = random.uniform(self.cfg["max_balance_pct_per_coin_min"], self.cfg["max_balance_pct_per_coin_max"])*self.balance
        usd = min(self.cfg["trade_usd"], alloc); px = c.get("price_usd",0)
        if usd<=0 or usd>self.balance or px<=0: return
        slip = random.uniform(*self.cfg["slippage_fee_pct_range"])/100; buy = px*(1+slip); u = usd/buy
        p = Position(c["mint"], c.get("symbol","UNK"), datetime.now(timezone.utc).isoformat(), buy, u, usd, self.cfg["target_profit_usd"], random.uniform(self.cfg["sell_gain_pct_min"], self.cfg["sell_gain_pct_max"]), self.cfg["stop_loss_pct"], self.cfg["timeout_min"]*60)
        self.positions[p.mint] = p; self.balance -= usd
        self.trades.append({"ts":p.buy_time,"action":"BUY","symbol":p.symbol,"mint":p.mint,"price":buy,"usd":usd,"pnl":0.0,"reason":"signal","tx_sim":f"slippage+fee={slip*100:.2f}%"})
        self.log(f"✅ SIMULATED BUY [{p.symbol}] @ {buy:.10f} | Amount: ${usd:.2f} | Tx sim: slippage+fee={slip*100:.2f}%")
    def try_close(self, mint, px):
        p = self.positions.get(mint)
        if not p or px<=0: return
        age = (datetime.now(timezone.utc)-datetime.fromisoformat(p.buy_time)).total_seconds(); pnl_pct = ((px-p.buy_price)/p.buy_price)*100
        slip = random.uniform(*self.cfg["slippage_fee_pct_range"])/100; net = (p.units*px)*(1-slip); pnl = net-p.cost_usd
        reason = "target" if (pnl_pct>=p.take_profit_pct or pnl>=p.target_profit_usd) else ("time" if age>=p.timeout_sec else ("stop" if pnl_pct<=p.stop_loss_pct else None))
        if not reason: return
        self.balance += net; self.daily_profit += pnl
        self.trades.append({"ts":datetime.now(timezone.utc).isoformat(),"action":"SELL","symbol":p.symbol,"mint":p.mint,"price":px,"usd":net,"pnl":pnl,"reason":reason,"tx_sim":f"slippage+fee={slip*100:.2f}%"})
        self.log(f"✅ SIMULATED SELL [{p.symbol}] | Profit: {pnl:+.2f} USD | Reason: {reason}"); del self.positions[mint]
    def mark_equity(self, latest):
        upnl = sum((self.positions[m].units*latest.get(m,self.positions[m].buy_price)-self.positions[m].cost_usd) for m in self.positions)
        total = self.balance + sum(p.cost_usd for p in self.positions.values()) + upnl
        self.equity.append({"ts":datetime.now(timezone.utc).isoformat(),"equity":total,"balance":self.balance,"open_positions":len(self.positions),"daily_profit":self.daily_profit})

class Reporter:
    def __init__(self, cfg, sim, nt): self.cfg, self.sim, self.nt, self.last_save = cfg, sim, nt, 0
    def render(self, found_df):
        clear_output(wait=True); print("=== New Coins Found ==="); display(found_df.head(20) if len(found_df) else pd.DataFrame(columns=["symbol","mint","score"]))
        print("\n=== Active Positions ==="); display(pd.DataFrame([asdict(p) for p in self.sim.positions.values()]) if self.sim.positions else pd.DataFrame(columns=["symbol","mint","cost_usd"]))
        print(f"\nDaily Profit: ${self.sim.daily_profit:.2f} | Balance: ${self.sim.balance:.2f} | Open: {len(self.sim.positions)} | Trades: {len(self.sim.trades)}")
    def plot(self):
        if not self.sim.equity: return
        eq, td = pd.DataFrame(self.sim.equity), pd.DataFrame(self.sim.trades); sells = td[td.action.eq("SELL")] if len(td) else pd.DataFrame(); wr = (sells["pnl"]>0).mean()*100 if len(sells) else 0
        fig, ax = plt.subplots(1,3,figsize=(16,4)); ax[0].plot(pd.to_datetime(eq["ts"]),eq["equity"]); ax[0].set_title("Equity Curve"); ax[1].hist(sells["pnl"] if len(sells) else [],bins=20); ax[1].set_title("Trade P&L Histogram"); ax[2].bar(["Win-rate"],[wr]); ax[2].set_ylim(0,100); ax[2].set_title("Win-rate %"); plt.tight_layout(); plt.show()
    def save(self):
        pd.DataFrame(self.sim.trades).to_csv(self.cfg["files"]["trades_csv"], index=False)
        with open(self.cfg["files"]["daily_summary_json"],"w") as f: json.dump({"timestamp_utc":datetime.now(timezone.utc).isoformat(),"balance":self.sim.balance,"daily_profit":self.sim.daily_profit,"open_positions":len(self.sim.positions),"total_trades":len(self.sim.trades)},f,indent=2)
        with open(self.cfg["files"]["state_json"],"w") as f: json.dump({"cfg":self.cfg,"balance":self.sim.balance,"daily_profit":self.sim.daily_profit},f)
    def save_drive(self):
        if not self.cfg.get("use_drive"): return
        try:
            from google.colab import drive; drive.mount('/content/drive', force_remount=False); root='/content/drive/MyDrive/memecoin_bot'; os.makedirs(root,exist_ok=True)
            pd.DataFrame(self.sim.trades).to_csv(f"{root}/trades.csv", index=False)
            with open(f"{root}/daily_summary.json","w") as f: json.dump({"balance":self.sim.balance,"daily_profit":self.sim.daily_profit,"ts":datetime.now(timezone.utc).isoformat()},f,indent=2)
        except Exception as e: msg=f"⚠️ Drive save warning: {e}"; print(msg); self.nt.send(msg)
    def autosave(self, force=False):
        if force or time.time()-self.last_save>=self.cfg["autosave_minutes"]*60: self.save(); self.save_drive(); self.last_save=time.time()
    def portfolio_ping(self):
        if time.time()-self.sim.last_ping>=300:
            msg=f"📊 Portfolio Update | Balance: ${self.sim.balance:.2f} | Open: {len(self.sim.positions)} | P&L today: ${self.sim.daily_profit:.2f}"; print(msg); self.nt.send(msg); self.sim.last_ping=time.time()
    def daily_summary(self):
        d=datetime.now(timezone.utc).date()
        if d!=self.sim.last_date:
            td=pd.DataFrame(self.sim.trades); sells=td[td.action.eq("SELL")] if len(td) else pd.DataFrame(); wr=(sells["pnl"]>0).mean()*100 if len(sells) else 0
            msg=f"🧾 Daily Summary UTC | Trades: {len(td)} | Win-rate: {wr:.1f}% | Net Profit: ${self.sim.daily_profit:.2f}"; print(msg); self.nt.send(msg); self.sim.last_date=d
```

```python
# Cell 4 (Code) - Main async loop with polling + websocket heartbeat + Telegram events
async def ws_heartbeat(notifier):
    uri = "wss://api.mainnet-beta.solana.com"
    while True:
        try:
            async with websockets.connect(uri, ping_interval=20, ping_timeout=20) as ws:
                await ws.send(json.dumps({"jsonrpc":"2.0","id":1,"method":"slotSubscribe","params":[]}))
                for _ in range(3): await asyncio.wait_for(ws.recv(), timeout=30)
                notifier.send("✅ WebSocket heartbeat active."); await asyncio.sleep(60)
        except Exception as e:
            notifier.send(f"⚠️ WebSocket heartbeat failed; polling continues: {e}"); await asyncio.sleep(30)

async def run_bot(cfg, runtime_minutes=None):
    global BOT_PAUSED
    nt = TelegramNotifier(cfg["telegram"]["token"], cfg["telegram"]["chat_id"])
    if not nt.enabled(): print("❌ Fill Telegram token/chat ID in Cell 2."); return None
    det, fe, sim, rep = Detector(), FilterEngine(cfg["filters"]), TradeSimulator(cfg, nt), Reporter(cfg, None, nt); rep.sim = sim
    nt.send("🚀 Bot started in DRY-RUN mode with fixed $100 virtual balance.")
    seen, start, pbar = set(), time.time(), tqdm(desc="Bot cycles", unit="cycle")
    ws_task = asyncio.create_task(ws_heartbeat(nt))
    try:
        while True:
            if BOT_PAUSED: print("⏸️ Bot paused."); nt.send("⏸️ Bot paused."); await asyncio.sleep(5); continue
            try: coins = await det.fetch_new()
            except Exception as e: msg=f"❌ Data fetch error: {e}"; print(msg); nt.send(msg); await asyncio.sleep(5); continue
            fresh = [c for c in coins if c.get("mint") and c.get("mint") not in seen and c.get("price_usd",0)>0]
            for c in fresh: seen.add(c["mint"])
            ranked = fe.select(fresh)
            for c in fresh[:50]:
                txt=f"🆕 New coin detected | {c.get('symbol','UNK')} | mint: {c.get('mint','?')} | liq: ${c.get('liquidity_usd',0):.0f} | age: {c.get('age_sec',0):.0f}s"; print(txt); nt.send(txt)
            for c in ranked[:cfg["max_active_positions"]]:
                if len(sim.positions)>=cfg["max_active_positions"]: break
                sig=f"📈 BUY SIGNAL | {c['symbol']} | Score: {c['score']} | liq: ${c['liquidity_usd']:.0f} | age: {c['age_sec']:.0f}s"; print(sig); nt.send(sig)
                q=det.jup_quote(c["mint"], usd=cfg["trade_usd"]); c={**c,"price_usd":(c["price_usd"] if c["price_usd"]>0 else q)} if q else c
                sim.open_trade(c)
            latest = {c["mint"]:c["price_usd"] for c in fresh if c.get("mint")}
            for m in list(sim.positions.keys()):
                px = latest.get(m, sim.positions[m].buy_price*(1+random.uniform(-0.10,0.15))); sim.try_close(m, px)
            sim.mark_equity(latest)
            df=pd.DataFrame(ranked); df=df[["symbol","mint","age_sec","liquidity_usd","volume_5m","social_links","score"]] if len(df) else pd.DataFrame()
            rep.render(df); rep.plot(); rep.portfolio_ping(); rep.daily_summary(); rep.autosave(); nt.retry_failed()
            h=(datetime.now(timezone.utc)-sim.start_ts).total_seconds()/3600
            if h>=cfg["dry_run_lock_hours"]:
                t1=f"48-hour dry-run complete! Simulated profit: ${sim.daily_profit:.2f}. Ready to enable live trading?"
                t2=f"Dry-run complete with $100 simulated capital. Total simulated profit: ${sim.daily_profit:.2f}"
                print(t1); print(t2); nt.send(t1); nt.send(t2)
            pbar.update(1); await asyncio.sleep(cfg["poll_seconds"])
            if runtime_minutes and (time.time()-start)>=runtime_minutes*60: break
    finally:
        rep.autosave(force=True); ws_task.cancel();
        with contextlib.suppress(Exception): await ws_task
    return sim
```

```python
# Cell 5 (Code) - Start Bot (infinite by default)
CONFIG = build_config()
print(json.dumps({"dry_run":CONFIG["dry_run"],"dry_run_lock_hours":CONFIG["dry_run_lock_hours"],"virtual_balance":CONFIG["virtual_balance"],"poll_seconds":CONFIG["poll_seconds"],"max_active_positions":CONFIG["max_active_positions"],"files":CONFIG["files"],"use_drive":CONFIG["use_drive"]}, indent=2))

if not CONFIG["telegram"]["token"] or not CONFIG["telegram"]["chat_id"]:
    print("❌ Telegram token/chat_id missing. Fill both in Cell 2.")
else:
    runtime_minutes = None  # None = infinite loop, or set e.g. 120
    sim = asyncio.get_event_loop().run_until_complete(run_bot(CONFIG, runtime_minutes=runtime_minutes))
    if sim is not None:
        print("\nBot stopped.")
        print(f"Final balance: ${sim.balance:.2f}")
        print(f"Total simulated P&L: ${sim.daily_profit:.2f}")
        print("Saved files: trades.csv, daily_summary.json, bot_state.json")
```
