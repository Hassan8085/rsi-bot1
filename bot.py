import os, io, re, time, json, random, html
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from zoneinfo import ZoneInfo

import requests
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# ======================= تنظیمات =======================
TOKEN = os.environ["BOT_TOKEN"]                      # توکن ربات از @BotFather
CHANNEL = os.environ.get("CHANNEL", "@Trading_ACO1")  # آیدی کانال (ربات باید ادمین باشه)
TFS = os.environ.get("TFS", "5m,15m,30m,1h,4h").split(",")
OB = float(os.environ.get("OB", 80))                 # اشباع خرید
OS = float(os.environ.get("OS", 20))                 # اشباع فروش
PERIOD = 14
TOP_N = int(os.environ.get("TOP_N", 120))            # تعداد ارزهای پرحجم
MIN_VOL = float(os.environ.get("MIN_VOL", 3_000_000))  # حداقل حجم ۲۴ ساعته (دلار)
COOLDOWN = int(os.environ.get("COOLDOWN", 3 * 3600))  # فاصله بین دو هشدار یک ارز
SCAN_EVERY = int(os.environ.get("SCAN_EVERY", 60))   # هر چند ثانیه یک اسکن
MAX_PER_SCAN = int(os.environ.get("MAX_PER_SCAN", 4))  # سقف پست در هر اسکن (ضد اسپم)
WORKERS = int(os.environ.get("WORKERS", 8))
ONCE = os.environ.get("ONCE") == "1"
BINANCE = os.environ.get("BINANCE_API", "https://data-api.binance.vision")
STATE_FILE = "state.json"
TZ = ZoneInfo("Asia/Tehran")

TF_FA = {"1m": "۱ دقیقه", "3m": "۳ دقیقه", "5m": "۵ دقیقه", "15m": "۱۵ دقیقه", "30m": "۳۰ دقیقه",
         "1h": "۱ ساعت", "2h": "۲ ساعت", "4h": "۴ ساعت", "1d": "روزانه"}
FA_NAMES = {"BTC": "بیت‌کوین", "ETH": "اتریوم", "SOL": "سولانا", "XRP": "ریپل", "BNB": "بایننس‌کوین",
            "DOGE": "دوج‌کوین", "ADA": "کاردانو", "TON": "تون‌کوین", "TRX": "ترون", "LTC": "لایت‌کوین",
            "AVAX": "آوالانچ", "LINK": "چین‌لینک", "DOT": "پولکادات", "SHIB": "شیبا", "PEPE": "پپه"}
# ارزهای معروف که برایشان خبر جستجو می‌شود (نام کامل‌ها؛ تیکر هم خودکار)
NEWS_COINS = {"BTC": ["bitcoin"], "ETH": ["ethereum", "ether"], "SOL": ["solana"], "XRP": ["xrp", "ripple"],
              "BNB": ["bnb chain", "binance coin"], "DOGE": ["dogecoin"], "ADA": ["cardano"],
              "AVAX": ["avalanche"], "LINK": ["chainlink"], "TON": ["toncoin"], "DOT": ["polkadot"],
              "LTC": ["litecoin"], "TRX": ["tron"], "SHIB": ["shiba inu"], "PEPE": ["pepe"], "SUI": [],
              "NEAR": ["near protocol"], "UNI": ["uniswap"], "ATOM": ["cosmos"], "BCH": ["bitcoin cash"],
              "XLM": ["stellar"], "APT": ["aptos"], "ARB": ["arbitrum"], "HBAR": ["hedera"], "INJ": ["injective"]}
FEEDS = [("CoinDesk", "https://www.coindesk.com/arc/outboundfeeds/rss/"),
         ("Cointelegraph", "https://cointelegraph.com/rss"),
         ("Decrypt", "https://decrypt.co/feed"),
         ("Bitcoin Magazine", "https://bitcoinmagazine.com/.rss/full/")]
STABLES = ("USDC", "FDUSD", "TUSD", "USDP", "DAI", "EUR", "AEUR", "USDE", "XUSD", "BFUSD")

BG, GRID, UPC, DNC = "#131722", "#2a2e39", "#26a69a", "#ef5350"
S = requests.Session()
S.headers["User-Agent"] = "Mozilla/5.0"
_DIG = str.maketrans("0123456789", "۰۱۲۳۴۵۶۷۸۹")


def fad(s):
    return str(s).translate(_DIG)


def now_ir():
    return datetime.now(TZ)


def g2j(gy, gm, gd):
    g_d_m = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334]
    gy2 = gy + 1 if gm > 2 else gy
    days = 355666 + (365 * gy) + ((gy2 + 3) // 4) - ((gy2 + 99) // 100) + ((gy2 + 399) // 400) + gd + g_d_m[gm - 1]
    jy = -1595 + (33 * (days // 12053))
    days %= 12053
    jy += 4 * (days // 1461)
    days %= 1461
    if days > 365:
        jy += (days - 1) // 365
        days = (days - 1) % 365
    if days < 186:
        jm, jd = 1 + days // 31, 1 + days % 31
    else:
        jm, jd = 7 + (days - 186) // 30, 1 + (days - 186) % 30
    return jy, jm, jd


def stamp_fa():
    dt = now_ir()
    jy, jm, jd = g2j(dt.year, dt.month, dt.day)
    return fad(f"{jy}/{jm:02d}/{jd:02d} ┆ {dt:%H:%M}")


def fmt(p):
    if p >= 1000:
        return f"{p:,.0f}"
    if p >= 1:
        return f"{p:.3f}".rstrip("0").rstrip(".")
    return f"{p:.6g}"


def load_state():
    try:
        return json.load(open(STATE_FILE))
    except Exception:
        return {}


def save_state(s):
    try:
        json.dump(s, open(STATE_FILE, "w"))
    except Exception:
        pass


# ======================= داده بازار =======================
def top_symbols():
    data = S.get(f"{BINANCE}/api/v3/ticker/24hr", timeout=20).json()
    rows = []
    for d in data:
        s = d["symbol"]
        if not s.endswith("USDT"):
            continue
        base = s[:-4]
        if base in STABLES or any(x in s for x in ("UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT")):
            continue
        if float(d["quoteVolume"]) < MIN_VOL:
            continue
        rows.append(d)
    rows.sort(key=lambda d: float(d["quoteVolume"]), reverse=True)
    return {d["symbol"]: d for d in rows[:TOP_N]}


def klines(symbol, tf, limit=100):
    try:
        r = S.get(f"{BINANCE}/api/v3/klines", params={"symbol": symbol, "interval": tf, "limit": limit},
                  timeout=20)
        if r.status_code in (418, 429):
            time.sleep(int(r.headers.get("Retry-After", 5)))
            return None
        df = pd.DataFrame(r.json(), columns=["t", "o", "h", "l", "c", "v", "ct", "q", "n", "tb", "tq", "i"])
    except Exception:
        return None
    for c in "ohlcv":
        df[c] = df[c].astype(float)
    df["t"] = pd.to_datetime(df["t"], unit="ms", utc=True).dt.tz_convert(TZ)
    return df


def rsi(close, period=PERIOD):
    d = close.diff()
    up = d.clip(lower=0).ewm(alpha=1 / period, adjust=False).mean()
    dn = (-d.clip(upper=0)).ewm(alpha=1 / period, adjust=False).mean()
    return 100 - 100 / (1 + up / dn.replace(0, np.nan))


def scan_symbol(sym):
    out = {}
    for tf in TFS:
        df = klines(sym, tf, 100)
        if df is None or len(df) < 40:
            continue
        v = float(rsi(df["c"]).iloc[-1])  # کندل زنده => اعلام سریع
        if v >= OB:
            out[tf] = ("OB", v, df)
        elif v <= OS:
            out[tf] = ("OS", v, df)
    return sym, out


# ======================= تحلیل سطوح =======================
def outlook(sym, side, price):
    df = klines(sym, "4h", 120)
    if df is None or len(df) < 30:
        return None
    hi, lo = df["h"].max(), df["l"].min()
    R = hi - lo
    atr = (df["h"] - df["l"]).tail(14).mean()
    if side == "OB":
        res = hi if price < hi * 0.995 else price + 1.5 * atr
        sup = [x for x in (hi - 0.382 * R, hi - 0.5 * R) if x < price]
        return {"main": res, "others": sup, "side": side}
    sup = lo if price > lo * 1.005 else price - 1.5 * atr
    res = [x for x in (lo + 0.236 * R, lo + 0.382 * R) if x > price]
    return {"main": sup, "others": res, "side": side}


# ======================= اخبار =======================
_news = {"t": 0, "items": []}
_tr = {}
_fng = {"t": 0, "v": None}


def fetch_news():
    if time.time() - _news["t"] < 600:
        return _news["items"]
    items = []
    for name, url in FEEDS:
        try:
            root = ET.fromstring(S.get(url, timeout=15).content)
            for it in root.iter("item"):
                try:
                    dt = parsedate_to_datetime(it.findtext("pubDate"))
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                except Exception:
                    continue
                if (datetime.now(timezone.utc) - dt).total_seconds() > 36 * 3600:
                    continue
                items.append({"src": name, "title": (it.findtext("title") or "").strip(),
                              "link": (it.findtext("link") or "").strip(), "dt": dt})
        except Exception:
            continue
    items.sort(key=lambda x: x["dt"], reverse=True)
    _news.update(t=time.time(), items=items)
    return items


def to_fa(text):
    if text in _tr:
        return _tr[text]
    try:
        r = S.get("https://translate.googleapis.com/translate_a/single",
                  params={"client": "gtx", "sl": "en", "tl": "fa", "dt": "t", "q": text}, timeout=8)
        res = "".join(x[0] for x in r.json()[0] if x[0])
    except Exception:
        res = text
    _tr[text] = res
    return res


def news_for(coin, n=2):
    if coin not in NEWS_COINS:
        return []
    pats = [re.compile(rf"\b{coin}\b")] + [re.compile(rf"\b{re.escape(a)}\b", re.I) for a in NEWS_COINS[coin]]
    res = []
    for it in fetch_news():
        if any(p.search(it["title"]) for p in pats):
            res.append(it)
        if len(res) >= n:
            break
    return res


def fear_greed():
    if time.time() - _fng["t"] > 1800:
        try:
            d = S.get("https://api.alternative.me/fng/", timeout=10).json()["data"][0]
            _fng.update(t=time.time(), v=int(d["value"]))
        except Exception:
            _fng["t"] = time.time()
    v = _fng["v"]
    if v is None:
        return None
    lab = ("ترس شدید" if v < 25 else "ترس" if v < 45 else "خنثی" if v < 55
           else "طمع" if v < 75 else "طمع شدید")
    return f"{fad(v)} ({lab})"


# ======================= چارت =======================
def make_chart(symbol, tf, df, r, side):
    df = df.tail(100).reset_index(drop=True)
    r = r.tail(100).reset_index(drop=True)
    n = len(df)
    fig = plt.figure(figsize=(10, 7.2), facecolor=BG)
    gs = fig.add_gridspec(2, 2, height_ratios=[3.2, 1], width_ratios=[6, 1], hspace=0.05, wspace=0.0)
    ax = fig.add_subplot(gs[0, 0], facecolor=BG)
    axv = fig.add_subplot(gs[0, 1], facecolor=BG, sharey=ax)
    axr = fig.add_subplot(gs[1, 0], facecolor=BG, sharex=ax)

    x = np.arange(n)
    colors = np.where(df["c"] >= df["o"], UPC, DNC)
    ax.vlines(x, df["l"], df["h"], color=colors, linewidth=1)
    ax.bar(x, (df["c"] - df["o"]).abs().clip(lower=1e-12), bottom=np.minimum(df["o"], df["c"]),
           color=colors, width=0.7)
    last = df["c"].iloc[-1]
    ax.axhline(last, color=UPC, linestyle="--", linewidth=0.8)
    ax.text(1.0, last, f" {fmt(last)} ", transform=ax.get_yaxis_transform(), color="white", fontsize=8,
            va="center", bbox=dict(facecolor=UPC, edgecolor="none"))

    bins = np.linspace(df["l"].min(), df["h"].max(), 30)
    hist, edges = np.histogram(df["c"], bins=bins, weights=df["v"])
    axv.barh((edges[:-1] + edges[1:]) / 2, hist, height=(edges[1] - edges[0]) * 0.9, color="#2962ff", alpha=0.8)
    axv.axis("off")

    axr.plot(x, r, color="#26a69a", linewidth=1)
    axr.axhspan(30, 70, color="#26a69a", alpha=0.12)
    for lv in (OS, 50, OB):
        axr.axhline(lv, color=GRID, linewidth=0.6, linestyle="--")
    axr.set_ylim(0, 100)
    axr.set_yticks([0, 20, 40, 60, 80])
    axr.text(0.005, 0.88, f"RSI ({PERIOD})", transform=axr.transAxes, color="#787b86", fontsize=8)
    axr.text(1.0, r.iloc[-1], f" {r.iloc[-1]:.2f} ", transform=axr.get_yaxis_transform(), color="white",
             fontsize=8, va="center", bbox=dict(facecolor=DNC if side == "OB" else UPC, edgecolor="none"))

    ticks = np.linspace(0, n - 1, 7).astype(int)
    axr.set_xticks(ticks)
    axr.set_xticklabels([df["t"].iloc[i].strftime("%H:%M") for i in ticks])
    ax.tick_params(labelbottom=False)
    for a in (ax, axr):
        a.yaxis.tick_right()
        a.tick_params(colors="#d1d4dc", labelsize=8)
        a.grid(color=GRID, linewidth=0.4)
        for s in a.spines.values():
            s.set_color(GRID)

    title = "RSI Extreme overbought" if side == "OB" else "RSI Extreme oversold"
    fig.text(0.5, 0.965, title, ha="center", color="#d1d4dc", fontsize=10)
    fig.text(0.08, 0.93, f"{symbol}, {tf}", color="#d1d4dc", fontsize=10, weight="bold")

    # واترمارک کانال: کم‌رنگ و مورب، بدون اشغال صفحه
    for (px, py) in [(0.28, 0.74), (0.60, 0.52), (0.30, 0.30)]:
        fig.text(px, py, CHANNEL, rotation=22, ha="center", va="center", color="white", alpha=0.085,
                 fontsize=28, weight="bold")
    fig.text(0.97, 0.012, CHANNEL, ha="right", color="#d1d4dc", alpha=0.75, fontsize=9, weight="bold")
    fig.text(0.08, 0.012, f"Updated: {now_ir():%Y/%m/%d %H:%M} (Tehran time)", color="#787b86", fontsize=7)

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=110, facecolor=BG)
    plt.close(fig)
    buf.seek(0)
    return buf


# ======================= کپشن (طبیعی و متنوع) =======================
INTRO_OB = ["{c} داغ شده! RSI رفته توی منطقه اشباع خرید 🔥",
            "⚠️ حواستون به {c} باشه؛ وارد ناحیه اشباع خرید شد",
            "{c} با قدرت بالا اومد و الان RSI تو سقف نشسته 📈",
            "🚨 {c} تو محدوده اشباع خرید قرار گرفت، احتمال تصحیح بالاست",
            "{c} زیادی گرم شده؛ RSI از خط قرمز رد شد 🌡"]
INTRO_OS = ["{c} حسابی ریخته! RSI رفته توی منطقه اشباع فروش 🧊",
            "👀 {c} وارد ناحیه اشباع فروش شد؛ شاید وقتِ واکنش باشه",
            "{c} سنگین فروخته شد و RSI کف نشسته 📉",
            "🚨 {c} تو محدوده اشباع فروش قرار گرفت، احتمال برگشت وجود داره",
            "{c} تا ته افت کرد؛ RSI زیر خط حمایتیِ ۲۰ ⚡️"]


def plain_len(s):
    return len(re.sub(r"<[^>]+>", "", html.unescape(s)))


def build_caption(sym, side, tfs, vals, price, chg, ol, news):
    coin = sym[:-4]
    fa = FA_NAMES.get(coin)
    c = f"<b>#{coin}</b>" + (f" ({fa})" if fa else "")
    ob = side == "OB"
    head = ("🔴 " if ob else "🟢 ") + random.choice(INTRO_OB if ob else INTRO_OS).format(c=c)

    lines = []
    for tf in tfs[::-1][:3]:
        lines.append(f"{'📉' if ob else '📈'} {TF_FA.get(tf, tf)} ┆ RSI <code>{vals[tf]:.1f}</code>")
    body = "\n".join(lines)

    cs = "+" if chg >= 0 else ""
    info = f"💰 قیمت ┆ <code>{fmt(price)}</code> دلار  ({fad(f'{cs}{chg:.1f}')}٪ در ۲۴ ساعت)"
    fg = fear_greed()
    if fg:
        info += f"\n🧭 شاخص ترس و طمع بازار ┆ {fg}"

    link = f"https://www.tradingview.com/chart/?symbol=BINANCE:{sym}"
    tv = f"📊 <a href=\"{link}\">مشاهده چارت در tradingview</a>"

    lvl = ""
    if ol:
        if ob:
            lvl = f"🎯 <b>سطوح مهم</b>\n🔺 مقاومت / سقف احتمالی ┆ <code>{fmt(ol['main'])}</code>"
            if ol["others"]:
                lvl += "\n🔻 حمایت‌های تصحیح ┆ " + " / ".join(f"<code>{fmt(x)}</code>" for x in ol["others"])
        else:
            lvl = f"🎯 <b>سطوح مهم</b>\n🛑 حمایت کلیدی ┆ <code>{fmt(ol['main'])}</code>"
            if ol["others"]:
                lvl += "\n🔼 اهداف برگشتی ┆ " + " / ".join(f"<code>{fmt(x)}</code>" for x in ol["others"])
        lvl = f"<blockquote>{lvl}</blockquote>"

    nw = ""
    if news:
        rows = []
        for it in news:
            t = to_fa(it["title"])
            t = html.escape(t if len(t) <= 95 else t[:92] + "…")
            rows.append(f"• {t} — <a href=\"{html.escape(it['link'])}\">{it['src']}</a>")
        nw = "📰 <b>خبرهای مرتبط</b>\n" + "\n".join(rows)

    foot = f"🕐 {stamp_fa()} (به وقت ایران)\n⚠️ توصیه خرید و فروش نیست\n💬 {CHANNEL}"

    def assemble(l, n):
        return "\n\n".join(x for x in (head, body, info, l, n, tv, foot) if x)

    cap = assemble(lvl, nw)
    if plain_len(cap) > 1000 and news:
        nw = nw.split("\n")
        nw = "\n".join(nw[:2]) if len(nw) > 2 else ""
        cap = assemble(lvl, nw)
    if plain_len(cap) > 1000:
        cap = assemble(lvl, "")
    if plain_len(cap) > 1000:
        cap = assemble("", "")
    return cap


# ======================= ارسال =======================
def send(sym, tf, df, side, caption):
    r = rsi(df["c"])
    photo = make_chart(sym, tf, df, r, side)
    for _ in range(3):
        resp = S.post(f"https://api.telegram.org/bot{TOKEN}/sendPhoto",
                      data={"chat_id": CHANNEL, "parse_mode": "HTML", "caption": caption},
                      files={"photo": ("chart.png", photo)}, timeout=40)
        if resp.status_code == 429:
            time.sleep(resp.json().get("parameters", {}).get("retry_after", 5) + 1)
            photo.seek(0)
            continue
        if not resp.ok:
            print("Telegram error:", resp.text)
        return resp.ok
    return False


def scan(state):
    tick = top_symbols()
    with ThreadPoolExecutor(WORKERS) as ex:
        results = list(ex.map(scan_symbol, list(tick)))
    now = time.time()
    cands = []
    for sym, out in results:
        if not out:
            continue
        ob = [tf for tf in TFS if tf in out and out[tf][0] == "OB"]
        os_ = [tf for tf in TFS if tf in out and out[tf][0] == "OS"]
        if len(ob) != len(os_):
            side = "OB" if len(ob) > len(os_) else "OS"
        else:
            so = max([out[t][1] - OB for t in ob] or [-1])
            sl = max([OS - out[t][1] for t in os_] or [-1])
            side = "OB" if so >= sl else "OS"
        tfs = ob if side == "OB" else os_
        key = f"{sym}:{side}"
        prev = state.get(key)
        if prev:
            age = now - prev["t"]
            new_high = bool((set(tfs) - set(prev["tfs"])) & {"1h", "4h"}) and age > 1800
            if age < COOLDOWN and not new_high:
                continue
        score = max(abs(out[t][1] - 50) for t in tfs) + 3 * len(tfs)
        cands.append((score, sym, side, tfs, out, key))
    cands.sort(reverse=True, key=lambda x: x[0])
    for _, sym, side, tfs, out, key in cands[:MAX_PER_SCAN]:
        d = tick[sym]
        price, chg = float(d["lastPrice"]), float(d["priceChangePercent"])
        vals = {t: out[t][1] for t in tfs}
        cap = build_caption(sym, side, tfs, vals, price, chg, outlook(sym, side, price), news_for(sym[:-4]))
        top_tf = tfs[-1]
        if send(sym, top_tf, out[top_tf][2], side, cap):
            state[key] = {"t": now, "tfs": tfs}
            save_state(state)
        time.sleep(3)


if __name__ == "__main__":
    st = load_state()
    while True:
        t0 = time.time()
        try:
            scan(st)
        except Exception as e:
            print("scan error:", e)
        save_state(st)
        if ONCE:
            break
        time.sleep(max(5, SCAN_EVERY - (time.time() - t0)))
