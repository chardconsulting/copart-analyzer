"""
Copart Daily Auction Analyzer
------------------------------
Apify (Copart) + Claude API
→ Writes ranked results to Google Sheets daily

SETUP: See SETUP_GUIDE.md
DEPLOY: Railway.app (runs 7am + 12pm daily, no PC needed)
"""

import os
import sys
import json
import time
import base64
import requests
import schedule
import gspread
from datetime import datetime
from google.oauth2.service_account import Credentials
from apify_client import ApifyClient
import anthropic

# ─────────────────────────────────────────────
# CONFIG — Set as environment variables on Railway
# ─────────────────────────────────────────────

APIFY_API_KEY      = os.getenv("APIFY_API_KEY",      "YOUR_APIFY_KEY_HERE")
ANTHROPIC_API_KEY  = os.getenv("ANTHROPIC_API_KEY",  "YOUR_ANTHROPIC_KEY_HERE")
GOOGLE_SHEET_ID    = os.getenv("GOOGLE_SHEET_ID",    "YOUR_SHEET_ID_HERE")
GOOGLE_CREDS_JSON  = os.getenv("GOOGLE_CREDS_JSON",  "")

# GA Copart yards: Atlanta South (855), Atlanta North (146), Savannah (187), Tifton (191)
COPART_SEARCH_URL = "https://www.copart.com/lotSearchResults?free=false&searchCriteria=%7B%22query%22%3A%5B%22%2A%22%5D%2C%22filter%22%3A%7B%22ODM%22%3A%5B%22odometer_reading_received%3A%5B0%20TO%209999999%5D%22%5D%2C%22YEAR%22%3A%5B%22lot_year%3A%5B2006%20TO%202027%5D%22%5D%2C%22LOC%22%3A%5B%22yard_name%3A%5C%22GA%20-%20ATLANTA%20SOUTH%5C%22%22%2C%22yard_name%3A%5C%22GA%20-%20ATLANTA%20NORTH%5C%22%22%2C%22yard_name%3A%5C%22GA%20-%20ATLANTA%20EAST%5C%22%22%2C%22yard_name%3A%5C%22GA%20-%20ATLANTA%20WEST%5C%22%22%5D%2C%22MISC%22%3A%5B%22%23VehicleTypeCode%3AVEHTYPE_V%22%5D%7D%7D"

MAX_ITEMS        = 50
MIN_MARGIN_PCT   = 25
MAX_MILEAGE      = 50000
COPART_BUYER_FEE = 1750
TARGET_MARGIN_PCT = 40

SHEET_HEADERS = [
    "Date", "Verdict", "Vehicle", "Lot #", "Odometer", "Primary Damage",
    "Current Bid", "ACV", "Repair Low", "Repair High",
    "Total In Low", "Total In High", "Margin % Best", "Margin % Worst",
    "MAX BID", "BIN Price", "BIN Worth It?", "BIN Verdict",
    "Recommended Resale Price", "Hidden Risks", "Verdict Reason", "URL"
]

# ─────────────────────────────────────────────
# GOOGLE SHEETS
# ─────────────────────────────────────────────

def get_sheet():
    creds_dict = json.loads(GOOGLE_CREDS_JSON)
    scopes = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds  = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    client = gspread.authorize(creds)
    sheet  = client.open_by_key(GOOGLE_SHEET_ID)
    tab_name = datetime.now().strftime("%Y-%m-%d")
    try:
        worksheet = sheet.worksheet(tab_name)
    except gspread.exceptions.WorksheetNotFound:
        worksheet = sheet.add_worksheet(title=tab_name, rows=500, cols=len(SHEET_HEADERS))
        worksheet.append_row(SHEET_HEADERS)
        worksheet.format("1:1", {"textFormat": {"bold": True}})
    return worksheet

def write_results_to_sheet(results):
    print("\n📊 Writing results to Google Sheets...")
    try:
        worksheet = get_sheet()
        rows = []
        for r in results:
            if not r:
                continue
            verdict = r.get("verdict", "N/A")
            verdict_display = {
                "HELL YEAH":  "🟢 HELL YEAH",
                "GOOD BUY":   "🟡 GOOD BUY",
                "RISKY":      "🟠 RISKY",
                "FUCK NO":    "🔴 FUCK NO",
                "DO NOT BUY": "🚨 DO NOT BUY"
            }.get(verdict, verdict)

            rows.append([
                datetime.now().strftime("%Y-%m-%d %H:%M"),
                verdict_display,
                r.get("vehicle", ""),
                r.get("lot_number", ""),
                r.get("odometer", ""),
                r.get("primary_damage", ""),
                r.get("current_bid", 0),
                r.get("acv", 0),
                r.get("repair_cost_low", 0),
                r.get("repair_cost_high", 0),
                r.get("total_cost_low", 0),
                r.get("total_cost_high", 0),
                f"{r.get('margin_pct_best', 0):.0f}%",
                f"{r.get('margin_pct_worst', 0):.0f}%",
                f"${r.get('max_bid', 0):,}",
                f"${r.get('buy_it_now_price', 0):,}" if r.get('buy_it_now_price', 0) > 0 else "N/A",
                "YES" if r.get("bin_worth_it") else "NO",
                r.get("bin_verdict", ""),
                r.get("recommended_resale_price", 0),
                " | ".join(r.get("hidden_risks", [])),
                r.get("verdict_reason", ""),
                r.get("url", ""),
            ])

        if rows:
            worksheet.append_rows(rows)
            print(f"✅ Wrote {len(rows)} rows to Google Sheets tab '{datetime.now().strftime('%Y-%m-%d')}'")
        else:
            print("⚠️ No results to write.")

    except Exception as e:
        print(f"❌ Google Sheets write failed: {e}")
        fname = f"auction_results_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
        with open(fname, "w") as f:
            json.dump(results, f, indent=2)
        print(f"📁 Fallback: saved locally to {fname}")

# ─────────────────────────────────────────────
# BID CALCULATORS
# ─────────────────────────────────────────────

def calc_max_bid(resale_price, repair_cost_high, target_margin=TARGET_MARGIN_PCT):
    if not resale_price or resale_price <= 0:
        return 0
    max_bid = (resale_price * (1 - target_margin / 100)) - repair_cost_high - COPART_BUYER_FEE
    return max(0, int(max_bid))

def calc_bin_verdict(bin_price, resale_price, repair_cost_high):
    if not bin_price or bin_price <= 0:
        return False, 0, "No BIN available"
    total_in = bin_price + COPART_BUYER_FEE + repair_cost_high
    if resale_price and resale_price > 0:
        margin = (resale_price - total_in) / resale_price * 100
    else:
        margin = 0
    if margin >= TARGET_MARGIN_PCT:
        verdict = f"YES — locks in {margin:.0f}% margin, eliminates auction risk"
    elif margin >= 20:
        verdict = f"MAYBE — only {margin:.0f}% margin at BIN, below your {TARGET_MARGIN_PCT}% target"
    else:
        verdict = f"NO — BIN leaves only {margin:.0f}% margin, better to bid or skip"
    return margin >= TARGET_MARGIN_PCT, round(margin, 1), verdict

# ─────────────────────────────────────────────
# STEP 1: Fetch Copart lots
# ─────────────────────────────────────────────

def fetch_copart_lots():
    print("🔍 Fetching Copart lots via Apify...")
    client = ApifyClient(APIFY_API_KEY)
    run    = client.actor("parseforge/copart-public-search-scraper").call(
        run_input={"startUrl": COPART_SEARCH_URL, "maxItems": MAX_ITEMS},
        memory_mbytes=1024
    )
    items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
    print(f"✅ Fetched {len(items)} lots")
    return items

# ─────────────────────────────────────────────
# STEP 2: Filter lots
# ─────────────────────────────────────────────

def parse_odometer(s):
    if not s:
        return 999999
    try:
        digits = ''.join(c for c in str(s).split('mi')[0] if c.isdigit() or c == ',')
        return int(digits.replace(',', ''))
    except:
        return 999999

CEL_KEYWORDS = [
    "check engine", "engine light", "cel ", "obd", "fault code",
    "trouble code", "malfunction indicator", "mil light"
]

def has_check_engine_light(lot):
    fields_to_check = [
        str(lot.get("primary_damage", "") or ""),
        str(lot.get("secondary_damage", "") or ""),
        str(lot.get("highlights", "") or ""),
        str(lot.get("doc_type", "") or ""),
        str(lot.get("damage_details", "") or ""),
        str(lot.get("build_sheet", "") or ""),
    ]
    combined = " ".join(fields_to_check).lower()
    return any(kw in combined for kw in CEL_KEYWORDS)

def filter_lots(lots):
    filtered = []
    skipped_cel = 0
    for lot in lots:
        odo = parse_odometer(lot.get("odometer", ""))
        bid = lot.get("current_bid", 0) or 0
        acv = lot.get("estimated_retail_value", 0) or 0
        if acv <= 0:
            continue
        if (acv - bid - COPART_BUYER_FEE) / acv * 100 < MIN_MARGIN_PCT:
            continue
        if has_check_engine_light(lot):
            lot["_cel_detected"] = True
            skipped_cel += 1
        filtered.append(lot)
    print(f"✅ {len(filtered)} lots passed filters (of {len(lots)} total) — {skipped_cel} flagged for CEL")
    return filtered

# ─────────────────────────────────────────────
# STEP 3: Analyze with Claude
# ─────────────────────────────────────────────

def analyze_lot_with_claude(lot):
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    image_urls   = (lot.get("images_high_res") or lot.get("images_full") or [])[:4]
    image_blocks = []
    for url in image_urls:
        try:
            resp = requests.get(url, timeout=10)
            if resp.status_code == 200:
                img_data = base64.b64encode(resp.content).decode("utf-8")
                ct = resp.headers.get("Content-Type", "image/jpeg").split(";")[0]
                image_blocks.append({"type": "image",
                    "source": {"type": "base64", "media_type": ct, "data": img_data}})
        except Exception as e:
            print(f"  ⚠️ Image failed: {e}")

    damage_details = lot.get("damage_details", [])
    damage_str = "\n".join(
        f"- {d.get('aasc_item_description','')}: {d.get('aasc_damage_description','')}"
        f" ({d.get('aasc_severity_description','')})"
        for d in damage_details
    ) if isinstance(damage_details, list) and damage_details else "No structured damage data."

    acv = lot.get("estimated_retail_value", 0) or 0

    prompt = f"""You are an expert used car flipper and mechanic in Georgia. Analyze this Copart lot.

LOT:
Vehicle: {lot.get('year')} {lot.get('make')} {lot.get('model')} {lot.get('trim','')}
Lot #: {lot.get('lot_number')} | Odo: {lot.get('odometer')} | Color: {lot.get('color')}
Current Bid: ${lot.get('current_bid',0):,} | ACV (Copart estimate): ${acv:,}
Damage: {lot.get('primary_damage')} | Title: {lot.get('title_group_description') or lot.get('title_type') or 'Unknown'}
Buyer Fee: ${COPART_BUYER_FEE:,}
Damage Report: {damage_str}

Use the ACV as your resale price baseline. Cross-check with your knowledge of current Georgia used car market values.
Target margin: {TARGET_MARGIN_PCT}%. Flag any hidden risks (hybrid battery, timing chain, flood history, etc).

Respond ONLY with this JSON (no markdown, no extra text):
{{
  "damage_items": [{{"component": "...", "issue": "...", "cost_low": 0, "cost_high": 0}}],
  "repair_cost_low": 0,
  "repair_cost_high": 0,
  "hidden_risks": ["..."],
  "recommended_resale_price": 0,
  "total_cost_low": 0,
  "total_cost_high": 0,
  "margin_pct_best": 0.0,
  "margin_pct_worst": 0.0,
  "verdict": "HELL YEAH | GOOD BUY | RISKY | FUCK NO",
  "verdict_reason": "..."
}}"""

    try:
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1000,
            messages=[{"role": "user", "content": image_blocks + [{"type": "text", "text": prompt}]}]
        )
        raw = response.content[0].text.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        analysis = json.loads(raw.strip())
        analysis.update({
            "current_bid": lot.get("current_bid", 0),
            "buy_it_now_price": lot.get("buy_it_now_price", 0) or 0,
            "acv": acv,
            "lot_number": lot.get("lot_number"),
            "vehicle": f"{lot.get('year')} {lot.get('make')} {lot.get('model')} {lot.get('trim','')}",
            "odometer": lot.get("odometer"),
            "url": f"https://www.copart.com/lot/{lot.get('lot_number')}",
            "primary_damage": lot.get("primary_damage"),
            "auction_date": lot.get("auction_date"),
        })

        # HARD OVERRIDE: Check engine light = DO NOT BUY
        if lot.get("_cel_detected"):
            analysis["verdict"]        = "DO NOT BUY"
            analysis["verdict_reason"] = "Check engine light detected. Unknown fault codes make repair costs unpredictable — automatic disqualification."
            analysis["max_bid"]        = 0
            analysis["bin_worth_it"]   = False
            analysis["bin_verdict"]    = "NO — Check engine light detected"
            analysis["bin_margin"]     = 0
            return analysis

        resale    = analysis.get("recommended_resale_price", 0) or acv
        repair_h  = analysis.get("repair_cost_high", 0)
        bin_price = analysis.get("buy_it_now_price", 0)

        analysis["max_bid"] = calc_max_bid(resale, repair_h)
        bin_worth, bin_margin, bin_verdict = calc_bin_verdict(bin_price, resale, repair_h)
        analysis["bin_worth_it"]  = bin_worth
        analysis["bin_margin"]    = bin_margin
        analysis["bin_verdict"]   = bin_verdict

        return analysis
    except Exception as e:
        print(f"  ❌ Claude failed: {e}")
        return None

# ─────────────────────────────────────────────
# STEP 4: Sort & output
# ─────────────────────────────────────────────

VERDICT_RANK = {"HELL YEAH": 0, "GOOD BUY": 1, "RISKY": 2, "FUCK NO": 3, "DO NOT BUY": 4}

def process_results(results):
    results = [r for r in results if r]
    results.sort(key=lambda r: (
        VERDICT_RANK.get(r.get("verdict", "FUCK NO"), 3),
        -r.get("margin_pct_best", 0)
    ))

    print("\n" + "="*70)
    print(f"🏆 RESULTS — {datetime.now().strftime('%b %d, %Y %I:%M %p')}")
    print("="*70)
    for i, r in enumerate(results, 1):
        v = r.get("verdict", "N/A")
        e = {"HELL YEAH": "🟢", "GOOD BUY": "🟡", "RISKY": "🟠", "FUCK NO": "🔴", "DO NOT BUY": "🚨"}.get(v, "⚪")
        print(f"\n#{i} {e} {v} — {r['vehicle']} | {r['odometer']}")
        print(f"   Bid ${r['current_bid']:,} → Max Bid: ${r.get('max_bid',0):,} | Margin {r['margin_pct_worst']:.0f}%–{r['margin_pct_best']:.0f}%")
        bin_p = r.get('buy_it_now_price', 0)
        if bin_p > 0:
            print(f"   BIN: ${bin_p:,} → {r.get('bin_verdict','')}")
        print(f"   💬 {r.get('verdict_reason','')}")
        print(f"   🔗 {r['url']}")

    write_results_to_sheet(results)
    return results

# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def run(quick=False):
    print(f"\n🚗 Copart Analyzer — Full Run — {datetime.now().strftime('%b %d %Y %I:%M %p')}\n")
    lots       = fetch_copart_lots()
    candidates = filter_lots(lots)

    if not candidates:
        print("No lots passed filters today.")
        return

    print(f"\n🤖 Analyzing {len(candidates)} lots...")
    results = []
    for i, lot in enumerate(candidates, 1):
        year  = lot.get("year")
        make  = lot.get("make", "")
        model = lot.get("model", "")
        print(f"\n  [{i}/{len(candidates)}] {year} {make} {model} (#{lot.get('lot_number')})")
        results.append(analyze_lot_with_claude(lot))
        time.sleep(1)

    process_results(results)

def run_scheduled():
    print("📅 Scheduler active — Full run 7:00 AM ET daily")
    schedule.every().day.at("07:00").do(run)
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    if "--schedule" in sys.argv:
        run_scheduled()
    else:
        run()
