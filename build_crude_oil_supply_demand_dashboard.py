from __future__ import annotations

import csv
import io
import json
import re
import statistics
import urllib.request
from bisect import bisect_right
from datetime import datetime
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET
from zipfile import ZipFile

from bs4 import BeautifulSoup


WORKDIR = Path("/Users/anders/Documents/Playground")
OUTPUT_DATA_PATH = WORKDIR / "crude-oil-supply-demand-data.js"

EIA_COMMERCIAL_STOCKS_URL = "https://www.eia.gov/dnav/pet/hist/LeafHandler.ashx?n=PET&s=WCESTUS1&f=W"
EIA_CUSHING_STOCKS_URL = "https://www.eia.gov/dnav/pet/hist/LeafHandler.ashx?n=PET&s=W_EPC0_SAX_YCUOK_MBBL&f=W"
EIA_REFINERY_UTIL_URL = "https://www.eia.gov/dnav/pet/hist/LeafHandler.ashx?n=PET&s=WPULEUS3&f=W"
EIA_GASOLINE_STOCKS_URL = "https://www.eia.gov/dnav/pet/hist/LeafHandler.ashx?n=PET&s=WGTSTUS1&f=W"
EIA_DISTILLATE_STOCKS_URL = "https://www.eia.gov/dnav/pet/hist/LeafHandler.ashx?n=PET&s=WDISTUS1&f=W"
EIA_STEO_XLSX_URL = "https://www.eia.gov/outlooks/steo/xls/STEO_m.xlsx"
FRED_WTI_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DCOILWTICO"
FRED_GASOLINE_SPOT_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DGASNYH"
FRED_DIESEL_SPOT_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DDFUELNYH"
FLOATING_STORAGE_XLSX_PATH = Path("/Users/anders/Desktop/全球浮式原油库存.xlsx")

MANUAL_WTI_SPREAD_SERIES = [
    {"date": "2026-02-13", "front": 62.89, "second": 62.75},
    {"date": "2026-02-16", "front": 63.74, "second": 63.57},
    {"date": "2026-02-17", "front": 62.33, "second": 62.26},
    {"date": "2026-02-18", "front": 65.19, "second": 65.05},
    {"date": "2026-02-19", "front": 66.43, "second": 66.40},
    {"date": "2026-02-20", "front": 66.39, "second": 66.48},
    {"date": "2026-02-23", "front": 66.31, "second": 66.13},
    {"date": "2026-02-24", "front": 65.63, "second": 65.54},
    {"date": "2026-02-25", "front": 65.42, "second": 65.33},
    {"date": "2026-02-26", "front": 65.21, "second": 65.11},
    {"date": "2026-02-27", "front": 67.02, "second": 66.89},
    {"date": "2026-03-02", "front": 71.23, "second": 70.72},
    {"date": "2026-03-03", "front": 74.56, "second": 73.55},
    {"date": "2026-03-04", "front": 74.66, "second": 73.46},
    {"date": "2026-03-05", "front": 81.01, "second": 78.64},
    {"date": "2026-03-06", "front": 90.90, "second": 87.52},
    {"date": "2026-03-09", "front": 94.77, "second": 91.48},
    {"date": "2026-03-10", "front": 83.45, "second": 87.31},
]

# OPEC+ actual production now follows the official OPEC MOMR tables.
# For 2026-01 country rows, OPEC members use direct communication and
# non-OPEC participants use Table 5-7 secondary-source rows. Required
# production levels are aligned to the official OPEC+ monthly increase path.
MANUAL_OPEC_COUNTRY_ROWS = [
    {"country": "沙特", "target": 10.06, "actual": 10.13},
    {"country": "伊拉克", "target": 4.25, "actual": 4.13},
    {"country": "阿联酋", "target": 3.40, "actual": 3.39},
    {"country": "科威特", "target": 2.57, "actual": 2.59},
    {"country": "哈萨克斯坦", "target": 1.56, "actual": 1.57},
    {"country": "俄罗斯", "target": 9.53, "actual": 9.02},
    {"country": "阿曼", "target": 0.81, "actual": 0.81},
    {"country": "阿尔及利亚", "target": 0.97, "actual": 0.97},
]

MANUAL_OPEC_TREND = [
    {"date": "2025-10", "actual": 32.81, "target": 32.88},
    {"date": "2025-11", "actual": 32.90, "target": 33.02},
    {"date": "2025-12", "actual": 32.72, "target": 33.15},
    {"date": "2026-01", "actual": 32.61, "target": 33.15},
]


def fetch_text(url: str) -> str:
    request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(request, timeout=60) as response:
        return response.read().decode("utf-8", errors="ignore")


def fetch_bytes(url: str) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(request, timeout=60) as response:
        return response.read()


def parse_number(text: str) -> float | None:
    value = (
        text.replace(",", "")
        .replace("%", "")
        .replace("\xa0", "")
        .strip()
    )
    if value in {"", "-", "--", "NA", "W"}:
        return None
    return float(value)


def parse_eia_history_series(url: str) -> list[dict[str, Any]]:
    html = fetch_text(url)
    soup = BeautifulSoup(html, "html.parser")
    tbody = soup.find("tbody")
    if tbody is None:
        raise RuntimeError(f"Unable to locate EIA history table for {url}")

    series: list[dict[str, Any]] = []
    for row in tbody.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 11:
            continue

        year_month = cells[0].get_text(" ", strip=True).replace("\xa0", "")
        if not year_month or not year_month[0].isdigit() or "-" not in year_month:
            continue

        year_text, _month_text = year_month.split("-", 1)
        year = int(year_text)

        for index in range(1, len(cells), 2):
            if index + 1 >= len(cells):
                break
            end_date_text = cells[index].get_text(" ", strip=True).replace("\xa0", "")
            value_text = cells[index + 1].get_text(" ", strip=True)
            if not end_date_text or "/" not in end_date_text:
                continue
            value = parse_number(value_text)
            if value is None:
                continue

            month, day = end_date_text.split("/")
            date = f"{year:04d}-{int(month):02d}-{int(day):02d}"
            series.append({"date": date, "value": value})

    series.sort(key=lambda item: item["date"])
    return series


def parse_fred_wti_series() -> list[dict[str, Any]]:
    text = fetch_text(FRED_WTI_URL)
    reader = csv.DictReader(text.splitlines())
    series = []
    for row in reader:
        raw_value = row.get("DCOILWTICO", "").strip()
        if raw_value in {".", ""}:
            continue
        series.append({"date": row["observation_date"], "value": float(raw_value)})
    return series


def parse_fred_series(url: str, field_name: str) -> list[dict[str, Any]]:
    text = fetch_text(url)
    reader = csv.DictReader(text.splitlines())
    series = []
    for row in reader:
        raw_value = row.get(field_name, "").strip()
        if raw_value in {".", ""}:
            continue
        series.append({"date": row["observation_date"], "value": float(raw_value)})
    return series


def nearest_on_or_before(date: str, series: list[dict[str, Any]]) -> dict[str, Any] | None:
    dates = [item["date"] for item in series]
    index = bisect_right(dates, date) - 1
    if index < 0:
        return None
    return series[index]


def parse_user_floating_storage_series() -> list[dict[str, Any]]:
    ns = {
        "a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
        "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
    }

    with ZipFile(FLOATING_STORAGE_XLSX_PATH) as archive:
        shared_strings: list[str] = []
        if "xl/sharedStrings.xml" in archive.namelist():
            shared_root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
            for item in shared_root.findall("a:si", ns):
                shared_strings.append("".join(text.text or "" for text in item.iterfind(".//a:t", ns)))

        workbook = ET.fromstring(archive.read("xl/workbook.xml"))
        rels = ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
        rel_map = {rel.attrib["Id"]: rel.attrib["Target"] for rel in rels}
        first_sheet = workbook.find("a:sheets", ns)[0]
        rel_id = first_sheet.attrib["{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"]
        target = "xl/" + rel_map[rel_id]
        root = ET.fromstring(archive.read(target))

        series: list[dict[str, Any]] = []
        rows = root.findall(".//a:sheetData/a:row", ns)
        for row in rows[1:]:
            values: dict[str, str] = {}
            for cell in row.findall("a:c", ns):
                ref = cell.attrib.get("r", "")
                key = ref[:1]
                cell_type = cell.attrib.get("t")
                value_node = cell.find("a:v", ns)
                value = "" if value_node is None else value_node.text or ""
                if cell_type == "s" and value:
                    value = shared_strings[int(value)]
                values[key] = value

            date = values.get("A", "").strip()
            raw_value = values.get("B", "").strip()
            if not date or not raw_value:
                continue

            # The user-provided workbook is already in wan bbl; store internally
            # as million bbl to keep the rest of the page logic unchanged.
            series.append({"date": date, "value": float(raw_value) / 100})

    series.sort(key=lambda item: item["date"])
    return series


def align_price(date: str, price_series: list[dict[str, Any]]) -> float | None:
    dates = [item["date"] for item in price_series]
    index = bisect_right(dates, date) - 1
    if index < 0:
        return None
    return price_series[index]["value"]


def excel_col_to_index(ref: str) -> int:
    letters = re.match(r"([A-Z]+)", ref)
    if not letters:
        raise ValueError(f"Invalid cell ref: {ref}")
    value = 0
    for char in letters.group(1):
        value = value * 26 + (ord(char) - 64)
    return value


def read_sheet_rows(xlsx_bytes: bytes, sheet_name: str) -> list[dict[int, str]]:
    ns = {
        "a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
        "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
    }
    with ZipFile(io.BytesIO(xlsx_bytes)) as archive:
        shared_strings: list[str] = []
        if "xl/sharedStrings.xml" in archive.namelist():
            shared_root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
            for item in shared_root.findall("a:si", ns):
                shared_strings.append("".join(text.text or "" for text in item.iterfind(".//a:t", ns)))

        workbook = ET.fromstring(archive.read("xl/workbook.xml"))
        rels = ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
        rel_map = {rel.attrib["Id"]: rel.attrib["Target"] for rel in rels}

        target = None
        for sheet in workbook.find("a:sheets", ns):
            if sheet.attrib["name"] == sheet_name:
                rel_id = sheet.attrib["{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"]
                target = "xl/" + rel_map[rel_id]
                break

        if target is None:
            raise RuntimeError(f"Sheet {sheet_name} not found")

        root = ET.fromstring(archive.read(target))
        rows: list[dict[int, str]] = []
        for row in root.findall(".//a:sheetData/a:row", ns):
            items: dict[int, str] = {}
            for cell in row.findall("a:c", ns):
                ref = cell.attrib.get("r", "")
                column_index = excel_col_to_index(ref)
                cell_type = cell.attrib.get("t")
                value_node = cell.find("a:v", ns)
                value = "" if value_node is None else value_node.text or ""
                if cell_type == "s" and value:
                    value = shared_strings[int(value)]
                items[column_index] = value
            rows.append(items)
        return rows
def parse_steo_world_balance() -> dict[str, Any]:
    xlsx_bytes = fetch_bytes(EIA_STEO_XLSX_URL)
    dates_rows = read_sheet_rows(xlsx_bytes, "Dates")
    table_rows = read_sheet_rows(xlsx_bytes, "3atab")

    month_row = next(row for row in dates_rows if row.get(2) == "AAAA_DATEX or AAAA_YEAR")
    historical_row = next(row for row in dates_rows if row.get(2) == "Historical")
    last_historical_month = next(row for row in dates_rows if row.get(1) == "Last Historical Month--- ")
    date_labels = []
    historical_flags = []
    for column in sorted(k for k in month_row.keys() if k >= 3):
        date_labels.append(str(month_row[column]))
        historical_flags.append(str(historical_row.get(column, "")) == "1")

    target_codes = {
        "papr_world": "production",
        "patc_world": "consumption",
        "t3_stchange_world": "inventoryWithdrawal",
        "pasc_oecd_t3": "oecdCommercialInventories",
    }

    extracted: dict[str, list[float | None]] = {}
    for code, label in target_codes.items():
        row = next((item for item in table_rows if item.get(1) == code), None)
        if row is None:
            raise RuntimeError(f"Missing STEO row: {code}")
        values = []
        for position, column in enumerate(sorted(k for k in row.keys() if k >= 3)):
            if position >= len(date_labels):
                break
            raw = row[column]
            values.append(float(raw) if raw not in {"", None} else None)
        extracted[label] = values

    series = []
    for index, date_label in enumerate(date_labels):
        year = int(date_label[:4])
        month = int(date_label[4:])
        date = f"{year:04d}-{month:02d}"
        production = extracted["production"][index]
        consumption = extracted["consumption"][index]
        withdrawal = extracted["inventoryWithdrawal"][index]
        oecd_inventory = extracted["oecdCommercialInventories"][index]
        if production is None or consumption is None or withdrawal is None:
            continue
        series.append(
            {
                "date": date,
                "production": round(production, 3),
                "consumption": round(consumption, 3),
                "surplus": round(production - consumption, 3),
                "inventoryWithdrawal": round(withdrawal, 3),
                "oecdCommercialInventories": round(oecd_inventory, 1) if oecd_inventory is not None else None,
                "isHistorical": historical_flags[index],
            }
        )

    history_through = str(last_historical_month.get(4))
    return {
        "historyThrough": f"{history_through[:4]}-{history_through[4:]}",
        "series": series,
    }


def enrich_floating_storage(price_series: list[dict[str, Any]]) -> dict[str, Any]:
    raw_series = parse_user_floating_storage_series()
    series = []
    for item in raw_series:
        price = align_price(item["date"], price_series)
        series.append(
            {
                "date": item["date"],
                "value": item["value"],
                "wti": round(price, 2) if price is not None else None,
            }
        )

    latest = series[-1]
    previous = series[-2]
    peak = max(series, key=lambda item: item["value"])
    trough = min(series, key=lambda item: item["value"])
    average_8 = statistics.mean(point["value"] for point in series[-8:])
    return {
        "series": series,
        "latest": latest,
        "previous": previous,
        "peak": peak,
        "trough": trough,
        "eightWeekAverage": round(average_8, 2),
        "wowPct": round((latest["value"] / previous["value"] - 1) * 100, 2),
        "fromPeakPct": round((latest["value"] / peak["value"] - 1) * 100, 2),
    }


def build_us_inventory_block(
    commercial_series: list[dict[str, Any]],
    cushing_series: list[dict[str, Any]],
    refinery_util_series: list[dict[str, Any]],
) -> dict[str, Any]:
    recent_cutoff = "2024-01-01"
    commercial_recent = [item for item in commercial_series if item["date"] >= recent_cutoff]
    cushing_recent = [item for item in cushing_series if item["date"] >= recent_cutoff]
    refinery_recent = [item for item in refinery_util_series if item["date"] >= recent_cutoff]

    latest_commercial = commercial_recent[-1]
    previous_commercial = commercial_recent[-2]
    latest_cushing = cushing_recent[-1]
    latest_refinery = refinery_recent[-1]

    trailing_year = [item["value"] for item in commercial_recent[-52:]]
    commercial_avg = statistics.mean(trailing_year)
    cushing_change_8w = latest_cushing["value"] - cushing_recent[-9]["value"]

    joined = {}
    for point in commercial_recent:
        joined.setdefault(point["date"], {})["commercial"] = point["value"]
    for point in cushing_recent:
        joined.setdefault(point["date"], {})["cushing"] = point["value"]
    for point in refinery_recent:
        joined.setdefault(point["date"], {})["refineryUtil"] = point["value"]

    combo = []
    for date in sorted(joined.keys()):
        item = joined[date]
        combo.append(
            {
                "date": date,
                "commercial": round(item.get("commercial", 0), 0) if item.get("commercial") is not None else None,
                "cushing": round(item.get("cushing", 0), 0) if item.get("cushing") is not None else None,
                "refineryUtil": round(item.get("refineryUtil", 0), 1) if item.get("refineryUtil") is not None else None,
            }
        )

    return {
        "series": combo,
        "latestCommercial": latest_commercial,
        "latestCushing": latest_cushing,
        "latestRefineryUtil": latest_refinery,
        "commercialWow": round(latest_commercial["value"] - previous_commercial["value"], 0),
        "commercialVs52wAvg": round(latest_commercial["value"] - commercial_avg, 1),
        "cushingChange8w": round(cushing_change_8w, 1),
    }


def percentile_rank(values: list[float], current: float) -> float:
    if not values:
        return 0.0
    count = sum(1 for value in values if value <= current)
    return round(count / len(values) * 100, 1)


def build_streak(series: list[dict[str, Any]]) -> dict[str, Any]:
    if len(series) < 2:
        return {"direction": "持平", "weeks": 0}
    weeks = 0
    direction = "持平"
    for index in range(len(series) - 1, 0, -1):
        delta = series[index]["value"] - series[index - 1]["value"]
        if delta > 0:
            current_direction = "连增"
        elif delta < 0:
            current_direction = "连降"
        else:
            current_direction = "持平"
        if weeks == 0:
            direction = current_direction
        if current_direction != direction or current_direction == "持平":
            break
        weeks += 1
    return {"direction": direction, "weeks": weeks}


def build_cushing_percentile_block(cushing_series: list[dict[str, Any]]) -> dict[str, Any]:
    recent = [item for item in cushing_series if item["date"] >= "2021-01-01"]
    latest = recent[-1]
    window_values = [item["value"] for item in recent]
    percentile = percentile_rank(window_values, latest["value"])
    streak = build_streak(recent)
    return {
        "latest": latest,
        "percentile5y": percentile,
        "streak": streak,
        "bandLabel": "极低缓冲" if percentile <= 10 else "偏低缓冲" if percentile <= 30 else "中性缓冲" if percentile < 70 else "偏高缓冲",
    }


def latest_five_year_average(series: list[dict[str, Any]]) -> float:
    latest = series[-1]
    target_md = latest["date"][5:]
    comparable = [item["value"] for item in series[:-1] if item["date"][5:] == target_md and item["date"][:4] >= "2021"]
    if not comparable:
        comparable = [item["value"] for item in series[-52 * 5:]]
    return statistics.mean(comparable)


def build_products_block(
    gasoline_series: list[dict[str, Any]],
    distillate_series: list[dict[str, Any]],
    commercial_series: list[dict[str, Any]],
) -> dict[str, Any]:
    gasoline_recent = [item for item in gasoline_series if item["date"] >= "2024-01-01"]
    distillate_recent = [item for item in distillate_series if item["date"] >= "2024-01-01"]
    latest_gasoline = gasoline_recent[-1]
    latest_distillate = distillate_recent[-1]
    gasoline_5y = latest_five_year_average(gasoline_series)
    distillate_5y = latest_five_year_average(distillate_series)
    crude_latest = commercial_series[-1]
    crude_52w = statistics.mean(item["value"] for item in commercial_series[-52:])

    crude_bias = "原油偏紧" if crude_latest["value"] < crude_52w else "原油偏松"
    product_bias = "成品油偏强" if latest_gasoline["value"] < gasoline_5y and latest_distillate["value"] < distillate_5y else "成品油中性"

    return {
        "gasolineLatest": latest_gasoline,
        "distillateLatest": latest_distillate,
        "gasolineVs5y": round(latest_gasoline["value"] - gasoline_5y, 0),
        "distillateVs5y": round(latest_distillate["value"] - distillate_5y, 0),
        "crudeBias": crude_bias,
        "productBias": product_bias,
    }


def build_crack_spread_block() -> dict[str, Any]:
    wti_series = parse_fred_series(FRED_WTI_URL, "DCOILWTICO")
    gasoline_series = parse_fred_series(FRED_GASOLINE_SPOT_URL, "DGASNYH")
    diesel_series = parse_fred_series(FRED_DIESEL_SPOT_URL, "DDFUELNYH")

    cutoff = "2026-01-15"
    combined = []
    for wti_point in wti_series:
        if wti_point["date"] < cutoff:
            continue
        gasoline_point = nearest_on_or_before(wti_point["date"], gasoline_series)
        diesel_point = nearest_on_or_before(wti_point["date"], diesel_series)
        if gasoline_point is None or diesel_point is None:
            continue
        gasoline_crack = gasoline_point["value"] * 42 - wti_point["value"]
        diesel_crack = diesel_point["value"] * 42 - wti_point["value"]
        combined.append(
            {
                "date": wti_point["date"],
                "gasoline": round(gasoline_crack, 2),
                "diesel": round(diesel_crack, 2),
            }
        )

    recent = combined[-30:]
    latest = recent[-1]
    prior = recent[-6] if len(recent) >= 6 else recent[0]
    avg_level = statistics.mean((item["gasoline"] + item["diesel"]) / 2 for item in recent)
    slope = ((latest["gasoline"] + latest["diesel"]) / 2) - ((prior["gasoline"] + prior["diesel"]) / 2)
    regime = "炼厂利润扩张" if slope > 0 and avg_level > 20 else "炼厂利润收缩" if slope < 0 else "炼厂利润高位震荡"
    return {
        "series": recent,
        "latest": latest,
        "avg30d": round(avg_level, 2),
        "regime": regime,
    }


def classify_spread(value: float) -> str:
    if value <= -1.5:
        return "深贴水"
    if value < -0.4:
        return "弱贴水"
    if value <= 0.4:
        return "平坦"
    if value <= 1.5:
        return "弱升水"
    return "深升水"


def build_wti_spread_block() -> dict[str, Any]:
    series = []
    for item in MANUAL_WTI_SPREAD_SERIES:
        spread = round(item["front"] - item["second"], 2)
        series.append({**item, "spread": spread})
    latest = series[-1]
    previous = series[-2]
    return {
        "series": series,
        "latest": latest,
        "wow": round(latest["spread"] - previous["spread"], 2),
        "status": classify_spread(latest["spread"]),
    }


def build_opec_block() -> dict[str, Any]:
    countries = []
    for item in MANUAL_OPEC_COUNTRY_ROWS:
        deviation = round(item["actual"] - item["target"], 2)
        execution = round(item["target"] / item["actual"] * 100, 1) if item["actual"] else None
        countries.append({**item, "deviation": deviation, "execution": execution})

    trend = []
    for item in MANUAL_OPEC_TREND:
        execution = round(item["target"] / item["actual"] * 100, 1) if item["actual"] else None
        trend.append({**item, "execution": execution, "deviation": round(item["actual"] - item["target"], 2)})

    latest = trend[-1]
    return {
        "countries": countries,
        "trend": trend,
        "latest": latest,
        "latestMonth": latest["date"],
    }


def build_summary(payload: dict[str, Any]) -> dict[str, Any]:
    floating = payload["floatingStorage"]
    global_balance = payload["globalBalance"]
    us = payload["usSupplyDemand"]
    products = payload["products"]
    crack = payload["crackSpreads"]
    wti_spread = payload["wtiSpread"]
    cushing_band = payload["cushingPercentile"]
    opec = payload["opecPlus"]

    latest_balance = next(item for item in reversed(global_balance["series"]) if item["isHistorical"])
    next_three = [item for item in global_balance["series"] if item["date"] > global_balance["historyThrough"]][:3]
    avg_next_draw = statistics.mean(item["inventoryWithdrawal"] for item in next_three) if next_three else 0.0

    floating_text = "回落" if floating["wowPct"] < 0 else "回升"
    balance_text = "去库" if latest_balance["inventoryWithdrawal"] > 0 else "累库"
    inventory_text = "高于" if us["commercialVs52wAvg"] > 0 else "低于"
    product_text = "同步偏低" if products["gasolineVs5y"] < 0 and products["distillateVs5y"] < 0 else "未形成同步偏强"

    headline = (
        f"浮仓近周{floating_text}，全球平衡表仍显示{balance_text}；"
        f"WTI 月差最新处于“{wti_spread['status']}”区间，美国商业库存目前{inventory_text}52周均值，"
        f"汽油与馏分油库存{product_text}，裂解利润处于“{crack['regime']}”状态。"
    )

    insights = [
        (
            f"全球浮仓最新报 {floating['latest']['value']:.2f} 百万桶，较前一周变化 {floating['wowPct']:.2f}% ，"
            f"较 2025-12-26 的阶段高点 {floating['peak']['value']:.2f} 百万桶回落 {abs(floating['fromPeakPct']):.2f}% 。"
        ),
        (
            f"EIA 短期展望显示 {latest_balance['date']} 全球原油与其他液体燃料库存隐含净去库 "
            f"{latest_balance['inventoryWithdrawal']:.2f} 百万桶/日；后续三个月平均仍为 {avg_next_draw:.2f} 百万桶/日。"
        ),
        (
            f"美国商业原油库存最新 {us['latestCommercial']['value'] / 1000:.1f} 百万桶，"
            f"较 52 周均值 {'高出' if us['commercialVs52wAvg'] > 0 else '低于'} "
            f"{abs(us['commercialVs52wAvg']) / 1000:.1f} 百万桶。"
        ),
        (
            f"库欣库存最新 {us['latestCushing']['value'] / 1000:.2f} 百万桶，"
            f"8 周累计变动 {us['cushingChange8w'] / 1000:.2f} 百万桶，反映近端交割缓冲有所恢复。"
        ),
        (
            f"美国炼厂开工率最新 {us['latestRefineryUtil']['value']:.1f}% ，"
            f"仍处偏高运行区间，意味着原油近端需求拉动尚未明显转弱。"
        ),
        (
            f"WTI 近月-次月月差最新可比收盘为 {wti_spread['latest']['spread']:.2f} 美元/桶，"
            f"状态落在“{wti_spread['status']}”；库欣库存位于近 5 年的 {cushing_band['percentile5y']:.1f}% 分位。"
        ),
        (
            f"美国汽油库存较 5 年同期 {'低' if products['gasolineVs5y'] < 0 else '高'} "
            f"{abs(products['gasolineVs5y']) / 1000:.1f} 百万桶，馏分油库存较 5 年同期 "
            f"{'低' if products['distillateVs5y'] < 0 else '高'} {abs(products['distillateVs5y']) / 1000:.1f} 百万桶。"
        ),
        (
            f"汽油裂解价差最新 {crack['latest']['gasoline']:.2f} 美元/桶，柴油裂解价差 {crack['latest']['diesel']:.2f} 美元/桶；"
            f"OPEC+ 在 {opec['latestMonth']} 的执行率约为 {opec['latest']['execution']:.1f}%。"
        ),
    ]

    return {
        "headline": headline,
        "latestBalance": latest_balance,
        "avgNextThreeMonthDraw": round(avg_next_draw, 2),
        "insights": insights,
    }


def build_payload() -> dict[str, Any]:
    price_series = parse_fred_wti_series()
    commercial_series = parse_eia_history_series(EIA_COMMERCIAL_STOCKS_URL)
    cushing_series = parse_eia_history_series(EIA_CUSHING_STOCKS_URL)
    refinery_util_series = parse_eia_history_series(EIA_REFINERY_UTIL_URL)
    gasoline_series = parse_eia_history_series(EIA_GASOLINE_STOCKS_URL)
    distillate_series = parse_eia_history_series(EIA_DISTILLATE_STOCKS_URL)
    floating_storage = enrich_floating_storage(price_series)
    global_balance = parse_steo_world_balance()
    us_supply = build_us_inventory_block(commercial_series, cushing_series, refinery_util_series)
    cushing_percentile = build_cushing_percentile_block(cushing_series)
    products = build_products_block(gasoline_series, distillate_series, commercial_series)
    crack_spreads = build_crack_spread_block()
    wti_spread = build_wti_spread_block()
    opec_plus = build_opec_block()

    payload: dict[str, Any] = {
        "generatedAt": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "meta": {
            "title": "原油供需全景跟踪",
            "subtitle": "浮仓、月差、裂解、库存与 OPEC+ 执行率的联动监测",
        },
        "floatingStorage": floating_storage,
        "globalBalance": global_balance,
        "usSupplyDemand": us_supply,
        "cushingPercentile": cushing_percentile,
        "products": products,
        "crackSpreads": crack_spreads,
        "wtiSpread": wti_spread,
        "opecPlus": opec_plus,
    }
    payload["summary"] = build_summary(payload)
    return payload


def main() -> None:
    payload = build_payload()
    OUTPUT_DATA_PATH.write_text(
        "window.CRUDE_OIL_SUPPLY_DASHBOARD = " + json.dumps(payload, ensure_ascii=False, indent=2) + ";\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "output": str(OUTPUT_DATA_PATH),
                "generatedAt": payload["generatedAt"],
                "floatingLatest": payload["floatingStorage"]["latest"],
                "globalBalanceHistoryThrough": payload["globalBalance"]["historyThrough"],
                "commercialStocksLatest": payload["usSupplyDemand"]["latestCommercial"],
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
