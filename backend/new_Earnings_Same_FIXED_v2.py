#!/usr/bin/env python3
"""
Department Summary PDF -> Excel (EARNINGS only) - Batch Runner + rename Total + sort + totals + MTD/QTD/YTD toggle

Adds:
1) Sort rows by Date (within Department + Check Date)
2) Optional include/exclude rows where Date starts with MTD/QTD/YTD
3) Adds column: "Actual Total Earnings" = sum of ALL earnings columns EXCEPT the Total column
4) Adds one final row at bottom with SUM of each numeric column (including "Actual Total Earnings")

Dependencies:
  pip install pdfplumber pandas openpyxl numpy
"""

import os
import re
import math
import numpy as np
import pandas as pd
import pdfplumber
from datetime import datetime
from typing import Optional


# =========================
# CONFIG (hardcode here)
# =========================
TABLE_NAME = "EARNINGS"   # used for renaming "Total" -> "EARNINGS Total"

PDF_PATHS = [
    r"PG January 2026 Department Summary.pdf",


]

# If None -> output next to each PDF
OUTPUT_DIR = None  # e.g. r"C:\Users\vkumawat\Downloads\Outputs"

# =========================
# SUMMARY ROW CONTROL
# =========================
INCLUDE_MTD = False
INCLUDE_QTD = False
INCLUDE_YTD = False


# =========================
# Regex / helpers
# =========================
DATE_RE = re.compile(r"^\d{2}/\d{2}/\d{2}$")
NUM_RE  = re.compile(r"^-?\d{1,3}(?:,\d{3})*(?:\.\d+)?$")


def rename_total_column(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """Rename only the generic 'Total' header to '<table_name> Total'."""
    key_cols = {"Department", "Check Date"}
    new_cols = []
    for c in df.columns:
        if c in key_cols:
            new_cols.append(c)
            continue
        if str(c).strip().upper() == "TOTAL":
            new_cols.append(f"{table_name} Total")
        else:
            new_cols.append(c)
    out = df.copy()
    out.columns = new_cols
    return out


def filter_summary_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Include / Exclude MTD, QTD, YTD rows based on config flags."""
    out = df.copy()
    if out.empty:
        return out
    d = out["Check Date"].astype(str).str.upper()
    mask = pd.Series(True, index=out.index)
    if not INCLUDE_MTD:
        mask &= ~d.str.startswith("MTD", na=False)
    if not INCLUDE_QTD:
        mask &= ~d.str.startswith("QTD", na=False)
    if not INCLUDE_YTD:
        mask &= ~d.str.startswith("YTD", na=False)
    out = out.loc[mask].reset_index(drop=True)
    return out


def _date_sort_key(date_label: str):
    """
    Sort key for the 'Date' column:
    - real dates (MM/DD/YY) come first, sorted by actual date
    - then MTD..., then QTD..., then YTD...
    - everything else goes last
    """
    s = (date_label or "").strip().upper()

    if DATE_RE.match(s):
        try:
            dt = datetime.strptime(s, "%m/%d/%y")
            return (0, dt)
        except Exception:
            return (0, datetime.max)

    if s.startswith("MTD"):
        return (1, s)
    if s.startswith("QTD"):
        return (2, s)
    if s.startswith("YTD"):
        return (3, s)

    return (4, s)


def sort_by_date(df: pd.DataFrame) -> pd.DataFrame:
    """Sort within Department + Check Date by Date using _date_sort_key."""
    out = df.copy()
    if out.empty:
        return out
    d = out["Check Date"].astype(str).str.upper()
    mask = pd.Series(True, index=out.index)
    if not INCLUDE_MTD:
        mask &= ~d.str.startswith("MTD", na=False)
    if not INCLUDE_QTD:
        mask &= ~d.str.startswith("QTD", na=False)
    if not INCLUDE_YTD:
        mask &= ~d.str.startswith("YTD", na=False)
    out = out.loc[mask].reset_index(drop=True)
    return out


def add_actual_total_earnings(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """
    Adds "Actual Total Earnings" = row-wise sum across ALL numeric earnings columns
    EXCEPT the 'Total' column (including renamed '<table_name> Total').
    """
    key_cols = {"Department", "Check Date"}

    total_names_upper = {
        "TOTAL",
        f"{table_name.strip().upper()} TOTAL",
    }

    def is_total_col(c: str) -> bool:
        return str(c).strip().upper() in total_names_upper

    earnings_cols = [c for c in df.columns if c not in key_cols and not is_total_col(c)]

    out = df.copy()
    for c in earnings_cols:
        out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0)

    out["Actual Total Earnings"] = out[earnings_cols].sum(axis=1)
    return out


def add_bottom_sum_row(df: pd.DataFrame) -> pd.DataFrame:
    """Add one final row at bottom that sums every numeric column."""
    key_cols = ["Department", "Check Date"]
    out = df.copy()

    sum_row = {c: "" for c in out.columns}
    sum_row["Department"] = "TOTAL"
    sum_row["Check Date"] = ""


    for c in out.columns:
        if c in key_cols:
            continue
        sum_row[c] = pd.to_numeric(out[c], errors="coerce").fillna(0).sum()

    out = pd.concat([out, pd.DataFrame([sum_row])], ignore_index=True)
    return out


# -------------------------
# Extraction logic
# -------------------------
def group_words_by_row(words, y_tol=3):
    words = sorted(words, key=lambda w: (w["top"], w["x0"]))
    rows = []
    for w in words:
        if not rows or abs(w["top"] - rows[-1]["top"]) > y_tol:
            rows.append({"top": w["top"], "words": [w]})
        else:
            rows[-1]["words"].append(w)
    for r in rows:
        r["words"] = sorted(r["words"], key=lambda w: w["x0"])
    return rows


def cluster_x_positions(xs, gap=12):
    xs = sorted(xs)
    clusters = []
    for x in xs:
        if not clusters or x - clusters[-1][-1] > gap:
            clusters.append([x])
        else:
            clusters[-1].append(x)
    return [sum(c) / len(c) for c in clusters]


def get_check_date(page):
    t = page.extract_text(layout=True) or ""
    m = re.search(r"Check\s*Date\s+(\d{2}/\d{2}/\d{2})", t, flags=re.IGNORECASE)
    if not m:
        m = re.search(r"CheckDate\s+(\d{2}/\d{2}/\d{2})", t, flags=re.IGNORECASE)
    return m.group(1) if m else None


def to_number(x):
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return np.nan
    s = str(x).strip()
    if not s:
        return np.nan
    s2 = s.replace(",", "")
    try:
        return float(s2)
    except Exception:
        return x


def is_numeric_series(s: pd.Series) -> bool:
    non_empty = s.replace("", np.nan).dropna()
    if len(non_empty) == 0:
        return True
    numeric_count = pd.to_numeric(non_empty, errors="coerce").notna().sum()
    return numeric_count / len(non_empty) >= 0.8


def merge_duplicate_rows(df: pd.DataFrame) -> pd.DataFrame:
    key_cols = ["Department", "Check Date"]
    val_cols = [c for c in df.columns if c not in key_cols]

    agg = {}
    for c in val_cols:
        if is_numeric_series(df[c]):
            def _sum_numeric(s):
                x = pd.to_numeric(s, errors="coerce")
                if x.notna().any():
                    return x.fillna(0).sum()
                return np.nan
            agg[c] = _sum_numeric
        else:
            def _first_text(s):
                for v in s:
                    if pd.isna(v):
                        continue
                    t = str(v).strip()
                    if t and t.lower() != "nan":
                        return t
                return ""
            agg[c] = _first_text

    merged = df.groupby(key_cols, as_index=False).agg(agg)
    merged = merged[df.columns]
    return merged


def _overlap_ratio(a0, a1, b0, b1):
    inter = max(0, min(a1, b1) - max(a0, b0))
    union = max(a1, b1) - min(a0, b0)
    return inter / union if union > 0 else 0


def merge_header_clusters(centers, words_by_cluster, overlap_thresh=0.55):
    def once(centers, words_by_cluster):
        merged_centers, merged_words = [], []
        i = 0

        def bbox(ws):
            return (min(w["x0"] for w in ws), max(w["x1"] for w in ws))

        def top_min(ws):
            return min(w["top"] for w in ws)

        while i < len(centers):
            ws = words_by_cluster[i]
            a0, a1 = bbox(ws)
            at = top_min(ws)

            if i < len(centers) - 1:
                ws2 = words_by_cluster[i + 1]
                b0, b1 = bbox(ws2)
                bt = top_min(ws2)

                if _overlap_ratio(a0, a1, b0, b1) >= overlap_thresh and abs(at - bt) > 2:
                    merged_centers.append((centers[i] + centers[i + 1]) / 2)
                    merged_words.append(ws + ws2)
                    i += 2
                    continue

            merged_centers.append(centers[i])
            merged_words.append(ws)
            i += 1

        return merged_centers, merged_words

    while True:
        c2, w2 = once(centers, words_by_cluster)
        if len(c2) == len(centers):
            return centers, words_by_cluster
        centers, words_by_cluster = c2, w2


def canonicalize_header(name: str) -> str:
    if not name:
        return name
    n = re.sub(r"\s+", " ", str(name)).strip()
    n = re.sub(r"_+$", "", n).strip()

    up = re.sub(r"\s+", " ", n.upper()).strip()

    CANON = {
        "BONUS-ATTENDANCE": "Bonus-Attendance",
        "BONUS-DISCRETIONARY": "Bonus-Discretionary",
        "BONUS-TRAINER": "Bonus-Trainer",
        "TRAINING OT": "Training OT",
        "TRAINING OT HOURS": "Training OT Hours",
        "NON-FLSA OVERTIME": "Non-FLSA Overtime",
        "COVID CHILDCARE": "Covid Childcare",
        "COVID SICK-EE": "Covid Sick-EE",
        "COVID SICK-FAM": "Covid Sick-Fam",
        "INSURANCE REFUND": "Insurance Refund",
        "HEALTH INS REBATE": "Health Ins Rebate",
    }

    if up in CANON:
        return CANON[up]
    return n


def fix_wrapped_header_fragments(colnames, col_lefts, centers, max_gap=38):
    if not colnames:
        return colnames, col_lefts, centers

    out_names, out_lefts, out_centers = [], [], []
    i = 0

    CONT_FRAGS = {"CE", "NARY", "ARY", "TION", "IONS", "AL", "ED", "EE", "ER", "HRS", "D"}
    TRUNC_ENDS = ("ATTENDAN", "DISCRETIO", "DIFFERENTIA", "REIMBURSEM", "ALLOCATE", "INSURAN")

    while i < len(colnames):
        name = (colnames[i] or "").strip()
        up = re.sub(r"\s+", " ", name.upper()).strip()

        if out_names:
            prev = out_names[-1]
            prev_up = re.sub(r"\s+", " ", prev.upper()).strip()

            gap_ok = True
            if i < len(centers) and len(out_centers) > 0:
                gap_ok = abs(centers[i] - out_centers[-1]) <= max_gap

            is_short_fragment = (len(up) <= 4 and re.fullmatch(r"[A-Z0-9-]+", up or "") is not None)
            looks_like_cont = (up in CONT_FRAGS or prev_up.endswith(TRUNC_ENDS) or prev_up.endswith("-"))

            if gap_ok and is_short_fragment and looks_like_cont:
                out_names[-1] = (prev + up).strip()
                i += 1
                continue

            if gap_ok and prev_up.endswith(TRUNC_ENDS) and re.fullmatch(r"[A-Z]+", up or ""):
                out_names[-1] = (prev + up).strip()
                i += 1
                continue

        out_names.append(name)
        out_lefts.append(col_lefts[i] if i < len(col_lefts) else None)
        out_centers.append(centers[i] if i < len(centers) else None)
        i += 1

    out_names = [canonicalize_header(n) for n in out_names]
    return out_names, out_lefts, out_centers


def split_wide_header_clusters(centers, words_by_cluster, x_gap=28):
    new_centers = []
    new_words = []
    for c, ws in zip(centers, words_by_cluster):
        if not ws:
            continue
        ws_sorted = sorted(ws, key=lambda w: w["x0"])

        groups = [[ws_sorted[0]]]
        for w in ws_sorted[1:]:
            prev = groups[-1][-1]
            if (w["x0"] - prev["x1"]) > x_gap:
                groups.append([w])
            else:
                groups[-1].append(w)

        if len(groups) > 1:
            for g in groups:
                new_words.append(g)
                new_centers.append(sum((w["x0"] + w["x1"]) / 2 for w in g) / len(g))
        else:
            new_words.append(ws)
            new_centers.append(c)

    return new_centers, new_words


def split_known_compound_headers(centers, words_by_cluster):
    new_centers = []
    new_words = []

    for c, ws in zip(centers, words_by_cluster):
        if not ws:
            continue

        text = " ".join(w["text"] for w in sorted(ws, key=lambda w: (w["top"], w["x0"]))).strip()
        compact = re.sub(r"[^A-Z0-9]+", "", text.upper())

        if "VACATIONVACATIONPAYOUT" in compact:
            if len(ws) == 1:
                w0 = ws[0]
                midx = (w0["x0"] + w0["x1"]) / 2

                left_w = dict(w0)
                left_w["text"] = "VACATION"
                left_w["x1"] = midx

                right_w = dict(w0)
                right_w["text"] = "VACATION PAYOUT"
                right_w["x0"] = midx

                new_words.append([left_w])
                new_centers.append((left_w["x0"] + left_w["x1"]) / 2)

                new_words.append([right_w])
                new_centers.append((right_w["x0"] + right_w["x1"]) / 2)
                continue

            mids = [((w["x0"] + w["x1"]) / 2) for w in ws]
            x_mid = (min(mids) + max(mids)) / 2
            left = [w for w in ws if ((w["x0"] + w["x1"]) / 2) < x_mid]
            right = [w for w in ws if ((w["x0"] + w["x1"]) / 2) >= x_mid]

            if left and right:
                new_words.append(left)
                new_centers.append(sum(((w["x0"] + w["x1"]) / 2) for w in left) / len(left))
                new_words.append(right)
                new_centers.append(sum(((w["x0"] + w["x1"]) / 2) for w in right) / len(right))
                continue

        new_words.append(ws)
        new_centers.append(c)

    return new_centers, new_words


def should_skip_dept(dept_text: str) -> bool:
    compact = re.sub(r"\s+", "", dept_text.upper())
    return compact.startswith("****ALLORGANIZATIONALUNITS")

def normalize_department(dept_raw: str, prev_dept: Optional[str]) -> Optional[str]:
    """
    PDFs sometimes print continuation department headers like:
      '**** 100DRIVERS(cont.)' or '**** CONT. 100DRIVERS'
    Those should NOT start a new department group; they should keep using the
    previous department so we end up with ONE row per (Department + Check Date + Date).
    """
    if not dept_raw:
        return prev_dept

    s = " ".join(str(dept_raw).split())
    up = s.upper()

    # If this is a continuation marker and we already have a previous department, keep it.
    if prev_dept and ("(CONT" in up or " CONT." in up or " CONT " in up or up.startswith("****CONT")):
        return prev_dept

    # Otherwise, strip any trailing "(cont.)" / "cont." from the department name.
    s = re.sub(r"\(\s*cont\.?\s*\)\s*", " ", s, flags=re.I)
    s = re.sub(r"\bcont\.?\b\s*$", "", s, flags=re.I).strip()

    return s or prev_dept



def clean_label_tokens(tokens):
    while tokens and NUM_RE.fullmatch(tokens[-1]):
        tokens = tokens[:-1]
    return " ".join(tokens).strip()


def is_section_heading(row_words) -> bool:
    if not row_words:
        return False
    txt = " ".join(w["text"] for w in row_words).strip().upper()
    if "EARNINGS" in txt:
        return True
    if "REIMBURSEMENTS" in txt or "OTHER ITEMS" in txt or "EMPLOYEE" in txt or "EMPLOYER" in txt:
        return True
    return False


def find_earnings_block_starts(rows):
    starts = []
    for i, r in enumerate(rows):
        txt = " ".join(w["text"] for w in r["words"]).upper()
        if "EARNINGS" in txt:
            starts.append(i)
    return starts


def parse_earnings(pdf_path: str) -> pd.DataFrame:
    records = []
    all_columns_in_order = []

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            words = page.extract_words()
            rows = group_words_by_row(words, y_tol=3)
            check_date = get_check_date(page)

            starts = find_earnings_block_starts(rows)
            if not starts:
                continue

            for start in starts:
                dept_row_idx = None
                for j in range(start + 1, min(len(rows), start + 70)):
                    if rows[j]["words"] and rows[j]["words"][0]["text"] == "****":
                        dept_row_idx = j
                        break
                    if j > start + 1 and is_section_heading(rows[j]["words"]) and "EARNINGS" not in " ".join(
                        w["text"] for w in rows[j]["words"]
                    ).upper():
                        dept_row_idx = None
                        break

                if dept_row_idx is None:
                    continue

                header_rows = rows[start + 1: dept_row_idx]

                header_words = []
                for rr in header_rows:
                    for w in rr["words"]:
                        if w["text"].upper() in ("Check Date", "DEPARTMENT", "SUMMARY", "EARNINGS"):
                            continue
                        header_words.append(w)

                xs = [(w["x0"] + w["x1"]) / 2 for w in header_words if (w["x0"] + w["x1"]) / 2 > 40]
                if not xs:
                    continue

                centers = sorted(cluster_x_positions(xs, gap=18))

                buckets = {c: [] for c in centers}
                for w in header_words:
                    mid = (w["x0"] + w["x1"]) / 2
                    if mid <= 40:
                        continue
                    c = min(centers, key=lambda cc: abs(cc - mid))
                    buckets[c].append(w)

                words_by_cluster = [sorted(buckets[c], key=lambda w: (w["top"], w["x0"])) for c in centers]

                centers2, words2 = merge_header_clusters(centers, words_by_cluster, overlap_thresh=0.55)
                centers2, words2 = split_wide_header_clusters(centers2, words2, x_gap=28)
                centers2, words2 = split_known_compound_headers(centers2, words2)

                colnames = []
                col_lefts = []
                centers_final = []

                for c, ws in zip(centers2, words2):
                    ws_sorted = sorted(ws, key=lambda w: (w["top"], w["x0"]))
                    name = " ".join([w["text"] for w in ws_sorted]).strip()

                    up = re.sub(r"\s+", " ", name.upper()).strip()
                    if "HOLIDAY" in up and "INSURANCE" in up and "REFUND" in up:
                        holiday_ws = [w for w in ws_sorted if "HOLIDAY" in w["text"].upper()]
                        insref_ws = [w for w in ws_sorted if w not in holiday_ws]
                        if holiday_ws and insref_ws:
                            colnames.append("Holiday")
                            col_lefts.append(min(w["x0"] for w in holiday_ws))
                            centers_final.append(sum((w["x0"] + w["x1"]) / 2 for w in holiday_ws) / len(holiday_ws))

                            colnames.append("Insurance Refund")
                            col_lefts.append(min(w["x0"] for w in insref_ws))
                            centers_final.append(sum((w["x0"] + w["x1"]) / 2 for w in insref_ws) / len(insref_ws))
                            continue

                    if name:
                        colnames.append(name)
                        col_lefts.append(min(w["x0"] for w in ws_sorted))
                        centers_final.append(c)

                if not colnames:
                    continue

                colnames, col_lefts, centers_final = fix_wrapped_header_fragments(colnames, col_lefts, centers_final)

                first_col_left = min([x for x in col_lefts if x is not None])

                for name in colnames:
                    if name not in all_columns_in_order:
                        all_columns_in_order.append(name)

                current_dept = None
                for r in rows[dept_row_idx:]:
                    if not r["words"]:
                        continue

                    if is_section_heading(r["words"]) and r is not rows[dept_row_idx]:
                        break

                    if r["words"][0]["text"] == "****":
                        dept_raw = " ".join([w["text"] for w in r["words"]])
                        current_dept = normalize_department(dept_raw, current_dept)
                        continue

                    if current_dept is None or should_skip_dept(current_dept):
                        continue

                    left_words = [w for w in r["words"] if w["x0"] < first_col_left - 6]
                    if not left_words:
                        continue

                    label = clean_label_tokens([w["text"] for w in left_words])

                    # Skip MTD/QTD/YTD rows entirely — only keep actual
                    # payroll-date rows. This prevents page-break continuations
                    # from duplicating data when summary rows appear on the next page.
                    if label.upper().startswith(("MTD", "QTD", "YTD")):
                        continue

                    if not DATE_RE.match(label):
                        # Use page-level date as fallback if row label is not a date
                        if check_date:
                            label = check_date
                        else:
                            continue

                    row_map = {cn: "" for cn in colnames}
                    for w in [w for w in r["words"] if w["x0"] >= first_col_left - 6]:
                        mid = (w["x0"] + w["x1"]) / 2
                        c = min(centers_final, key=lambda cc: abs(cc - mid))
                        idx = centers_final.index(c)
                        cn = colnames[idx]
                        row_map[cn] = (row_map[cn] + " " + w["text"]).strip()

                    rec = {"Department": current_dept, "Check Date": label}
                    rec.update(row_map)
                    records.append(rec)

    df = pd.DataFrame(records)

    base_cols = ["Department", "Check Date"]
    for cn in all_columns_in_order:
        if cn not in df.columns:
            df[cn] = np.nan

    df = df[base_cols + all_columns_in_order]

    for cn in all_columns_in_order:
        df[cn] = df[cn].apply(to_number)

    return df


def make_output_path(pdf_path: str) -> str:
    base = os.path.splitext(os.path.basename(pdf_path))[0]
    out_name = f"{base}_{TABLE_NAME.title()}_Output.xlsx"
    if OUTPUT_DIR:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        return os.path.join(OUTPUT_DIR, out_name)
    return os.path.join(os.path.dirname(pdf_path) or ".", out_name)


def process_one(pdf_path: str) -> tuple[bool, str]:
    if not os.path.exists(pdf_path):
        return False, f"Missing PDF: {pdf_path}"

    out_xlsx = make_output_path(pdf_path)
    try:
        df = parse_earnings(pdf_path)
        if df.empty:
            df = pd.DataFrame(columns=["Department", "Check Date"])

        df = merge_duplicate_rows(df)

        # Rename "Total" -> "<TABLE_NAME> Total"
        df = rename_total_column(df, TABLE_NAME)

        # Optional filter (MTD/QTD/YTD)
        df = filter_summary_rows(df)

        # Convert earnings to numeric + blanks->0 (keep identifiers untouched)
        key_cols = ["Department", "Check Date"]
        for col in df.columns:
            if col not in key_cols:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        # Sort by Date (within Department + Check Date)
        df = sort_by_date(df)

        # Add Actual Total Earnings (excluding Total column)
        df = add_actual_total_earnings(df, TABLE_NAME)

        # Add bottom SUM row (must be after sorting so it stays last)
        df = add_bottom_sum_row(df)

        df.to_excel(out_xlsx, index=False, sheet_name="Earnings")
        return True, f"Saved: {out_xlsx} | Rows={len(df)} Cols={len(df.columns)}"
    except Exception as e:
        return False, f"ERROR for {pdf_path}: {type(e).__name__}: {e}"


def main():
    print("Batch starting...")
    ok, bad = 0, 0
    for p in PDF_PATHS:
        success, msg = process_one(p)
        print(msg)
        if success:
            ok += 1
        else:
            bad += 1
    print(f"Done. Success={ok} Failed={bad}")


if __name__ == "__main__":
    main()