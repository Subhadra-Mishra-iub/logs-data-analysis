#!/usr/bin/env python3
"""
Parse daily broadcast log PDFs into structured CSV.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)
logging.getLogger("pdfminer").setLevel(logging.ERROR)

SCHED_TIME_PATTERN = re.compile(r"^(\d{1,2}:\d{2}:\d{2}:\d{2})\b")
TIMEISH_PATTERN = re.compile(r"^\d{1,2}:\d{2}(?::\d{2}){0,2}$")
TYPE_PATTERN = re.compile(r"^(PG|PR|FI|ID|GX|AD|TX|PS|[A-Z]{2,4})$")
HEADER_SKIP_PATTERNS = [
    r"^DAILY TV PROGRAM LOG",
    r"^PAGE:\s*\d+\s+OF:",
    r"^DAY & DATE:",
    r"^SCHED\.\s+ACTUAL",
    r"^TIME\s+TIME\s+LENGTH",
    r"^CSA$",
    r"^OPERATOR[_\s]",
    r"^PRINTED:",
    r"^\(cid:\d+\)$",
]
HEADER_SKIP_REGEX = re.compile("|".join(f"(?:{p})" for p in HEADER_SKIP_PATTERNS), re.IGNORECASE)


@dataclass
class ParsedRow:
    log_date: str
    sched_time: str
    duration_raw: str
    video_source: str
    type_code: str
    title_raw: str
    full_row_text: str
    actual_time: str = ""
    parse_status: str = "ok"
    parse_error: str = ""

    def as_dict(self) -> dict:
        out = self.__dict__.copy()
        out["duration_seconds"] = duration_to_seconds(self.duration_raw)
        out["hour_of_day"] = hour_from_sched_time(self.sched_time)
        out["content_category"] = classify_content_category(self.type_code, self.title_raw)
        return out


def extract_text_pdfplumber(pdf_path: Path) -> str:
    import pdfplumber

    text_parts: list[str] = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text() or ""
            if page_text.strip():
                text_parts.append(page_text)
    return "\n".join(text_parts)


def extract_text_pymupdf(pdf_path: Path) -> str:
    import fitz

    text_parts: list[str] = []
    with fitz.open(pdf_path) as doc:
        for page in doc:
            page_text = page.get_text() or ""
            if page_text.strip():
                text_parts.append(page_text)
    return "\n".join(text_parts)


def extract_pdf_text(pdf_path: Path) -> str:
    try:
        return extract_text_pdfplumber(pdf_path)
    except Exception as exc:
        logger.warning("pdfplumber failed for %s: %s. Trying pymupdf fallback.", pdf_path.name, exc)

    try:
        return extract_text_pymupdf(pdf_path)
    except Exception as exc:
        raise RuntimeError(f"Both extractors failed for {pdf_path.name}: {exc}") from exc


def should_skip_line(line: str) -> bool:
    line = line.strip()
    if not line:
        return True
    if line.isdigit() and len(line) <= 4:
        return True
    return bool(HEADER_SKIP_REGEX.match(line))


def filename_to_log_date(pdf_path: Path) -> str:
    match = re.fullmatch(r"(\d{2})(\d{2})(\d{2})", pdf_path.stem)
    if not match:
        return ""
    month, day, year_short = match.groups()
    year = 2000 + int(year_short)
    return f"{year:04d}-{month}-{day}"


def duration_to_seconds(raw: str) -> float:
    raw = (raw or "").strip()
    if not raw:
        return 0.0

    parts = raw.split(":")
    try:
        ints = [int(x) for x in parts]
    except ValueError:
        return 0.0

    # HH:MM:SS:FF
    if len(ints) == 4:
        h, m, s, f = ints
        return h * 3600 + m * 60 + s + f / 30.0

    # Could be MM:SS:FF or HH:MM:SS
    if len(ints) == 3:
        a, b, c = ints
        # broadcast lengths in source are mostly MM:SS:FF
        if a < 60 and b < 60 and c < 60:
            return a * 60 + b + c / 30.0
        return a * 3600 + b * 60 + c

    if len(ints) == 2:
        m, s = ints
        return m * 60 + s

    if len(ints) == 1:
        return float(ints[0])

    return 0.0


def hour_from_sched_time(sched_time: str) -> int:
    try:
        return int((sched_time or "").split(":")[0]) % 24
    except Exception:
        return -1


def classify_content_category(type_code: str, title_raw: str) -> str:
    code = (type_code or "").upper().strip()
    title = (title_raw or "").upper()

    if code == "PG":
        return "Program"
    if code == "PR":
        return "Promo"
    if code == "FI":
        return "Filler"
    if code == "ID":
        return "ID"
    if code == "GX":
        return "Graphics"
    if code == "AD":
        return "Audio"
    if code == "TX":
        return "Text"
    if code == "PS":
        return "PublicService"

    if "PROMO" in title:
        return "Promo"
    if "FILLER" in title:
        return "Filler"
    if "PUBLIC SERVICE" in title:
        return "PublicService"
    return "Other"


def tokenize_row_payload(payload: str) -> tuple[str, str, str, str]:
    """
    After sched time, parse payload into:
    actual_time, duration_raw, video_source, type_code, and tail title text.
    """
    tokens = payload.split()
    if not tokens:
        return "", "", "", ""

    idx = 0
    actual_time = ""
    duration_raw = ""

    def cleaned_time_token(token: str) -> str:
        return re.sub(r"[^0-9:]", "", token)

    if idx < len(tokens) and TIMEISH_PATTERN.match(cleaned_time_token(tokens[idx])):
        actual_time = cleaned_time_token(tokens[idx])
        idx += 1

    if idx < len(tokens) and TIMEISH_PATTERN.match(cleaned_time_token(tokens[idx])):
        duration_raw = cleaned_time_token(tokens[idx])
        idx += 1
    elif actual_time and not duration_raw:
        # rows may omit actual_time; treat first as duration
        duration_raw = actual_time
        actual_time = ""

    video_source = tokens[idx] if idx < len(tokens) else ""
    idx += 1 if idx < len(tokens) else 0

    type_code = tokens[idx] if idx < len(tokens) and TYPE_PATTERN.match(tokens[idx]) else ""
    idx += 1 if type_code else 0

    title_raw = " ".join(tokens[idx:]).strip()
    return actual_time, duration_raw, video_source, type_code, title_raw


def parse_new_row_line(line: str, log_date: str) -> ParsedRow | None:
    match = SCHED_TIME_PATTERN.match(line)
    if not match:
        return None

    sched_time = match.group(1)
    payload = line[match.end():].strip()
    actual_time, duration_raw, video_source, type_code, title_raw = tokenize_row_payload(payload)

    parse_status = "ok"
    parse_error = ""
    if not duration_raw:
        parse_status = "partial"
        parse_error = "missing_duration"
    elif not type_code:
        parse_status = "partial"
        parse_error = "missing_type_code"

    return ParsedRow(
        log_date=log_date,
        sched_time=sched_time,
        actual_time=actual_time,
        duration_raw=duration_raw,
        video_source=video_source,
        type_code=type_code,
        title_raw=title_raw,
        full_row_text=line.strip(),
        parse_status=parse_status,
        parse_error=parse_error,
    )


def merge_continuation_line(row: ParsedRow, continuation: str) -> ParsedRow:
    continuation = continuation.strip()
    if not continuation:
        return row

    row.full_row_text = f"{row.full_row_text} {continuation}".strip()

    # Ignore pure duplicate asset code lines in title merge.
    if continuation == row.video_source:
        return row
    if continuation == row.type_code:
        return row

    row.title_raw = f"{row.title_raw} {continuation}".strip()
    return row


def iter_clean_lines(raw_text: str) -> Iterable[str]:
    for line in raw_text.splitlines():
        cleaned = line.replace("\t", " ").strip()
        cleaned = re.sub(r"\s+", " ", cleaned)
        if should_skip_line(cleaned):
            continue
        yield cleaned


def parse_single_pdf(pdf_path: Path) -> list[dict]:
    log_date = filename_to_log_date(pdf_path)
    raw_text = extract_pdf_text(pdf_path)
    lines = list(iter_clean_lines(raw_text))

    parsed_rows: list[ParsedRow] = []
    current: ParsedRow | None = None

    for line in lines:
        row = parse_new_row_line(line, log_date)
        if row is not None:
            if current is not None:
                parsed_rows.append(current)
            current = row
        elif current is not None:
            current = merge_continuation_line(current, line)

    if current is not None:
        parsed_rows.append(current)

    return [r.as_dict() for r in parsed_rows]


def parse_logs_folder(logs_dir: Path, output_csv: Path) -> pd.DataFrame:
    if not logs_dir.exists():
        raise FileNotFoundError(f"Logs folder not found: {logs_dir}")

    pdf_files = sorted(logs_dir.glob("*.pdf"))
    if not pdf_files:
        logger.warning("No PDF files found in %s", logs_dir)

    all_rows: list[dict] = []
    for pdf_file in pdf_files:
        try:
            rows = parse_single_pdf(pdf_file)
            all_rows.extend(rows)
            logger.info("Parsed %s -> %d rows", pdf_file.name, len(rows))
        except Exception as exc:
            logger.exception("Failed parsing %s", pdf_file.name)
            # Preserve failure record instead of dropping silently.
            all_rows.append(
                ParsedRow(
                    log_date=filename_to_log_date(pdf_file),
                    sched_time="",
                    duration_raw="",
                    video_source="",
                    type_code="",
                    title_raw="",
                    full_row_text=f"PARSE_FAILED::{pdf_file.name}",
                    parse_status="failed",
                    parse_error=str(exc),
                ).as_dict()
            )

    df = pd.DataFrame(all_rows)
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_csv, index=False)
    logger.info("Saved parsed logs CSV: %s (%d rows)", output_csv, len(df))
    return df


def main() -> None:
    project_root = Path(__file__).resolve().parent
    logs_dir = project_root / "Logs"
    output_csv = project_root / "data" / "output" / "parsed_logs.csv"
    parse_logs_folder(logs_dir, output_csv)


if __name__ == "__main__":
    main()
