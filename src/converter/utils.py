from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import re
from typing import Optional, Tuple

from .constants import FIAT_CURRENCIES

_DECIMAL_CLEANER = re.compile(r"[^0-9,\-.]")


def parse_decimal(value: Optional[str]) -> Optional[Decimal]:
    if value is None:
        return None
    cleaned = value.strip()
    if not cleaned:
        return None
    cleaned = _DECIMAL_CLEANER.sub("", cleaned)
    if not cleaned or cleaned in {"-", "-.", "."}:
        return None
    # remove thousands separators
    if cleaned.count(",") > 0 and cleaned.count(".") == 0:
        cleaned = cleaned.replace(",", ".")
    else:
        cleaned = cleaned.replace(",", "")
    try:
        return Decimal(cleaned)
    except InvalidOperation:
        return None


def decimal_to_str(value: Optional[Decimal]) -> str:
    if value is None:
        return ""
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def abs_decimal_to_str(value: Optional[Decimal]) -> str:
    if value is None:
        return ""
    return decimal_to_str(abs(value))


def parse_coinbase_timestamp(raw: str) -> str:
    raw = (raw or "").strip()
    if not raw:
        return ""
    try:
        dt = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S %Z")
        return dt.replace(tzinfo=timezone.utc).isoformat()
    except ValueError:
        pass
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return dt.isoformat()
    except ValueError:
        return raw


def parse_iso_timestamp(raw: str) -> str:
    raw = (raw or "").strip()
    if not raw:
        return ""
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).isoformat()
    except ValueError:
        return raw


def parse_firi_timestamp(raw: str) -> str:
    raw = (raw or "").strip()
    if not raw:
        return ""
    patterns = [
        "%a %b %d %Y %H:%M:%S GMT%z (Coordinated Universal Time)",
        "%a %b %d %Y %H:%M:%S %Z",
    ]
    for fmt in patterns:
        try:
            dt = datetime.strptime(raw, fmt)
            return dt.astimezone(timezone.utc).isoformat()
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).isoformat()
    except ValueError:
        return raw


def parse_kraken_timestamp(raw: str) -> str:
    raw = (raw or "").strip()
    if not raw:
        return ""
    formats = [
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(raw, fmt)
            return dt.replace(tzinfo=timezone.utc).isoformat()
        except ValueError:
            continue
    return raw


def format_market(base: str, quote: Optional[str]) -> str:
    base_clean = (base or "").strip().upper()
    quote_clean = (quote or "").strip().upper()
    if not base_clean and not quote_clean:
        return ""
    if not quote_clean:
        return base_clean
    return f"{base_clean}-{quote_clean}"


def split_market(symbol: str) -> Tuple[str, Optional[str]]:
    cleaned = (symbol or "").strip().upper()
    if not cleaned:
        return "", None
    candidates = sorted(FIAT_CURRENCIES | {"USDC", "USDT", "BTC", "ETH"}, key=len, reverse=True)
    for quote in candidates:
        if cleaned.endswith(quote) and cleaned != quote:
            return cleaned[:-len(quote)], quote
    if len(cleaned) > 3:
        return cleaned[:-3], cleaned[-3:]
    return cleaned, None


def is_fiat(currency: Optional[str]) -> bool:
    return (currency or "").strip().upper() in FIAT_CURRENCIES
