from __future__ import annotations

import csv
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ..utils import format_market, is_fiat, parse_decimal


@dataclass
class NbxTradeBreakdown:
    side: str
    base_amount: Decimal
    base_currency: str
    quote_amount: Decimal
    quote_currency: str
    price: Optional[Decimal]
    market: str


def load_nbx_rows(file_path: Path) -> Tuple[List[Dict[str, str]], Dict[str, Any]]:
    """Read NBX annual report exports which use semicolon delimiters."""

    with file_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle, delimiter=";")
        header = next(reader, None)
        if not header:
            return [], {}
        header = [col.strip().strip('"') for col in header]
        rows: List[Dict[str, str]] = []
        for raw in reader:
            if not any(raw):
                continue
            normalized = [value.strip().strip('"') for value in raw]
            while len(normalized) < len(header):
                normalized.append("")
            row = {
                header[idx]: normalized[idx]
                for idx in range(len(header))
                if header[idx]
            }
            rows.append(row)
        return rows, {}


def nbx_trade_breakdown(row: Dict[str, str]) -> Optional[NbxTradeBreakdown]:
    tx_type = (row.get("Type") or "").strip().title()
    if tx_type != "Trade":
        return None

    amount_in = parse_decimal(row.get("In")) or Decimal("0")
    currency_in = (row.get("In-Currency") or "").upper()
    amount_out = parse_decimal(row.get("Out")) or Decimal("0")
    currency_out = (row.get("Out-Currency") or "").upper()

    if is_fiat(currency_in) and not is_fiat(currency_out):
        side = "SELL"
        base_amount = amount_out
        base_currency = currency_out
        quote_amount = amount_in
        quote_currency = currency_in
    elif not is_fiat(currency_in) and is_fiat(currency_out):
        side = "BUY"
        base_amount = amount_in
        base_currency = currency_in
        quote_amount = amount_out
        quote_currency = currency_out
    else:
        side = "BUY"
        base_amount = amount_in or amount_out
        base_currency = currency_in or currency_out
        quote_amount = amount_out if base_amount == amount_in else amount_in
        quote_currency = currency_out or currency_in

    filled_price: Optional[Decimal] = None
    if base_amount and quote_amount:
        try:
            filled_price = quote_amount / base_amount
        except (ArithmeticError, InvalidOperation):  # pragma: no cover - defensive
            filled_price = None

    notes = (row.get("Notes") or "").strip()
    market = notes.replace("/", "-") if notes else format_market(base_currency, quote_currency)

    return NbxTradeBreakdown(
        side=side,
        base_amount=base_amount,
        base_currency=base_currency,
        quote_amount=quote_amount,
        quote_currency=quote_currency,
        price=filled_price,
        market=market,
    )
