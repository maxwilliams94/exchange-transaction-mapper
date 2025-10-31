from __future__ import annotations

import csv
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional

from ..constants import OUTPUT_HEADERS
from ..utils import (
    abs_decimal_to_str,
    decimal_to_str,
    format_market,
    is_fiat,
    parse_decimal,
    parse_iso_timestamp,
)


def _read_nbx_rows(file_path: Path) -> List[Dict[str, str]]:
    with file_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle, delimiter=";")
        header = next(reader, None)
        if not header:
            return []
        header = [col.strip().strip('"') for col in header]
        rows: List[Dict[str, str]] = []
        for raw in reader:
            if not any(raw):
                continue
            normalized = [value.strip().strip('"') for value in raw]
            while len(normalized) < len(header):
                normalized.append("")
            row = {header[idx]: normalized[idx] for idx in range(len(header)) if header[idx]}
            rows.append(row)
        return rows


def _map_trade(row: Dict[str, str]) -> Dict[str, str]:
    tx_id = row.get("ID", "")
    amount_in = parse_decimal(row.get("In")) or Decimal("0")
    currency_in = (row.get("In-Currency") or "").upper()
    amount_out = parse_decimal(row.get("Out")) or Decimal("0")
    currency_out = (row.get("Out-Currency") or "").upper()
    fee = parse_decimal(row.get("Fee"))
    fee_currency = (row.get("Fee-Currency") or "").upper()

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
        filled_price = quote_amount / base_amount

    notes = row.get("Notes") or ""
    market = notes.replace("/", "-") if notes else format_market(base_currency, quote_currency)

    return {
        "Id": f"nbx-{tx_id}",
        "ExchangeId": tx_id,
        "timeStamp": parse_iso_timestamp(row.get("Timestamp", "")),
        "Status": "COMPLETED",
        "Market": market,
        "Exchange": "NBX",
        "Side": side,
        "TransactionType": "TRADE",
        "FilledQuantity": abs_decimal_to_str(base_amount),
        "FilledQuote": abs_decimal_to_str(quote_amount),
        "FilledPrice": decimal_to_str(filled_price),
        "Fee": abs_decimal_to_str(fee),
        "FeeCurrency": fee_currency,
    }


def _map_deposit_withdraw(row: Dict[str, str], transaction_type: str, side: str) -> Dict[str, str]:
    tx_id = row.get("ID", "")
    amount = parse_decimal(row.get("In")) or parse_decimal(row.get("Out")) or Decimal("0")
    currency = (row.get("In-Currency") or row.get("Out-Currency") or "").upper()
    fee = parse_decimal(row.get("Fee"))
    fee_currency = (row.get("Fee-Currency") or "").upper()
    return {
        "Id": f"nbx-{tx_id}",
        "ExchangeId": tx_id,
        "timeStamp": parse_iso_timestamp(row.get("Timestamp", "")),
        "Status": "COMPLETED",
        "Market": currency,
        "Exchange": "NBX",
        "Side": side,
        "TransactionType": transaction_type,
        "FilledQuantity": abs_decimal_to_str(amount),
        "FilledQuote": "",
        "FilledPrice": "",
        "Fee": abs_decimal_to_str(fee),
        "FeeCurrency": fee_currency,
    }


def map_nbx_file(file_path: Path, _rows: List[Dict[str, str]], _context: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows = _read_nbx_rows(file_path)
    mapped: List[Dict[str, str]] = []
    for row in rows:
        tx_type = (row.get("Type") or "").strip().title()
        if tx_type == "Trade":
            mapped.append(_map_trade(row))
        elif tx_type == "Deposit":
            mapped.append(_map_deposit_withdraw(row, "DEPOSIT", "DEPOSIT"))
        elif tx_type == "Withdraw":
            mapped.append(_map_deposit_withdraw(row, "WITHDRAWAL", "WITHDRAW"))
        else:
            continue
    ordered: List[Dict[str, Any]] = []
    for row in mapped:
        ordered.append({key: row.get(key, "") for key in OUTPUT_HEADERS})
    return ordered
