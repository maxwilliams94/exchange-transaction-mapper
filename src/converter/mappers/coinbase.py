from __future__ import annotations

import csv
from dataclasses import dataclass
from decimal import Decimal
from io import StringIO
from pathlib import Path
from typing import Any, Dict, List, Optional

from ..constants import OUTPUT_HEADERS
from ..utils import (
    abs_decimal_to_str,
    decimal_to_str,
    format_market,
    parse_coinbase_timestamp,
    parse_decimal,
)


@dataclass
class CoinbaseContext:
    account_id: Optional[str]


TRANSACTION_TYPE_MAP = {
    "sell": "TRADE",
    "buy": "TRADE",
    "reward income": "REWARD",
    "staking income": "STAKING_REWARD",
    "airdrop": "AIRDROP",
    "deposit": "DEPOSIT",
    "withdrawal": "WITHDRAWAL",
}


def _extract_account_and_rows(file_path: Path) -> tuple[CoinbaseContext, List[Dict[str, str]]]:
    content = file_path.read_text(encoding="utf-8")
    lines = content.splitlines()
    account_id: Optional[str] = None
    header_index = None
    for idx, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("User,"):
            parts = [part.strip() for part in stripped.split(",")]
            if len(parts) >= 3:
                account_id = parts[2] or None
        if stripped.startswith("ID,"):
            header_index = idx
            break
    if header_index is None:
        return CoinbaseContext(account_id), []
    data = "\n".join(lines[header_index:])
    reader = csv.DictReader(StringIO(data))
    rows = [row for row in reader if row.get("ID")]
    return CoinbaseContext(account_id), rows


def _determine_side(tx_type: str, quantity: Decimal) -> str:
    lower_type = tx_type.lower()
    if "withdraw" in lower_type:
        return "WITHDRAW"
    if "deposit" in lower_type:
        return "DEPOSIT"
    if "sell" in lower_type or quantity < 0:
        return "SELL"
    return "BUY"


def _map_transaction_type(tx_type: str) -> str:
    return TRANSACTION_TYPE_MAP.get(tx_type.lower(), tx_type.upper())


def map_coinbase_file(file_path: Path, _rows: List[Dict[str, str]], _context: Dict[str, Any]) -> List[Dict[str, Any]]:
    context, rows = _extract_account_and_rows(file_path)
    mapped: List[Dict[str, Any]] = []
    for row in rows:
        tx_id = row.get("ID", "").strip()
        if not tx_id:
            continue
        tx_type = row.get("Transaction Type", "").strip()
        asset = row.get("Asset", "").strip().upper()
        price_currency = row.get("Price Currency", "").strip().upper()
        quantity = parse_decimal(row.get("Quantity Transacted")) or Decimal("0")
        subtotal = parse_decimal(row.get("Subtotal"))
        total = parse_decimal(row.get("Total (inclusive of fees and/or spread)"))
        fee = parse_decimal(row.get("Fees and/or Spread"))
        explicit_price = parse_decimal(row.get("Price at Transaction"))

        filled_quantity = abs(quantity)
        filled_quote = abs(total or subtotal or Decimal("0"))

        price = explicit_price
        if not price and filled_quantity and filled_quote:
            price = filled_quote / filled_quantity

        side = _determine_side(tx_type, quantity)
        transaction_type = _map_transaction_type(tx_type)

        fee_value = abs(fee) if fee else None
        fee_currency = price_currency if fee_value else ""

        mapped.append({
            "Id": f"coinbase-{tx_id}",
            "ExchangeId": context.account_id or tx_id,
            "timeStamp": parse_coinbase_timestamp(row.get("Timestamp", "")),
            "Status": "COMPLETED",
            "Market": format_market(asset, price_currency),
            "Exchange": "COINBASE",
            "Side": side,
            "TransactionType": transaction_type,
            "FilledQuantity": abs_decimal_to_str(filled_quantity),
            "FilledQuote": abs_decimal_to_str(filled_quote),
            "FilledPrice": decimal_to_str(price),
            "Fee": abs_decimal_to_str(fee_value),
            "FeeCurrency": fee_currency,
        })
    # ensure columns ordered
    ordered: List[Dict[str, Any]] = []
    for row in mapped:
        ordered.append({key: row.get(key, "") for key in OUTPUT_HEADERS})
    return ordered
