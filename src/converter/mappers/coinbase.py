from __future__ import annotations

import csv
from decimal import Decimal
from io import StringIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


TRANSACTION_TYPE_MAP = {
    "sell": "TRADE",
    "buy": "TRADE",
    "reward income": "REWARD",
    "staking income": "STAKING_REWARD",
    "airdrop": "AIRDROP",
    "deposit": "DEPOSIT",
    "withdrawal": "WITHDRAWAL",
}


def load_coinbase_rows(file_path: Path) -> Tuple[List[Dict[str, str]], Dict[str, Any]]:
    """Read a Coinbase export, extracting the account id metadata row."""

    content = file_path.read_text(encoding="utf-8")
    lines = content.splitlines()
    account_id: Optional[str] = None
    header_index: Optional[int] = None

    for idx, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("User,"):
            parts = [part.strip() for part in stripped.split(",")]
            if len(parts) >= 3 and parts[2]:
                account_id = parts[2]
        if stripped.startswith("ID,"):
            header_index = idx
            break

    if header_index is None:
        return [], {"account_id": account_id}

    data = "\n".join(lines[header_index:])
    reader = csv.DictReader(StringIO(data))
    rows: List[Dict[str, str]] = []
    for row in reader:
        identifier = (row.get("ID") or "").strip()
        if not identifier:
            continue
        normalized = {
            (key or "").strip(): (value or "").strip()
            for key, value in row.items()
        }
        rows.append(normalized)

    return rows, {"account_id": account_id}


def coinbase_determine_side(
    transaction_type: Optional[str], quantity: Optional[Decimal]
) -> str:
    tx_type = (transaction_type or "").lower()
    if "withdraw" in tx_type:
        return "WITHDRAW"
    if "deposit" in tx_type:
        return "DEPOSIT"
    if "sell" in tx_type:
        return "SELL"
    if "buy" in tx_type:
        return "BUY"
    if quantity is not None and quantity < 0:
        return "SELL"
    return "BUY"


def coinbase_transaction_type(transaction_type: Optional[str]) -> str:
    tx_type = (transaction_type or "").strip().lower()
    if not tx_type:
        return "UNKNOWN"
    return TRANSACTION_TYPE_MAP.get(tx_type, tx_type.upper())


def coinbase_compute_price(
    total_value: Optional[Decimal], quantity: Optional[Decimal]
) -> Optional[Decimal]:
    if total_value is None or quantity is None:
        return None
    quantity_abs = abs(quantity)
    if not quantity_abs:
        return None
    return abs(total_value) / quantity_abs


def coinbase_fee_currency(
    price_currency: Optional[str], fee_amount: Optional[Decimal]
) -> str:
    if fee_amount is None or not fee_amount:
        return ""
    return (price_currency or "").strip().upper()
