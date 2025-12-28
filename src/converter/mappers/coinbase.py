from __future__ import annotations

import csv
import logging
import re
from dataclasses import dataclass
from decimal import Decimal
from io import StringIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ..utils import parse_decimal

logger = logging.getLogger(__name__)


@dataclass
class CoinbaseAdvancedTrade:
    base_amount: Decimal
    base_currency: str
    quote_amount: Decimal
    quote_currency: str
    market: str
    price: Decimal


TRANSACTION_TYPE_MAP = {
    "sell": "TRADE",
    "buy": "TRADE",
    "advanced trade sell": "TRADE",
    "advanced trade buy": "TRADE",
    "reward income": "STAKING_REWARD",
    "staking income": "STAKING_REWARD",
    "airdrop": "AIRDROP",
    "deposit": "DEPOSIT",
    "exchange deposit": "INTERNAL_TRANSFER",
    "receive": "STAKING_REWARD",
    "withdrawal": "WITHDRAW",
    "exchange withdrawal": "INTERNAL_TRANSFER",
    "pro withdrawal": "WITHDRAW",
    "pro deposit": "TRADE",
    "send": "WITHDRAW",
    "retail staking transfer": "INTERNAL_TRANSFER",
    "retail unstaking transfer": "INTERNAL_TRANSFER",
}

# Transaction types are normalized via TRANSACTION_TYPE_MAP to match downstream import expectations.


_ADVANCED_TRADE_REGEX = re.compile(
    r"(?:Bought|Sold)\s+([-0-9.]+)\s+([A-Za-z]+)\s+for\s+([-0-9.]+)\s+([A-Za-z]+)\s+on\s+([A-Za-z]+-[A-Za-z]+)\s+at\s+([-0-9.]+)\s+([A-Za-z]+)/([A-Za-z]+)",
    re.IGNORECASE,
)

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
    tx_type = (transaction_type or "").strip().lower()
    mapped_type = TRANSACTION_TYPE_MAP.get(tx_type)

    # Internal transfers should never carry a side
    if mapped_type == "INTERNAL_TRANSFER" or "transfer" in tx_type:
        return ""
    if "withdraw" in tx_type:
        return ""
    # Pro Deposit is a TRADE and needs a side based on quantity; skip deposit check for it
    if "deposit" in tx_type and "pro" not in tx_type:
        return ""
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
    mapped = TRANSACTION_TYPE_MAP.get(tx_type)
    if mapped:
        return mapped
    # Warn if transaction type is not in the mapping
    logger.warning(
        f"Unknown Coinbase transaction type '{transaction_type}' - using "
        f"'{tx_type.upper()}' as fallback. Please add this to TRANSACTION_TYPE_MAP "
        f"if needed."
    )
    return tx_type.upper()


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


def coinbase_parse_advanced_trade(notes: Optional[str]) -> Optional[CoinbaseAdvancedTrade]:
    """Extract advanced trade details from the Coinbase notes text.

    Coinbase advanced trade rows for ETH-USDC include the real quote currency in the
    notes even when the Price Currency column shows a fiat currency. We parse the
    amounts and currencies from the notes so downstream balance updates are correct.
    """

    if not notes:
        return None

    match = _ADVANCED_TRADE_REGEX.search(notes)
    if not match:
        return None

    base_amount = parse_decimal(match.group(1))
    base_currency = match.group(2).upper()
    quote_amount = parse_decimal(match.group(3))
    quote_currency = match.group(4).upper()
    market = match.group(5).upper()
    price = parse_decimal(match.group(6))

    if None in (base_amount, quote_amount, price):
        return None

    return CoinbaseAdvancedTrade(
        base_amount=base_amount,
        base_currency=base_currency,
        quote_amount=quote_amount,
        quote_currency=quote_currency,
        market=market,
        price=price,
    )
