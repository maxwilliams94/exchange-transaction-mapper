from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from ..constants import OUTPUT_HEADERS
from ..utils import (
    abs_decimal_to_str,
    decimal_to_str,
    format_market,
    is_fiat,
    parse_decimal,
    parse_kraken_timestamp,
)


@dataclass
class KrakenLedgerRow:
    txid: str
    refid: str
    time: str
    event_type: str
    subtype: str
    asset: str
    amount: Decimal
    fee: Decimal


def _normalize_row(raw: Dict[str, str]) -> KrakenLedgerRow:
    return KrakenLedgerRow(
        txid=(raw.get("txid") or "").strip(),
        refid=(raw.get("refid") or "").strip(),
        time=parse_kraken_timestamp(raw.get("time") or ""),
        event_type=(raw.get("type") or "").strip().lower(),
        subtype=(raw.get("subtype") or "").strip().lower(),
        asset=(raw.get("asset") or "").strip().upper(),
        amount=parse_decimal(raw.get("amount")) or Decimal("0"),
        fee=parse_decimal(raw.get("fee")) or Decimal("0"),
    )


def _group_by_refid(rows: Iterable[KrakenLedgerRow]) -> Dict[str, List[KrakenLedgerRow]]:
    grouped: Dict[str, List[KrakenLedgerRow]] = defaultdict(list)
    for row in rows:
        key = row.refid or row.txid
        grouped[key].append(row)
    return grouped


def _map_reward(rows: List[KrakenLedgerRow]) -> Optional[Dict[str, Any]]:
    reward = rows[0]
    if not reward.asset:
        return None
    identifier = reward.refid or reward.txid
    return {
        "Id": identifier,
        "ExchangeId": identifier,
        "timeStamp": reward.time,
        "Status": "COMPLETED",
        "Market": reward.asset,
        "Exchange": "KRAKEN",
        "Side": "BUY",
        "TransactionType": "AIRDROP",
        "FilledQuantity": abs_decimal_to_str(reward.amount),
        "FilledQuote": "",
        "FilledPrice": "",
        "Fee": abs_decimal_to_str(reward.fee) if reward.fee else "",
        "FeeCurrency": reward.asset if reward.fee else "",
    }


def _map_trade_group(rows: List[KrakenLedgerRow]) -> Optional[Dict[str, Any]]:
    positive_rows = [row for row in rows if row.amount > 0]
    negative_rows = [row for row in rows if row.amount < 0]

    if not positive_rows and not negative_rows:
        return None

    receive = positive_rows[0] if positive_rows else None
    spend = negative_rows[0] if negative_rows else None

    base_row: Optional[KrakenLedgerRow]
    quote_row: Optional[KrakenLedgerRow]
    side = "BUY"

    if receive and spend:
        receive_is_fiat = is_fiat(receive.asset)
        spend_is_fiat = is_fiat(spend.asset)
        if receive_is_fiat and not spend_is_fiat:
            side = "SELL"
            base_row = spend
            quote_row = receive
        else:
            side = "BUY"
            base_row = receive
            quote_row = spend
    elif spend and not receive:
        side = "SELL"
        base_row = spend
        quote_row = None
    else:
        base_row = receive
        quote_row = None

    if base_row is None:
        return None

    identifier = base_row.refid or base_row.txid

    timestamp = base_row.time
    if receive and not timestamp:
        timestamp = receive.time
    if spend and not timestamp:
        timestamp = spend.time

    base_currency = base_row.asset
    quote_currency = quote_row.asset if quote_row else ""

    base_amount = abs(base_row.amount)
    quote_amount = abs(quote_row.amount) if quote_row else None

    price: Optional[Decimal] = None
    if base_amount and quote_amount:
        try:
            price = quote_amount / base_amount
        except (ArithmeticError, ZeroDivisionError):  # pragma: no cover - defensive
            price = None

    fee_total = sum((row.fee for row in rows), Decimal("0"))
    fee_currency = quote_currency or base_currency if fee_total else ""

    return {
        "Id": identifier,
        "ExchangeId": identifier,
        "timeStamp": timestamp,
        "Status": "COMPLETED",
        "Market": format_market(base_currency, quote_currency),
        "Exchange": "KRAKEN",
        "Side": side,
        "TransactionType": "TRADE",
        "FilledQuantity": abs_decimal_to_str(base_amount),
        "FilledQuote": abs_decimal_to_str(quote_amount) if quote_amount is not None else "",
        "FilledPrice": decimal_to_str(price),
        "Fee": abs_decimal_to_str(fee_total) if fee_total else "",
        "FeeCurrency": fee_currency,
    }


def map_kraken_ledger(
    _file_path: Path, rows: List[Dict[str, str]], _context: Dict[str, Any]
) -> List[Dict[str, Any]]:
    if not rows:
        return []

    normalized = [_normalize_row(row) for row in rows if row.get("txid")]
    grouped = _group_by_refid(normalized)

    mapped: List[Dict[str, Any]] = []
    for group in grouped.values():
        if not group:
            continue
        event_type = group[0].event_type
        if event_type == "reward":
            mapped_row = _map_reward(group)
        elif event_type in {"trade", "spend", "receive"}:
            mapped_row = _map_trade_group(group)
        else:
            mapped_row = None
        if mapped_row:
            mapped.append({key: mapped_row.get(key, "") for key in OUTPUT_HEADERS})

    mapped.sort(key=lambda row: row.get("timeStamp", ""))
    return mapped