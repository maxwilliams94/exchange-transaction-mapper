from __future__ import annotations

from collections import defaultdict
from decimal import Decimal
from pathlib import Path
from typing import Any, DefaultDict, Dict, Iterable, List, Optional, Tuple

from ..constants import OUTPUT_HEADERS
from ..utils import (
    abs_decimal_to_str,
    decimal_to_str,
    format_market,
    is_fiat,
    parse_decimal,
    parse_firi_timestamp,
    split_market,
)

MATCH_ACTION = "Match"
MATCH_FEE_ACTION = "MatchFee"
STAKING_REWARD_ACTION = "StakingReward"
BANK_DEPOSIT_ACTION = "BankDeposit"
BANK_WITHDRAW_ACTION = "BankWithdrawal"
INTERNAL_ACTIONS = {"InternalTransfer", "Stake"}


def _sum_amounts(rows: Iterable[Dict[str, str]]) -> Dict[str, Decimal]:
    totals: Dict[str, Decimal] = {}
    for row in rows:
        amount = parse_decimal(row.get("Amount")) or Decimal("0")
        currency = (row.get("Currency") or "").upper()
        if not currency:
            continue
        totals[currency] = totals.get(currency, Decimal("0")) + amount
    return totals


def _select_currency(totals: Dict[str, Decimal], prefer_fiat: bool) -> Tuple[str, Decimal]:
    candidate: Optional[Tuple[str, Decimal]] = None
    for currency, amount in totals.items():
        if prefer_fiat != is_fiat(currency):
            continue
        if candidate is None or abs(amount) > abs(candidate[1]):
            candidate = (currency, amount)
    if candidate:
        return candidate
    if totals:
        currency, amount = max(totals.items(), key=lambda item: abs(item[1]))
        return currency, amount
    return "", Decimal("0")


def _group_matches(rows: Iterable[Dict[str, str]]) -> DefaultDict[str, List[Dict[str, str]]]:
    grouped: DefaultDict[str, List[Dict[str, str]]] = defaultdict(list)
    for row in rows:
        action = (row.get("Action") or "").strip()
        if action not in {MATCH_ACTION, MATCH_FEE_ACTION}:
            continue
        match_id = (row.get("Match ID") or row.get("MatchId") or "").strip()
        if not match_id:
            continue
        grouped[match_id].append(row)
    return grouped


def _map_match(match_id: str, rows: List[Dict[str, str]]) -> Dict[str, str]:
    match_rows = [row for row in rows if (row.get("Action") or "").strip() == MATCH_ACTION]
    fee_rows = [row for row in rows if (row.get("Action") or "").strip() == MATCH_FEE_ACTION]

    match_totals = _sum_amounts(match_rows)
    base_currency, base_amount = _select_currency(match_totals, prefer_fiat=False)
    quote_currency, quote_amount = _select_currency(match_totals, prefer_fiat=True)

    side = "BUY" if base_amount >= 0 else "SELL"
    filled_quantity = abs(base_amount)
    filled_quote = abs(quote_amount)

    price: Optional[Decimal] = None
    if filled_quantity and filled_quote:
        price = filled_quote / filled_quantity

    fee_total = Decimal("0")
    fee_currency = ""
    if fee_rows:
        fee_totals = _sum_amounts(fee_rows)
        fee_currency, fee_total = _select_currency(fee_totals, prefer_fiat=True)
        fee_total = abs(fee_total)

    timestamps = [parse_firi_timestamp(row.get("Created at", "")) for row in rows]
    timestamp_candidates = [ts for ts in timestamps if ts]
    timestamp = min(timestamp_candidates) if timestamp_candidates else ""

    market = format_market(base_currency, quote_currency or "UNKNOWN")

    return {
        "Id": f"firi-match-{match_id}",
        "ExchangeId": match_id,
        "timeStamp": timestamp,
        "Status": "COMPLETED",
        "Market": market,
        "Exchange": "FIRI",
        "Side": side,
        "TransactionType": "TRADE",
        "FilledQuantity": abs_decimal_to_str(filled_quantity),
        "FilledQuote": abs_decimal_to_str(filled_quote),
        "FilledPrice": decimal_to_str(price),
        "Fee": abs_decimal_to_str(fee_total),
        "FeeCurrency": fee_currency,
    }


def _map_staking_reward(row: Dict[str, str]) -> Dict[str, str]:
    currency = (row.get("Currency") or "").upper()
    amount = parse_decimal(row.get("Amount")) or Decimal("0")
    timestamp = parse_firi_timestamp(row.get("Created at", ""))
    return {
        "Id": f"firi-staking-{row.get('Transaction ID', '')}",
        "ExchangeId": row.get("Transaction ID", ""),
        "timeStamp": timestamp,
        "Status": "COMPLETED",
        "Market": format_market(currency, "UNKNOWN"),
        "Exchange": "FIRI",
        "Side": "BUY",
        "TransactionType": "STAKING_REWARD",
        "FilledQuantity": abs_decimal_to_str(amount),
        "FilledQuote": "",
        "FilledPrice": "",
        "Fee": "",
        "FeeCurrency": "",
    }


def _map_bank_entry(row: Dict[str, str], transaction_type: str, side: str) -> Dict[str, str]:
    currency = (row.get("Currency") or "").upper()
    amount = parse_decimal(row.get("Amount")) or Decimal("0")
    timestamp = parse_firi_timestamp(row.get("Created at", ""))
    return {
        "Id": f"firi-{side.lower()}-{row.get('Transaction ID', '')}",
        "ExchangeId": row.get("Transaction ID", ""),
        "timeStamp": timestamp,
        "Status": "COMPLETED",
        "Market": currency,
        "Exchange": "FIRI",
        "Side": side,
        "TransactionType": transaction_type,
        "FilledQuantity": abs_decimal_to_str(amount),
        "FilledQuote": "",
        "FilledPrice": "",
        "Fee": "",
        "FeeCurrency": "",
    }


def _map_transactions(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    grouped = _group_matches(rows)
    mapped: List[Dict[str, str]] = []

    for match_id, match_rows in grouped.items():
        mapped.append(_map_match(match_id, match_rows))

    for row in rows:
        action = (row.get("Action") or "").strip()
        if action in {MATCH_ACTION, MATCH_FEE_ACTION}:
            continue
        if action in INTERNAL_ACTIONS:
            continue
        if action == STAKING_REWARD_ACTION:
            mapped.append(_map_staking_reward(row))
        elif action == BANK_DEPOSIT_ACTION:
            mapped.append(_map_bank_entry(row, "DEPOSIT", "DEPOSIT"))
        elif action == BANK_WITHDRAW_ACTION:
            mapped.append(_map_bank_entry(row, "WITHDRAWAL", "WITHDRAW"))

    mapped.sort(key=lambda item: item.get("timeStamp", ""))
    return mapped


def _map_trades(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    mapped: List[Dict[str, str]] = []
    for row in rows:
        trade_id = row.get("Trade") or row.get("trade")
        if not trade_id:
            continue
        market_symbol = (row.get("Market") or "").upper()
        base, quote = split_market(market_symbol)
        price = parse_decimal(row.get("Price"))
        volume = parse_decimal(row.get("Volume"))
        cost = parse_decimal(row.get("Cost"))
        volume_currency = (row.get("Volume currency") or "").upper()

        base_amount: Optional[Decimal] = None
        if volume is not None and volume_currency and base:
            if volume_currency == base.upper():
                base_amount = volume
        if base_amount is None and cost is not None and price:
            base_amount = cost / price if price else None
        if base_amount is None:
            continue
        quote_amount = (price * base_amount) if (price and base_amount) else cost or Decimal("0")
        side = "BUY" if (row.get("Order Type") or "").lower() == "bid" else "SELL"
        mapped.append({
            "Id": f"firi-trade-{trade_id}",
            "ExchangeId": trade_id,
            "timeStamp": parse_firi_timestamp(row.get("Executed", "")),
            "Status": "COMPLETED",
            "Market": format_market(base, quote or "UNKNOWN"),
            "Exchange": "FIRI",
            "Side": side,
            "TransactionType": "TRADE",
            "FilledQuantity": abs_decimal_to_str(base_amount),
            "FilledQuote": abs_decimal_to_str(quote_amount),
            "FilledPrice": decimal_to_str(price),
            "Fee": "",
            "FeeCurrency": "",
        })
    mapped.sort(key=lambda item: item.get("timeStamp", ""))
    return mapped


def _map_orders(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    mapped: List[Dict[str, str]] = []
    for row in rows:
        order_id = row.get("Order ID")
        if not order_id:
            continue
        market_symbol = (row.get("Market") or "").upper()
        base_market, quote_market = split_market(market_symbol)
        filled = parse_decimal(row.get("Filled")) or Decimal("0")
        price = parse_decimal(row.get("Price"))
        base_currency = (row.get("Filled currency") or base_market or "").upper()
        quote_currency = quote_market
        filled_quote = (price * filled) if (price and filled) else None
        side = "BUY" if (row.get("Order Type") or "").lower() == "bid" else "SELL"
        status = (row.get("Status") or "").upper()
        mapped.append({
            "Id": f"firi-order-{order_id}",
            "ExchangeId": order_id,
            "timeStamp": parse_firi_timestamp(row.get("Created at", "")),
            "Status": status,
            "Market": format_market(base_currency, quote_currency or "UNKNOWN"),
            "Exchange": "FIRI",
            "Side": side,
            "TransactionType": "ORDER",
            "FilledQuantity": abs_decimal_to_str(filled),
            "FilledQuote": abs_decimal_to_str(filled_quote),
            "FilledPrice": decimal_to_str(price),
            "Fee": "",
            "FeeCurrency": "",
        })
    mapped.sort(key=lambda item: item.get("timeStamp", ""))
    return mapped


def _ensure_columns(row: Dict[str, Any]) -> Dict[str, Any]:
    return {column: row.get(column, "") for column in OUTPUT_HEADERS}


def map_firi_file(_file_path: Path, rows: List[Dict[str, str]], _context: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not rows:
        return []
    header_keys = {key.strip() for key in rows[0].keys() if key}
    if "Action" in header_keys:
        mapped_rows = _map_transactions(rows)
    elif "Trade" in header_keys:
        mapped_rows = _map_trades(rows)
    elif "Order ID" in header_keys:
        mapped_rows = _map_orders(rows)
    else:
        mapped_rows = []
    return [_ensure_columns(row) for row in mapped_rows]
