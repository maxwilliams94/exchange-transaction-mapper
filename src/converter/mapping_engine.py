from __future__ import annotations

from decimal import Decimal
from typing import Any, Dict, List, Optional, cast

from .constants import OUTPUT_HEADERS
from .utils import (
    abs_decimal_to_str,
    decimal_to_str,
    format_market,
    is_fiat,
    parse_coinbase_timestamp,
    parse_decimal,
    parse_firi_timestamp,
    parse_iso_timestamp,
    parse_kraken_timestamp,
)
from .mappers.coinbase import (
    coinbase_compute_price,
    coinbase_determine_side,
    coinbase_fee_currency,
    coinbase_transaction_type,
)
from .mappers.nbx import nbx_trade_breakdown


class MappingConfigurationError(RuntimeError):
    """Raised when a mapping expression cannot be evaluated."""


_SAFE_GLOBALS: Dict[str, Any] = {"__builtins__": {}}

_BASE_ENV: Dict[str, Any] = {
    "Decimal": Decimal,
    "decimal": parse_decimal,
    "decimal_to_str": decimal_to_str,
    "abs_decimal_to_str": abs_decimal_to_str,
    "format_market": format_market,
    "parse_coinbase_timestamp": parse_coinbase_timestamp,
    "parse_iso_timestamp": parse_iso_timestamp,
    "parse_firi_timestamp": parse_firi_timestamp,
    "parse_kraken_timestamp": parse_kraken_timestamp,
    "is_fiat": is_fiat,
    "coinbase_determine_side": coinbase_determine_side,
    "coinbase_transaction_type": coinbase_transaction_type,
    "coinbase_compute_price": coinbase_compute_price,
    "coinbase_fee_currency": coinbase_fee_currency,
    "nbx_trade_breakdown": nbx_trade_breakdown,
    "abs": abs,
    "max": max,
    "min": min,
    "round": round,
    "str": str,
    "len": len,
}


def _normalize_output_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, Decimal):
        return decimal_to_str(value)
    if isinstance(value, float):
        return decimal_to_str(Decimal(str(value)))
    if isinstance(value, int):
        return str(value)
    if isinstance(value, dict):
        raise MappingConfigurationError(
            "Mapping expressions must resolve to primitive values; received a dict"
        )
    if isinstance(value, list):
        raise MappingConfigurationError(
            "Mapping expressions must resolve to primitive values; received a list"
        )
    if isinstance(value, str):
        return value.strip()
    return str(value)


def _evaluate_expression(
    expression: str,
    env: Dict[str, Any],
    label: str,
    row_index: Optional[int],
) -> Any:
    try:
        return eval(expression, _SAFE_GLOBALS, env)
    except Exception as exc:  # pragma: no cover - defensive
        location = f"row {row_index}" if row_index is not None else "row"
        raise MappingConfigurationError(
            f"Error evaluating expression '{expression}' for {label} ({location}): {exc}"
        ) from exc


def _build_environment(row: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
    env = dict(_BASE_ENV)
    env["row"] = row
    env["context"] = context
    env["config"] = context.get("config")
    if "row_index" in context:
        env["row_index"] = context["row_index"]
        env["row_number"] = context["row_index"]
    return env


def apply_row_mapping(
    row: Dict[str, Any],
    file_config: Dict[str, Any],
    context: Dict[str, Any],
) -> Optional[Dict[str, str]]:
    env = _build_environment(row, context)
    row_index = context.get("row_index")

    precompute = cast(Dict[str, str], file_config.get("precompute") or {})
    for name, expr in precompute.items():
        env[name] = _evaluate_expression(expr, env, f"precompute:{name}", row_index)

    skip_when = cast(List[str], file_config.get("skip_when") or [])
    for predicate in skip_when:
        if _evaluate_expression(predicate, env, "skip_when", row_index):
            return None

    mapping = cast(Dict[str, str], file_config.get("mapping") or {})
    defaults = cast(Dict[str, Any], file_config.get("defaults") or {})

    result: Dict[str, str] = {}
    for column in OUTPUT_HEADERS:
        if column in mapping:
            value = _evaluate_expression(
                mapping[column], env, f"column:{column}", row_index
            )
        else:
            value = defaults.get(column, "")
        result[column] = _normalize_output_value(value)
    return result
