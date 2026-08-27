from datetime import datetime, timezone

import httpx


EXCHANGE_INFO_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"


async def fetch_instrument_metadata(
    symbols: list[str]
) -> dict[str, dict]:
    requested_symbols = {symbol.upper() for symbol in symbols}

    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.get(EXCHANGE_INFO_URL)
        response.raise_for_status()
        exchange_info = response.json()

    metadata = {}

    for instrument in exchange_info["symbols"]:
        symbol = instrument["symbol"]
        if symbol not in requested_symbols:
            continue

        filters = {
            item["filterType"]: item
            for item in instrument["filters"]
        }
        notional_filter = filters.get("MIN_NOTIONAL", filters.get("NOTIONAL", {}))
        onboard_date = instrument.get("onboardDate")
        if onboard_date is not None:
            onboard_date = datetime.fromtimestamp(
                onboard_date / 1000,
                tz=timezone.utc
            ).isoformat()

        metadata[symbol.lower()] = {
            "contract_type": instrument["contractType"],
            "status": instrument["status"],
            "base_asset": instrument["baseAsset"],
            "quote_asset": instrument["quoteAsset"],
            "tick_size": filters["PRICE_FILTER"]["tickSize"],
            "quantity_step": filters["LOT_SIZE"]["stepSize"],
            "price_precision": instrument.get("pricePrecision"),
            "quantity_precision": instrument.get("quantityPrecision"),
            "min_quantity": filters["LOT_SIZE"].get("minQty"),
            "min_notional": notional_filter.get("notional"),
            "onboard_date": onboard_date
        }

    missing_symbols = requested_symbols - {symbol.upper() for symbol in metadata}
    if missing_symbols:
        missing = ", ".join(sorted(missing_symbols))
        raise ValueError(f"Instrument metadata not found for: {missing}")

    return metadata
