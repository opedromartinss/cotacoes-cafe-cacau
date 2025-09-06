#!/usr/bin/env python3
"""
Scraper and updater for coffee prices.

This script fetches the latest coffee prices for Arábica and Robusta (Conilon)
from the Notícias Agrícolas widgets and writes the results into two JSON
files used by the site:

* ``data/prices.json`` – holds the most recent price snapshot along with
  metadata like the update time, whether the market is open, and the units
  associated with each product.
* ``data/precos.json`` – maintains a rolling history of up to ten unique
  trading days (as defined by the ``referente_a`` field).  Each day has
  two entries: one for Arábica and one for Conilon.  Older entries beyond
  ten days are pruned to keep the file lightweight.

Additionally, the script updates ``index.html`` so the current prices are
rendered directly in the HTML.  This ensures search engines can read the
values as plain text rather than relying solely on client‑side JavaScript.

The script is idempotent: running it repeatedly in a short period will
overwrite ``data/prices.json`` with fresh metadata and append a record to
``data/precos.json`` only when a new date is encountered.  Existing
entries for the current date are replaced.

Usage:
    python scrape_prices.py

Dependencies:
    - requests
    - beautifulsoup4

Environment:
    This script assumes it resides in the root directory of the site
    repository.  Paths are computed relative to this file.
"""

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Tuple, List

import bs4
import requests


def parse_price(url: str) -> Tuple[str, float]:
    """Fetch a price table from Notícias Agrícolas and return the date and price.

    The widget pages present a simple HTML table containing one row
    with the most recent trading date and its corresponding price.  This
    function retrieves the page, parses it with BeautifulSoup and extracts
    the date string (dd/mm/YYYY) and price as a floating point number.

    Args:
        url: The URL of the widget (e.g., ``https://www.noticiasagricolas.com.br/widgets/cotacoes?id=29``).

    Returns:
        A tuple ``(data, preco)`` where ``data`` is a string in
        ``dd/mm/YYYY`` format and ``preco`` is the numeric value of the
        quoted price (Brazilian real per saca).

    Raises:
        Exception: If the request fails or the expected table structure
        cannot be found.
    """
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    html = resp.text
    soup = bs4.BeautifulSoup(html, "html.parser")
    tbody = soup.find("tbody")
    if not tbody:
        raise ValueError(f"No table body found in {url}")
    row = tbody.find("tr")
    if not row:
        raise ValueError(f"No data row found in {url}")
    cols = [c.get_text(strip=True) for c in row.find_all("td")]
    if len(cols) < 2:
        raise ValueError(f"Unexpected column count in {url}: {cols}")
    date_str = cols[0]  # e.g. '05/09/2025'
    # Convert '2.277,03' -> 2277.03
    price_str = cols[1].strip().replace(".", "").replace(",", ".")
    try:
        price = float(price_str)
    except ValueError as e:
        raise ValueError(f"Could not parse price '{cols[1]}' from {url}") from e
    return date_str, price


def is_market_open(now: datetime) -> bool:
    """Determine whether the market is open at a given moment.

    The coffee market is considered open from 8:00 to 17:00 on weekdays
    (Monday through Friday).  Outside of these hours or on weekends the
    function returns ``False``.

    Args:
        now: The current datetime.

    Returns:
        ``True`` if within market hours on a weekday, ``False`` otherwise.
    """
    return now.weekday() < 5 and 8 <= now.hour < 17


def update_prices(prices_path: Path, arabica_price: float, conilon_price: float) -> None:
    """Write the latest prices and metadata to ``prices.json``.

    Args:
        prices_path: Path to the ``prices.json`` file.
        arabica_price: Current price for Arábica per saca.
        conilon_price: Current price for Conilon/Robusta per saca.
    """
    now = datetime.now()
    data_formatada = now.strftime("%d/%m/%Y")
    hora_formatada = now.strftime("%H:%M:%S")
    data = {
        "ultima_atualizacao": now.isoformat(),
        "data_formatada": data_formatada,
        "hora_formatada": hora_formatada,
        "pregao_aberto": is_market_open(now),
        "fonte": "noticiasagricolas",
        "cafe": {
            "arabica": {
                "preco": arabica_price,
                "unidade": "saca",
                "peso_kg": 60,
                "moeda": "BRL",
            },
            "robusta": {
                "preco": conilon_price,
                "unidade": "saca",
                "peso_kg": 60,
                "moeda": "BRL",
            },
        },
        # Retain existing keys for cacau if they exist; if not, provide
        # sensible placeholders.  Consumers of this file should ignore
        # cacau entries if they are not updated here.
        "cacau": {
            "bahia": {
                "preco": None,
                "unidade": "arroba",
                "peso_kg": 15,
                "moeda": "BRL",
            },
            "para": {
                "preco": None,
                "unidade": "arroba",
                "peso_kg": 15,
                "moeda": "BRL",
            },
        },
    }
    prices_path.parent.mkdir(parents=True, exist_ok=True)
    with prices_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def update_history(history_path: Path, arabica_price: float, conilon_price: float, trade_date: str, collected: datetime) -> None:
    """Append today's prices to ``precos.json`` and prune older entries.

    Each entry in the history file contains the following fields:

    - ``referente_a``: The trading date (YYYY‑MM‑DD) the price refers to.
    - ``coletado_em``: The ISO timestamp when the data was scraped.
    - ``produto``: Always ``"cafe"`` for coffee.
    - ``tipo``: Either ``"arabica"`` or ``"conillon"``.
    - ``valor``: The numeric price per saca.
    - ``unidade``: The unit (always ``"saca"``).
    - ``moeda``: The currency (``"BRL"``).

    The file is truncated to keep only the latest ten unique ``referente_a``
    dates, preserving all entries for each retained date.

    Args:
        history_path: Path to the ``precos.json`` file.
        arabica_price: Latest Arábica price.
        conilon_price: Latest Conilon price.
        trade_date: The date string from the widget (dd/mm/YYYY).  This is
            converted to YYYY‑MM‑DD when stored in the history.
        collected: Datetime when the scraping occurred.
    """
    # Convert the provided trade date (dd/mm/YYYY) to ISO date (YYYY‑MM‑DD)
    try:
        date_obj = datetime.strptime(trade_date, "%d/%m/%Y")
    except ValueError:
        # Fallback to today's date if parsing fails
        date_obj = collected
    referente_a = date_obj.strftime("%Y-%m-%d")
    history_path.parent.mkdir(parents=True, exist_ok=True)
    # Load existing history if present
    if history_path.exists():
        with history_path.open("r", encoding="utf-8") as f:
            try:
                history: List[dict] = json.load(f)
            except json.JSONDecodeError:
                history = []
    else:
        history = []
    # Remove any existing entries for the same date and types to avoid duplicates
    history = [record for record in history if not (
        record.get("referente_a") == referente_a and record.get("produto") == "cafe"
    )]
    # Append new records for the current day
    iso_ts = collected.isoformat()
    history.extend([
        {
            "referente_a": referente_a,
            "coletado_em": iso_ts,
            "produto": "cafe",
            "tipo": "arabica",
            "valor": arabica_price,
            "unidade": "saca",
            "moeda": "BRL",
        },
        {
            "referente_a": referente_a,
            "coletado_em": iso_ts,
            "produto": "cafe",
            "tipo": "conillon",
            "valor": conilon_price,
            "unidade": "saca",
            "moeda": "BRL",
        },
    ])
    # Build a mapping of date -> records and keep only the latest 10 dates
    by_date: dict[str, List[dict]] = {}
    for record in history:
        by_date.setdefault(record["referente_a"], []).append(record)
    # Sort dates descending and keep 10 most recent
    dates_sorted = sorted(by_date.keys(), reverse=True)
    keep_dates = dates_sorted[:10]
    pruned_history: List[dict] = []
    for d in keep_dates:
        # Sort each day's records by type for consistency
        day_records = sorted(by_date[d], key=lambda r: r.get("tipo", ""))
        pruned_history.extend(day_records)
    # Write back to file
    with history_path.open("w", encoding="utf-8") as f:
        json.dump(pruned_history, f, ensure_ascii=False, indent=2)


def update_index_html(index_path: Path, arabica_price: float, conilon_price: float) -> None:
    """Inject the latest prices into ``index.html``.

    This function parses the home page using BeautifulSoup, locates the
    elements with IDs ``preco-arabica`` and ``preco-robusta`` and replaces
    their text with the formatted currency values.  If those elements are
    not found (which could happen if the HTML structure changes), the
    function does nothing.

    Args:
        index_path: Path to the ``index.html`` file.
        arabica_price: Latest Arábica price.
        conilon_price: Latest Conilon price.
    """
    if not index_path.exists():
        return
    with index_path.open("r", encoding="utf-8") as f:
        html = f.read()
    soup = bs4.BeautifulSoup(html, "html.parser")
    def format_brl(value: float) -> str:
        # Format number to Brazilian currency string (e.g., R$ 2.277,03)
        return f"R$ {value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    arabica_elem = soup.find(id="preco-arabica")
    if arabica_elem:
        arabica_elem.string = format_brl(arabica_price)
    robusta_elem = soup.find(id="preco-robusta")
    if robusta_elem:
        robusta_elem.string = format_brl(conilon_price)
    # Write back only if changes were made
    with index_path.open("w", encoding="utf-8") as f:
        f.write(str(soup))


def main() -> None:
    # Determine repository root based on this file's location
    root = Path(__file__).resolve().parent
    data_dir = root / "data"
    prices_path = data_dir / "prices.json"
    history_path = data_dir / "precos.json"
    index_path = root / "index.html"
    # URLs for coffee widgets
    arabica_url = "https://www.noticiasagricolas.com.br/widgets/cotacoes?id=29"
    conilon_url = "https://www.noticiasagricolas.com.br/widgets/cotacoes?id=31"
    # Fetch prices
    date_arabica, price_arabica = parse_price(arabica_url)
    date_conilon, price_conilon = parse_price(conilon_url)
    # If dates differ, choose the most recent one for history
    trade_date = date_arabica if date_arabica >= date_conilon else date_conilon
    now = datetime.now()
    # Update files
    update_prices(prices_path, price_arabica, price_conilon)
    update_history(history_path, price_arabica, price_conilon, trade_date, now)
    update_index_html(index_path, price_arabica, price_conilon)


if __name__ == "__main__":
    main()