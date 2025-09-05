import os
import json
import re
import time
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from dateutil import parser as dtp

ARABICA_URL = os.getenv("ARABICA_URL")
CONILLON_URL = os.getenv("CONILLON_URL")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

OUT_HISTORY_FILE = "data/precos.json"
OUT_SUMMARY_FILE = "data/prices.json"

UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
CURRENCY_RE = re.compile(r"(\d{1,3}(?:\.\d{3})*,\d{2})")  # ex: 1.402,21

def br_to_float(s: str) -> float:
    return float(s.replace(".", "").replace(",", "."))

def fetch(url: str, retries: int = 3, backoff: float = 1.5) -> str:
    last = None
    for i in range(retries):
        try:
            r = requests.get(url, headers={"User-Agent": UA}, timeout=40)
            r.raise_for_status()
            return r.text
        except Exception as e:
            last = e
            time.sleep(backoff * (i+1))
    raise last

def parse_hist_from_text(text: str, tipo: str, fonte_url: str):
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    out = []
    i = 0
    while i < len(lines):
        line = lines[i]
        if line.startswith("Fechamento:"):
            m = re.search(r"(\d{2}/\d{2}/\d{4})", line)
            ref_date_iso = None
            if m:
                try:
                    ref_date_iso = dtp.parse(m.group(1), dayfirst=True).date().isoformat()
                except Exception:
                    ref_date_iso = None
            valor = None
            for j in range(i+1, min(i+11, len(lines))):
                m2 = CURRENCY_RE.search(lines[j])
                if m2:
                    valor = br_to_float(m2.group(1))
                    break
            if valor is not None:
                out.append({
                    "produto": "cafe",
                    "tipo": tipo,
                    "moeda": "BRL",
                    "valor": round(valor, 2),
                    "referente_a": ref_date_iso,
                    "fonte_url": fonte_url,
                    "coletado_em": datetime.now(timezone.utc).isoformat()
                })
        i += 1
    return out

def parse_page_direct(url: str, tipo: str):
    html = fetch(url)
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=True)
    items = parse_hist_from_text(text, tipo, url)
    return items, html

def extract_with_openai(html: str, tipo: str, fonte_url: str):
    if not OPENAI_API_KEY:
        return []
    endpoint = "https://api.openai.com/v1/responses"
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }
    schema = {
        "type": "array",
        "items": {
            "type": "object",
            "properties": {
                "produto": {"type": "string"},
                "tipo": {"type": "string"},
                "moeda": {"type": "string"},
                "valor": {"type": "number"},
                "referente_a": {"type": ["string", "null"]},
                "fonte_url": {"type": "string"},
                "coletado_em": {"type": "string"}
            },
            "required": ["produto","tipo","moeda","valor","fonte_url","coletado_em"],
            "additionalProperties": False
        }
    }
    prompt = f"""
Você receberá o HTML bruto de uma página do Notícias Agrícolas com histórico do indicador Cepea/Esalq de café {tipo}.
Extraia um ARRAY de objetos JSON, um por data exibida na página.
Para cada objeto:
- produto = "cafe"
- tipo = "{tipo}"
- moeda = "BRL"
- valor = número em reais (use ponto decimal; ex.: 1402.21)
- referente_a = data (YYYY-MM-DD) se constar algo como "Fechamento: DD/MM/AAAA"; senão null
- fonte_url = "{fonte_url}"
- coletado_em = timestamp ISO UTC atual (você pode repetir o mesmo para todos)

HTML:
{html[:150000]}
"""
    payload = {
        "model": "gpt-4o-mini",
        "input": prompt,
        "response_format": {
            "type": "json_schema",
            "json_schema": {"name": "lista_precos", "schema": schema, "strict": True}
        }
    }
    resp = requests.post(endpoint, headers=headers, json=payload, timeout=90)
    resp.raise_for_status()
    data = resp.json()
    output_text = data.get("output_text")
    if not output_text:
        try:
            output_text = data["output"][0]["content"][0]["text"]
        except Exception:
            raise RuntimeError("Não foi possível ler a saída JSON do modelo.")
    items = json.loads(output_text)
    now_iso = datetime.now(timezone.utc).isoformat()
    normalized = []
    for r in items:
        r["produto"] = "cafe"
        r["tipo"] = tipo
        r["moeda"] = "BRL"
        r["fonte_url"] = fonte_url
        r["coletado_em"] = r.get("coletado_em") or now_iso
        if isinstance(r.get("valor"), (int, float)):
            r["valor"] = round(float(r["valor"]), 2)
        normalized.append(r)
    return normalized

def parse_with_fallback(url: str, tipo: str):
    items, html = parse_page_direct(url, tipo)
    if not items:
        try:
            return extract_with_openai(html, tipo, url)
        except Exception:
            return items
    if len(items) <= 2 and OPENAI_API_KEY:
        try:
            ai_items = extract_with_openai(html, tipo, url)
            items.extend(ai_items)
        except Exception:
            pass
    return items

def load_history(path: str):
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []

def save_history(path: str, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)

def dedup(rows):
    d = {}
    for r in rows:
        key = (r.get("produto"), r.get("tipo"), (r.get("referente_a") or r["coletado_em"][:10]))
        prev = d.get(key)
        if not prev or r["coletado_em"] > prev["coletado_em"]:
            d[key] = r
    return sorted(d.values(), key=lambda x: ((x.get("referente_a") or ""), x.get("tipo",""), x["coletado_em"]))

def update_price_summary(rows):
    latest = {}
    for r in rows:
        if r.get("produto") == "cafe":
            tipo = r.get("tipo")
            ref_date = r.get("referente_a") or r["coletado_em"][:10]
            if tipo not in latest or ref_date > latest[tipo]["ref_date"]:
                latest[tipo] = {"ref_date": ref_date, "valor": r["valor"]}
    summary = {}
    if os.path.exists(OUT_SUMMARY_FILE):
        try:
            with open(OUT_SUMMARY_FILE, "r", encoding="utf-8") as f:
                summary = json.load(f)
        except Exception:
            summary = {}
    cafe = summary.get("cafe", {})
    date = None
    if "arabica" in latest and "conillon" in latest:
        date = latest["arabica"]["ref_date"] if latest["arabica"]["ref_date"] >= latest["conillon"]["ref_date"] else latest["conillon"]["ref_date"]
    elif "arabica" in latest:
        date = latest["arabica"]["ref_date"]
    elif "conillon" in latest:
        date = latest["conillon"]["ref_date"]
    if date:
        cafe["date"] = date
    if "arabica" in latest:
        cafe["arabica"] = latest["arabica"]["valor"]
    if "conillon" in latest:
        cafe["robusta"] = latest["conillon"]["valor"]
    summary["cafe"] = cafe
    os.makedirs(os.path.dirname(OUT_SUMMARY_FILE), exist_ok=True)
    with open(OUT_SUMMARY_FILE, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

def main():
    rows = load_history(OUT_HISTORY_FILE)
    if not isinstance(rows, list):
        rows = []
    if ARABICA_URL:
        rows += parse_with_fallback(ARABICA_URL, "arabica")
    if CONILLON_URL:
        rows += parse_with_fallback(CONILLON_URL, "conillon")
    rows = dedup(rows)
    save_history(OUT_HISTORY_FILE, rows)
    update_price_summary(rows)
    print(f"OK: {len(rows)} registros totais.")

if __name__ == "__main__":
    main()
