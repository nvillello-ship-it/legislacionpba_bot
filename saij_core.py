# saij_core.py — Búsqueda PBA con ranking, parser NL y utilidades de comparación
import os, re, hashlib, requests, pandas as pd
from typing import Optional, Dict, List, Tuple
from rapidfuzz import fuzz
from unidecode import unidecode

PBA_PORTAL   = "https://catalogo.datos.gba.gob.ar"
DATASET_SLUG = "base-saij-de-normativa-provincial"
CACHE_DIR    = os.path.join(os.path.dirname(__file__), "_cache")
os.makedirs(CACHE_DIR, exist_ok=True)

CAND_COLS = {
    "provincia": ["provincia","jurisdiccion","jurisdicción"],
    "tipo":      ["tipo_norma","tipo","tipo de norma"],
    "numero":    ["numero","número","nro","n_norma","nro_norma"],
    "anio":      ["anio","año","ano","ano_norma","año_norma"],
    "fecha":     ["fecha","fecha_sancion","fecha sancion","fecha_sanción","fecha_publicacion"],
    "sumario":   ["sumario","titulo","título","caratula","carátula","descripcion","descripción"],
    "estado":    ["estado","vigencia","estado_vigencia"],
    "url":       ["url","enlace","link","href"]
}

# -------- descarga/caché --------
def _ckan_package_show() -> Dict:
    r = requests.get(f"{PBA_PORTAL}/api/3/action/package_show", params={"id": DATASET_SLUG}, timeout=60)
    r.raise_for_status()
    j = r.json()
    if not j.get("success"): raise RuntimeError("CKAN package_show no exitoso")
    return j["result"]

def _best_resource(resources) -> Optional[Dict]:
    prefer = ("CSV","XLSX","XLS")
    cands = [r for r in resources if r.get("format","").upper() in prefer and r.get("url")]
    if not cands: return None
    def score(r):
        fmt = prefer.index(r.get("format","").upper())
        mod = r.get("last_modified") or r.get("created") or ""
        return (fmt, mod)
    cands.sort(key=score)
    return cands[0]

def _cache_path(url: str) -> str:
    ext = ".csv" if ".csv" in url.lower() else (".xlsx" if ".xlsx" in url.lower() else ".xls")
    return os.path.join(CACHE_DIR, f"saij_pba_{hashlib.md5(url.encode()).hexdigest()}{ext}")

def load_latest_dataframe() -> pd.DataFrame:
    res = _best_resource(_ckan_package_show().get("resources", []))
    if not res: raise RuntimeError("No se encontró recurso descargable")
    url = res["url"]; path = _cache_path(url)
    if not os.path.exists(path):
        with requests.get(url, stream=True, timeout=300) as r:
            r.raise_for_status()
            with open(path, "wb") as f:
                for ch in r.iter_content(1<<20):
                    if ch: f.write(ch)
    if path.lower().endswith(".csv"):
        df = pd.read_csv(path, dtype=str, low_memory=False)
    else:
        df = pd.read_excel(path, dtype=str)
    df.columns = [c.strip() for c in df.columns]
    return df

# -------- helpers de columnas/normalización --------
def _pick_col(df: pd.DataFrame, keys: List[str]) -> Optional[str]:
    cols = list(df.columns); low = [c.lower() for c in cols]
    for k in keys:
        if k.lower() in low: return cols[low.index(k.lower())]
    for i,c in enumerate(low):
        if any(k.lower() in c for k in keys): return cols[i]
    return None

def _filter_pba(df: pd.DataFrame) -> pd.DataFrame:
    c = _pick_col(df, CAND_COLS["provincia"])
    if c and c in df.columns:
        return df[df[c].fillna("").str.contains("Buenos Aires", case=False, na=False)].copy()
    return df.copy()

def _norm(s: str) -> str:
    return unidecode((s or "").lower())

# -------- Parser de español (intención y filtros) --------
_TIPO_ALIASES = {"ley":"LEY","decreto":"DECRETO","resolucion":"RESOLUCIÓN","resolución":"RESOLUCIÓN","res.":"RESOLUCIÓN","decr.":"DECRETO"}

def parse_nl_query(text: str) -> Dict:
    raw = (text or "").strip()
    out = {"q": None, "tipo": None, "numero": None, "anio": None, "anio_desde": None, "anio_hasta": None, "vigente": None, "limit": None, "action": "search"}

    # intención comparar
    if re.search(r"\bcompar", raw, re.I):
        out["action"] = "compare"

    # "ley 14528", "decreto 2366/2025"
    m = re.search(r"(ley|decreto|resoluci[oó]n)\s+(\d+)(?:/\s*(\d{4}))?", raw, flags=re.I)
    if m:
        out["tipo"] = _TIPO_ALIASES.get(m.group(1).lower(), m.group(1).upper())
        out["numero"] = m.group(2)
        if m.group(3): out["anio"] = m.group(3)

    # tipo suelto
    for k,v in _TIPO_ALIASES.items():
        if re.search(rf"\b{k}\b", raw, re.I): out["tipo"] = v

    # vigencia
    if re.search(r"\b(vigente|vigentes|en vigor|activo)\b", raw, re.I): out["vigente"]=True
    if re.search(r"\b(derogad|no vigente|anulad|abrogad|caduc)\b", raw, re.I): out["vigente"]=False

    # años/rangos
    md = re.search(r"desde\s+(\d{4})", raw, re.I)
    mh = re.search(r"hasta\s+(\d{4})", raw, re.I)
    mr = re.search(r"(\d{4})\s*(?:a|hasta|-|al)\s*(\d{4})", raw, re.I)
    if md: out["anio_desde"] = md.group(1)
    if mh: out["anio_hasta"] = mh.group(1)
    if mr: out["anio_desde"], out["anio_hasta"] = mr.group(1), mr.group(2)
    ma = re.search(r"(?:año|anio)\s*(\d{4})", raw, re.I)
    if ma: out["anio"] = ma.group(1)

    ml = re.search(r"(?:limit|límite|limite)\s*[: ]\s*(\d{1,2})", raw, re.I)
    if ml: out["limit"] = int(ml.group(1))

    # quitar lo reconocido para "q"
    q = re.sub(r"(ley|decreto|resoluci[oó]n)\s+\d+(?:/\d{4})?", " ", raw, flags=re.I)
    q = re.sub(r"(desde|hasta)\s+\d{4}", " ", q, flags=re.I)
    q = re.sub(r"\b\d{4}\s*(?:a|hasta|-|al)\s*\d{4}\b", " ", q, flags=re.I)
    q = re.sub(r"(vigente|vigentes|derogad|no vigente|anulad|abrogad|caduc)", " ", q, flags=re.I)
    q = re.sub(r"(limit|l[ií]mite|limite)\s*[: ]\s*\d{1,2}", " ", q, flags=re.I)
    q = " ".join(q.split())
    out["q"] = q or None
    return out

# -------- ranking y búsqueda --------
def _rank_score(text: str, terms: List[str]) -> float:
    t = _norm(text); score = 0.0
    for term in terms:
        if not term: continue
        tt = _norm(term)
        score += 2.0 * t.count(tt)           # literal
        score += 0.02 * fuzz.partial_ratio(t, tt)  # aproximado
    return score

def search(df: pd.DataFrame,
           query: Optional[str]=None, tipo: Optional[str]=None,
           vigente: Optional[bool]=None, numero: Optional[str]=None,
           anio: Optional[str]=None, anio_desde: Optional[str]=None,
           anio_hasta: Optional[str]=None, limit: int=10) -> Tuple[pd.DataFrame, Dict[str,str]]:
    out = _filter_pba(df)
    c_sum = _pick_col(out, CAND_COLS["sumario"])
    c_tip = _pick_col(out, CAND_COLS["tipo"])
    c_est = _pick_col(out, CAND_COLS["estado"])
    c_num = _pick_col(out, CAND_COLS["numero"])
    c_an  = _pick_col(out, CAND_COLS["anio"])
    c_fe  = _pick_col(out, CAND_COLS["fecha"])
    c_url = _pick_col(out, CAND_COLS["url"])

    if tipo and c_tip: out = out[out[c_tip].fillna("").str.contains(tipo, case=False, na=False)]
    if numero and c_num: out = out[out[c_num].fillna("").str.contains(re.escape(str(numero)), case=False, na=False)]
    if anio and c_an:
        out = out[out[c_an].fillna("").str.contains(str(anio), case=False, na=False)]
    else:
        if (anio_desde or anio_hasta) and c_an:
            lo = int(anio_desde or "1800"); hi = int(anio_hasta or "9999")
            ext = out[c_an].fillna("").str.extract(r"(\d{4})", expand=False).fillna("0").astype(int)
            out = out[ext.between(lo, hi, inclusive="both")]

    if vigente is not None and c_est:
        if vigente:
            mask = out[c_est].fillna("").str.contains("vigente|en vigor|activo", case=False, na=False)
        else:
            mask = out[c_est].fillna("").str.contains("no vigente|derog|anulad|abrog|caduc", case=False, na=False)
        out = out[mask]

    if c_sum and query:
        terms = [t for t in (query or "").split() if t.strip()]
        out = out.copy()
        out["_score"] = out[c_sum].fillna("").apply(lambda s: _rank_score(str(s), terms))
        out = out.sort_values("_score", ascending=False).drop(columns=["_score"])
    elif c_fe:
        try:
            out["_f"] = pd.to_datetime(out[c_fe], errors="coerce", dayfirst=True)
            out = out.sort_values("_f", ascending=False).drop(columns=["_f"])
        except Exception:
            pass

    if limit: out = out.head(int(limit))
    cols = {"sumario":c_sum,"tipo":c_tip,"estado":c_est,"numero":c_num,"anio":c_an,"fecha":c_fe,"url":c_url}
    return out, cols

# -------- comparación de normas --------
def summarize_row(row: pd.Series, cols: Dict[str,str]) -> str:
    t = []
    for k in ("tipo","numero","anio","fecha","estado"):
        c = cols.get(k)
        if c and c in row and str(row[c]): t.append(f"{k.capitalize()}: {row[c]}")
    s = str(row.get(cols.get("sumario",""),""))
    return ("; ".join(t) + "\n" + s).strip()

def compare_rows(a: pd.Series, b: pd.Series, cols: Dict[str,str]) -> str:
    sa, sb = summarize_row(a, cols), summarize_row(b, cols)
    # similitud general
    sim = fuzz.token_set_ratio(sa, sb)
    # palabras clave distintas (muy simple, sirve para orientar)
    def keys(s): 
        toks = re.findall(r"[A-Za-zÁÉÍÓÚáéíóúñÑ]{4,}", _norm(s))
        stop = set("sobre para como ante entre hacia desde hasta fuera dentro este esta estaos estas".split())
        return set([w for w in toks if w not in stop])
    ka, kb = keys(sa), keys(sb)
    only_a = ", ".join(sorted(list(ka-kb))[:10])
    only_b = ", ".join(sorted(list(kb-ka))[:10])
    txt = (f"🔎 *Similitud*: {sim}%\n\n"
           f"◼️ A — {sa[:400]}\n\n"
           f"◼️ B — {sb[:400]}\n\n"
           f"🧩 Palabras/temas que aparecen solo en A: {only_a or '—'}\n"
           f"🧩 Palabras/temas que aparecen solo en B: {only_b or '—'}")
    return txt
