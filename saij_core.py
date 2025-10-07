# saij_core.py — Núcleo SAIJ PBA (descarga + búsqueda avanzada)
import os, re, hashlib, requests, pandas as pd
from typing import Optional, Dict, List, Tuple

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
    h = hashlib.md5(url.encode()).hexdigest()
    ext = ".csv" if ".csv" in url.lower() else (".xlsx" if ".xlsx" in url.lower() else ".xls")
    return os.path.join(CACHE_DIR, f"saij_pba_{h}{ext}")

def load_latest_dataframe() -> pd.DataFrame:
    res = _best_resource(_ckan_package_show().get("resources", []))
    if not res: raise RuntimeError("No se encontró recurso descargable")
    url = res["url"]
    path = _cache_path(url)
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

def search(df: pd.DataFrame, query: Optional[str]=None, tipo: Optional[str]=None,
           vigente: Optional[bool]=None, numero: Optional[str]=None, anio: Optional[str]=None,
           limit: int=10) -> Tuple[pd.DataFrame, Dict[str,str]]:
    out = _filter_pba(df)

    c_sum = _pick_col(out, CAND_COLS["sumario"])
    c_tip = _pick_col(out, CAND_COLS["tipo"])
    c_est = _pick_col(out, CAND_COLS["estado"])
    c_num = _pick_col(out, CAND_COLS["numero"])
    c_an  = _pick_col(out, CAND_COLS["anio"])
    c_fe  = _pick_col(out, CAND_COLS["fecha"])
    c_url = _pick_col(out, CAND_COLS["url"])

    if tipo and c_tip:
        out = out[out[c_tip].fillna("").str.contains(tipo, case=False, na=False)]
    if numero and c_num:
        out = out[out[c_num].fillna("").str.contains(re.escape(str(numero)), case=False, na=False)]
    if anio and c_an:
        out = out[out[c_an].fillna("").str.contains(str(anio), case=False, na=False)]
    if vigente is not None and c_est:
        if vigente:
            mask = out[c_est].fillna("").str.contains("vigente|en vigor|activo", case=False, na=False)
        else:
            mask = out[c_est].fillna("").str.contains("no vigente|derog|anulad|abrog|caduc", case=False, na=False)
        out = out[mask]
    if query and c_sum:
        for t in [t.strip() for t in query.split() if t.strip()]:
            out = out[out[c_sum].fillna("").str.contains(t, case=False, na=False)]

    if c_fe:
        try:
            out["_f"] = pd.to_datetime(out[c_fe], errors="coerce", dayfirst=True)
            out = out.sort_values("_f", ascending=False).drop(columns=["_f"])
        except Exception:
            pass

    if limit: out = out.head(int(limit))
    cols = {"sumario":c_sum,"tipo":c_tip,"estado":c_est,"numero":c_num,"anio":c_an,"fecha":c_fe,"url":c_url}
    return out, cols
