# saij_core.py - descarga y búsqueda básica del dataset SAIJ PBA
import os, requests, pandas as pd, hashlib

PBA_PORTAL = "https://catalogo.datos.gba.gob.ar"
DATASET_SLUG = "base-saij-de-normativa-provincial"
CACHE_DIR = os.path.join(os.path.dirname(__file__), "_cache")
os.makedirs(CACHE_DIR, exist_ok=True)

def _ckan_package_show():
    url = f"{PBA_PORTAL}/api/3/action/package_show?id={DATASET_SLUG}"
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    data = r.json()
    if not data.get("success"):
        raise RuntimeError("Error CKAN")
    return data["result"]

def _best_resource(resources):
    for fmt in ["CSV", "XLSX", "XLS"]:
        for r in resources:
            if r.get("format","").upper() == fmt:
                return r
    return None

def _cache_path(url):
    h = hashlib.md5(url.encode()).hexdigest()
    return os.path.join(CACHE_DIR, f"{h}.csv")

def load_latest_dataframe():
    pkg = _ckan_package_show()
    res = _best_resource(pkg["resources"])
    url = res["url"]
    path = _cache_path(url)
    if not os.path.exists(path):
        r = requests.get(url, timeout=120)
        with open(path,"wb") as f: f.write(r.content)
    df = pd.read_csv(path, dtype=str, low_memory=False)
    return df

def search(df, query, limit=10):
    col = "sumario" if "sumario" in df.columns else df.columns[0]
    f = df[col].fillna("").str.contains(query, case=False, na=False)
    return df[f].head(limit)
