# bot.py — Legislación PBA (v3 conversacional con intenciones)
import os, logging, re
import pandas as pd
from typing import Dict, Any, Tuple
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, CallbackQueryHandler, ContextTypes, filters
from saij_core import load_latest_dataframe, search as saij_search, parse_nl_query, compare_rows

logging.basicConfig(level=logging.INFO)
TOKEN = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
DF: pd.DataFrame | None = None

HELP = (
    "🧠 *Asistente de Legislación PBA*\n"
    "Preguntá en español y te entiendo: `adopción vigentes desde 2020`, `ley 14528`, `compará 14528 con 15464`.\n\n"
    "Filtros: `tipo:LEY`, `anio:2023`, `numero:14134`, `vigente:true|false`, `limit:15`.\n"
    "Comandos: /help /status /limite 15 /vigente on|off /detalle N /comparar N M\n"
)

def _format_page(df: pd.DataFrame, cols: Dict[str,str], offset=0, page=5) -> Tuple[str, InlineKeyboardMarkup | None]:
    items = df.iloc[offset:offset+page]
    lines=[]
    for idx, row in items.iterrows():
        partes=[]
        if cols.get("tipo") and cols["tipo"] in row and str(row[cols["tipo"]]): partes.append(str(row[cols["tipo"]])[:60])
        if cols.get("numero") and cols["numero"] in row and str(row[cols["numero"]]): partes.append(f"N° {row[cols['numero']]}")
        if cols.get("anio") and cols["anio"] in row and str(row[cols["anio"]]): partes.append(f"Año {row[cols['anio']]}")
        titulo=" — ".join(partes) if partes else "Norma"
        sumario=str(row.get(cols.get("sumario",""),""))[:360]
        url=str(row.get(cols.get("url",""),"")).strip()
        block=f"*{idx+1}. {titulo}*\n{sumario}"
        if url.startswith("http"): block+=f"\n{url}"
        lines.append(block)
    text="\n\n".join(lines) if lines else "⚠️ Sin resultados."
    btns=[]
    if offset>0: btns.append(InlineKeyboardButton("⬅️ Anterior", callback_data=f"page:{max(0,offset-page)}"))
    if offset+page < len(df): btns.append(InlineKeyboardButton("Siguiente ➡️", callback_data=f"page:{offset+page}"))
    kb = InlineKeyboardMarkup([btns]) if btns else None
    return text, kb

# -------- commands --------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_markdown_v2("👋 Bienvenido.\n\n"+HELP)

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_markdown_v2(HELP)

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    n = len(DF) if DF is not None else 0
    await update.message.reply_text(f"📊 Base cargada: {n} filas (Provincia de Buenos Aires).")

async def set_limit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        n = int((context.args or [""])[0]); n = max(1, min(50, n))
        context.user_data["limit"]=n
        await update.message.reply_text(f"✅ Límite fijado en {n}.")
    except Exception:
        await update.message.reply_text("Usá: /limite 15")

async def set_vigente(update: Update, context: ContextTypes.DEFAULT_TYPE):
    val = (context.args or [""])[0].lower() if context.args else ""
    if val in ("on","true","si","sí","1"):
        context.user_data["vigente"]=True;  await update.message.reply_text("✅ Filtro vigente: ON")
    elif val in ("off","false","0","no"):
        context.user_data["vigente"]=False; await update.message.reply_text("✅ Filtro vigente: OFF")
    else:
        await update.message.reply_text("Usá: /vigente on | off")

async def detalle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        i = int((context.args or [""])[0]) - 1
        df: pd.DataFrame = context.user_data.get("last_results")
        cols = context.user_data.get("last_cols", {})
        if df is None or i<0 or i>=len(df):
            await update.message.reply_text("No tengo resultados en memoria o índice inválido.")
            return
        row = df.iloc[i]
        partes=[]
        for k in ("tipo","numero","anio","fecha","estado"):
            c = cols.get(k)
            if c and c in row and str(row[c]): partes.append(f"{k.capitalize()}: {row[c]}")
        sumario = str(row.get(cols.get("sumario",""),""))
        url = str(row.get(cols.get("url",""),"")).strip()
        txt = f"🧾 *Detalle #{i+1}*\n" + ("\n".join(partes) + "\n\n" if partes else "") + sumario
        if url.startswith("http"): txt += f"\n{url}"
        await update.message.reply_markdown(txt, disable_web_page_preview=False)
    except Exception:
        await update.message.reply_text("Usá: /detalle N (ej: /detalle 2)")

async def comparar_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        a = int((context.args or [""])[0]) - 1
        b = int((context.args or ["",""])[1]) - 1
        df: pd.DataFrame = context.user_data.get("last_results")
        cols = context.user_data.get("last_cols", {})
        if df is None: 
            await update.message.reply_text("No tengo resultados en memoria. Hacé primero una búsqueda.")
            return
        txt = compare_rows(df.iloc[a], df.iloc[b], cols)
        await update.message.reply_markdown(txt, disable_web_page_preview=False)
    except Exception:
        await update.message.reply_text("Usá: /comparar N M (ej: /comparar 1 3)")

# -------- main handler (intenciones) --------
async def handle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global DF
    if DF is None:
        await update.message.reply_text("⏳ Cargando base SAIJ PBA (primera vez). Puede tardar 1–2 minutos…")
        DF = load_latest_dataframe()

    raw = update.message.text or ""
    intent = parse_nl_query(raw)

    # override por preferencias del usuario
    if intent.get("limit") is None and "limit" in context.user_data:
        intent["limit"] = context.user_data["limit"]
    if intent.get("vigente") is None and "vigente" in context.user_data:
        intent["vigente"] = context.user_data["vigente"]
    if intent.get("limit") is None: intent["limit"]=10

    # intención: comparar por números explícitos (p.ej. "compará 14528 con 15464")
    if intent["action"] == "compare" and re.findall(r"\d{4,6}", raw):
        nums = re.findall(r"\d{4,6}", raw)
        # buscamos cada número por separado y comparamos el mejor match de cada uno
        found=[]
        for n in nums[:2]:
            df1, cols1 = saij_search(DF, numero=n, limit=1)
            if df1.empty: continue
            found.append((df1.iloc[0], cols1))
        if len(found)==2:
            a,(brow, bcols) = found[0][0], found[1]
            txt = compare_rows(a, brow, found[0][1])
            await update.message.reply_markdown(txt, disable_web_page_preview=False)
            return

    # por defecto, búsqueda
    df, cols = saij_search(
        DF, query=intent.get("q"), tipo=intent.get("tipo"), vigente=intent.get("vigente"),
        numero=intent.get("numero"), anio=intent.get("anio"),
        anio_desde=intent.get("anio_desde"), anio_hasta=intent.get("anio_hasta"),
        limit=max(10, intent["limit"])
    )

    if df.empty:
        await update.message.reply_text("⚠️ Sin resultados. Probá con menos palabras o quitá filtros /vigente off.")
        return

    context.user_data["last_results"]=df.reset_index(drop=True)
    context.user_data["last_cols"]=cols
    context.user_data["offset"]=0

    text, kb = _format_page(context.user_data["last_results"], cols, offset=0, page=5)
    await update.message.reply_markdown(text, reply_markup=kb, disable_web_page_preview=False)

async def page_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    if not (q.data or "").startswith("page:"): return
    off = int(q.data.split(":",1)[1])
    df: pd.DataFrame = context.user_data.get("last_results")
    cols = context.user_data.get("last_cols", {})
    if df is None:
        await q.edit_message_text("No hay resultados. Enviá una consulta nueva.")
        return
    text, kb = _format_page(df, cols, offset=off, page=5)
    context.user_data["offset"]=off
    try:
        await q.edit_message_text(text, reply_markup=kb, parse_mode="Markdown", disable_web_page_preview=False)
    except Exception:
        await q.message.reply_markdown(text, reply_markup=kb, disable_web_page_preview=False)

def main():
    if not TOKEN: raise SystemExit("Falta TELEGRAM_BOT_TOKEN")
    app = ApplicationBuilder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("status", status))
    app.add_handler(CommandHandler("limite", set_limit))
    app.add_handler(CommandHandler("vigente", set_vigente))
    app.add_handler(CommandHandler("detalle", detalle))
    app.add_handler(CommandHandler("comparar", comparar_cmd))
    app.add_handler(CallbackQueryHandler(page_cb))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle))
    app.run_polling()

if __name__ == "__main__":
    main()
