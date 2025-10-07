# bot.py — Legislación PBA conversacional (Telegram)
import os, logging, re
from typing import Dict, Any, Tuple
import pandas as pd
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, CallbackQueryHandler, ContextTypes, filters
from saij_core import load_latest_dataframe, search as saij_search

logging.basicConfig(level=logging.INFO)
TOKEN = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()

# Cache global
DF: pd.DataFrame | None = None

HELP = (
    "🧭 *Cómo buscar* (podés mezclar palabras y filtros):\n"
    "• Palabras: `seguridad pública`, `inteligencia artificial`, `ambiente`\n"
    "• Filtros: `tipo:LEY` `tipo:DECRETO` `anio:2022` `numero:14134` `vigente:true|false` `limit:10`\n\n"
    "Ejemplos:\n"
    "• `seguridad pública tipo:LEY vigente:true limit:10`\n"
    "• `inteligencia artificial anio:2023`\n"
    "• `loteo numero:8912 tipo:DECRETO`\n"
    "Usá /limite 15 o /vigente on|off para fijar preferencias.\n"
)

def parse_params(text: str) -> Dict[str, Any]:
    p = {"q": None, "tipo": None, "vigente": None, "numero": None, "anio": None, "limit": None}
    parts = (text or "").split()
    free = []
    for tok in parts:
        if ":" in tok:
            k,v = tok.split(":",1)
            k = k.lower().strip(); v = v.strip()
            if k=="tipo": p["tipo"]=v
            elif k=="vigente": p["vigente"]=v.lower() in ("true","1","si","sí","on","vigente")
            elif k=="numero": p["numero"]=v
            elif k=="anio": p["anio"]=v
            elif k=="limit":
                try: p["limit"]=max(1,min(50,int(v)))
                except: pass
        else:
            free.append(tok)
    if free: p["q"]=" ".join(free)
    return p

def format_results(df: pd.DataFrame, cols: Dict[str,str], offset=0, page_size=5) -> Tuple[str, InlineKeyboardMarkup | None, int]:
    items = df.iloc[offset:offset+page_size]
    if items.empty:
        return "⚠️ Sin resultados en esta página.", None, offset
    lines=[]
    for _,row in items.iterrows():
        partes=[]
        if cols.get("tipo") and cols["tipo"] in row and str(row[cols["tipo"]]):
            partes.append(str(row[cols["tipo"]])[:60])
        if cols.get("numero") and cols["numero"] in row and str(row[cols["numero"]]):
            partes.append(f"N° {row[cols['numero']]}")
        if cols.get("anio") and cols["anio"] in row and str(row[cols['anio']]):
            partes.append(f"Año {row[cols['anio']]}")
        titulo=" — ".join(partes) if partes else "Norma"
        sumario=str(row.get(cols.get("sumario",""),""))[:380]
        url=str(row.get(cols.get("url",""),"")).strip()
        block=f"• *{titulo}*\n{sumario}"
        if url.startswith("http"):
            block+=f"\n{url}"
        lines.append(block)
    text="\n\n".join(lines)
    # Paginación
    keyboard=[]
    if offset>0: keyboard.append(InlineKeyboardButton("⬅️ Anterior", callback_data=f"page:{max(0,offset- page_size)}"))
    if offset+page_size < len(df): keyboard.append(InlineKeyboardButton("Siguiente ➡️", callback_data=f"page:{offset+page_size}"))
    kb = InlineKeyboardMarkup([keyboard]) if keyboard else None
    return text, kb, offset

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_markdown_v2("👋 Bot de *Legislación PBA* listo.\n\n"+HELP)

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_markdown_v2(HELP)

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global DF
    n = len(DF) if DF is not None else 0
    await update.message.reply_text(f"📊 Base cargada: {n} filas. Jurisdicción: Provincia de Buenos Aires.")

async def set_limit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        n = int((context.args or [""])[0])
        context.user_data["limit"]=max(1,min(50,n))
        await update.message.reply_text(f"✅ Límite fijado en {context.user_data['limit']}.")
    except Exception:
        await update.message.reply_text("Usá: /limite 10")

async def set_vigente(update: Update, context: ContextTypes.DEFAULT_TYPE):
    val = (context.args or [""])[0].lower() if context.args else ""
    if val in ("on","true","si","sí","1"):
        context.user_data["vigente"]=True
        await update.message.reply_text("✅ Filtro vigente: ON")
    elif val in ("off","false","0","no"):
        context.user_data["vigente"]=False
        await update.message.reply_text("✅ Filtro vigente: OFF")
    else:
        await update.message.reply_text("Usá: /vigente on  o  /vigente off")

async def handle_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global DF
    if DF is None:
        await update.message.reply_text("⏳ Cargando base SAIJ PBA (primera vez). Puede tardar 1–2 minutos…")
        DF = load_latest_dataframe()

    txt = update.message.text or ""
    p = parse_params(txt)
    # aplica preferencias del usuario
    if p["limit"] is None and "limit" in context.user_data:
        p["limit"]=context.user_data["limit"]
    if p["vigente"] is None and "vigente" in context.user_data:
        p["vigente"]=context.user_data["vigente"]
    if p["limit"] is None: p["limit"]=10

    df, cols = saij_search(DF, query=p["q"], tipo=p["tipo"], vigente=p["vigente"],
                           numero=p["numero"], anio=p["anio"], limit=max(10,p["limit"]))
    if df.empty:
        await update.message.reply_text("⚠️ Sin resultados. Probá quitar filtros o usar menos palabras.")
        return

    # guardar contexto para paginar
    context.user_data["last_results"]=df.reset_index(drop=True)
    context.user_data["last_cols"]=cols
    context.user_data["offset"]=0

    text, kb, _ = format_results(context.user_data["last_results"], cols, offset=0, page_size=5)
    await update.message.reply_markdown(text, reply_markup=kb, disable_web_page_preview=False)

async def page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    if not data.startswith("page:"): return
    offset = int(data.split(":",1)[1])
    df: pd.DataFrame = context.user_data.get("last_results")
    cols = context.user_data.get("last_cols", {})
    if df is None:
        await q.edit_message_text("No hay resultados en memoria. Enviá una consulta nueva.")
        return
    text, kb, _ = format_results(df, cols, offset=offset, page_size=5)
    context.user_data["offset"]=offset
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
    app.add_handler(CallbackQueryHandler(page_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_query))
    app.run_polling()

if __name__ == "__main__":
    main()
