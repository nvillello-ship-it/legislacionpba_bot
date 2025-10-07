# bot.py - Legislacion PBA Bot
import os, logging
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes
from saij_core import load_latest_dataframe, search

logging.basicConfig(level=logging.INFO)
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Hola! Soy legislacionpba_bot. Escribime un texto y te busco las normas correspondientes de la Provincia de Buenos Aires.")

async def handle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.message.text
    df = load_latest_dataframe()
    result = search(df, q, 5)
    if result.empty:
        await update.message.reply_text("Sin resultados.")
    else:
        text = "\n\n".join(result.iloc[:,0].astype(str).head(5))
        await update.message.reply_text(text[:3900])

def main():
    if not TOKEN:
        raise RuntimeError("Falta TELEGRAM_BOT_TOKEN")
    app = ApplicationBuilder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle))
    app.run_polling()

if __name__ == "__main__":
    main()
