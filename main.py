import pandas as pd
import requests
from io import StringIO, BytesIO
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
import asyncio
import logging

# Setup detailed logging
logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# Bot configuration
BOT_TOKEN = "7688630745:AAHAaaupNBxo6CD_YkifO4VQNyiI2emy9DY"
CHANNEL_ID = "@QUIZGOVERMENTEXAM"
CSV_URL = "https://raw.githubusercontent.com/Govexam/telegramquiz/refs/heads/main/mg.csv"

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info("Received /start command")
    await update.message.reply_text(
        "Welcome to @KUNALQUIZ_BOT! Send a CSV or use /postquiz to fetch quizzes."
    )

async def handle_csv(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info("Received a message, checking for CSV")
    if update.message.document and update.message.document.file_name.endswith(".csv"):
        logger.info(f"Detected CSV file: {update.message.document.file_name}")
        try:
            file = await update.message.document.get_file()
            file_bytes = await file.download_as_bytearray()
            context.user_data["csv_file"] = file_bytes
            df = pd.read_csv(BytesIO(file_bytes))
            logger.info(f"CSV uploaded successfully. Columns: {df.columns.tolist()}")
            await update.message.reply_text("CSV received! Send /postquiz to post.")
        except Exception as e:
            logger.error(f"CSV upload error: {e}")
            await update.message.reply_text(f"Error with CSV: {e}. Needs Question, Option1-4, CorrectAnswer.")
    else:
        logger.info("No CSV detected in message")
        await update.message.reply_text("Please send a .csv file.")

async def post_quiz(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info("Starting /postquiz command")
    csv_data = None
    source = "upload"
    try:
        # Test poll to verify bot functionality
        logger.info("Sending test poll")
        test_poll = await context.bot.send_poll(
            chat_id=CHANNEL_ID,
            question="Test: Is this working?",
            options=["Yes", "No", "Maybe", "Not sure"],
            type="quiz",
            correct_option_id=0,
            explanation="Test OK! © Exams Insight",
            is_anonymous=True,
        )
        logger.info(f"Test poll sent: {test_poll.poll.id}")
        await update.message.reply_text("Test poll posted!")

        # Check for uploaded CSV
        logger.info(f"Checking context.user_data: {context.user_data}")
        if "csv_file" in context.user_data and context.user_data["csv_file"] is not None:
            logger.info("Found uploaded CSV in context.user_data")
            csv_data = context.user_data["csv_file"]
            df = pd.read_csv(BytesIO(csv_data))
            source = "upload"
        else:
            # Fetch from URL if no uploaded CSV
            source = "web"
            logger.info(f"No uploaded CSV found, fetching from {CSV_URL}")
            response = requests.get(CSV_URL)
            response.raise_for_status()
            df = pd.read_csv(StringIO(response.text))
        logger.info(f"Loaded CSV from {source} with columns: {df.columns.tolist()}")
        logger.info(f"First row: {df.iloc[0].to_dict()}")

        # Require exactly these columns
        required_columns = ["Question", "Option1", "Option2", "Option3", "Option4", "CorrectAnswer"]
        if not all(col in df.columns for col in required_columns):
            logger.error("Missing required columns")
            await update.message.reply_text("CSV must have: Question, Option1-4, CorrectAnswer.")
            return

        for index, row in df.iterrows():
            question = row["Question"]
            options = [row["Option1"], row["Option2"], row["Option3"], row["Option4"]]
            if len([opt for opt in options if pd.notna(opt)]) != 4:
                logger.warning(f"Row {index+1}: Invalid option count")
                await update.message.reply_text(f"Row {index+1}: Must have exactly 4 options.")
                continue
            correct_answer = row["CorrectAnswer"]
            if correct_answer not in options:
                logger.warning(f"Row {index+1}: CorrectAnswer mismatch")
                await update.message.reply_text(f"Row {index+1}: CorrectAnswer must match one option.")
                continue
            correct_option_id = options.index(correct_answer)
            base_explanation = row.get("Explanation", "Correct! 🎉").strip()[:30]
            explanation = f"{base_explanation} © Exams Insight"
            logger.info(f"Sending poll: {question} | Explanation: '{explanation}' (Length: {len(explanation)})")

            # Send poll
            try:
                poll_message = await context.bot.send_poll(
                    chat_id=CHANNEL_ID,
                    question=str(question)[:255],
                    options=[str(opt)[:100] for opt in options],
                    type="quiz",
                    correct_option_id=correct_option_id,
                    explanation=explanation[:300],
                    is_anonymous=True,
                )
                logger.info(f"Poll posted: {question} | Poll ID: {poll_message.poll.id}")
                await update.message.reply_text(f"Posted: {question}")
                await context.bot.send_message(
                    chat_id=CHANNEL_ID,
                    text=f"Explanation: {explanation}"
                )
                await asyncio.sleep(2)
            except Exception as e:
                logger.error(f"Poll posting failed for '{question}': {e}")
                await update.message.reply_text(f"Failed to post '{question}': {str(e)}")
                continue
        await update.message.reply_text(f"All quizzes posted from {source}!")
    except requests.exceptions.RequestException as e:
        logger.error(f"URL fetch error: {e}")
        await update.message.reply_text(f"Failed to fetch CSV from URL: {e}. Upload a CSV instead.")
    except Exception as e:
        logger.error(f"General error: {e}")
        await update.message.reply_text(f"Error: {str(e)}")

def main():
    logger.info("Starting bot")
    application = Application.builder().token(BOT_TOKEN).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_csv))
    application.add_handler(CommandHandler("postquiz", post_quiz))
    application.run_polling()

if __name__ == "__main__":
    main()