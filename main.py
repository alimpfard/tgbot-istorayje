from db import DB
from bot import IstorayjeBot
import logging, os

# Enable logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.WARN
)

logger = logging.getLogger(__name__)

mode = os.environ.get("MODE", "dev")

bot = IstorayjeBot(
    tokens=os.environ["TOKEN"].split(";"),
    db=DB(url=os.environ["MONGO_URL"]),
    dev=mode == "dev",
)

print("Starting to do shit")

bot.start_webhook() if mode == "prod" else bot.start_polling()
