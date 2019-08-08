from db import DB
from bot import IstorayjeBot
import logging, os

# Enable logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.DEBUG)

logger = logging.getLogger(__name__)

bot = IstorayjeBot(os.environ['TOKEN'], DB(url=os.environ['MONGO_URL']))

mode = os.environ.get('MODE', 'dev')

if mode == 'prod':
    bot.start_webhook()
else:
    bot.start_polling()