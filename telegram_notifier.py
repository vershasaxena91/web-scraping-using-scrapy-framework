from telegram.ext import Updater


class TelegramNotifier:
    def __init__(self):
        self.client = Updater("2126958170:AAEj2G0a8j33aaPSgVydkqEQGlY4TTg5lHU")

    def send_message(self, msg):
        for i in range(1 + len(msg) // 4096):
            self.client.bot.send_message("-736146093", msg[i * 4096 : (i + 1) * 4096])
