import telebot
from telebot import types

# Используем предоставленный токен
TOKEN = '8331773910:AAE4HqX39-LYlHm1ZirdSqrnjZuut7trE1w'
bot = telebot.TeleBot(TOKEN)

@bot.message_handler(commands=['start'])
def send_welcome(message):
    """
    Отправляет приветственное сообщение с кнопкой для запуска Mini App.
    """
    markup = types.InlineKeyboardMarkup()
    # URL для Telegram Mini App
    web_app = types.WebAppInfo("t.me/hollyapp_bot/parser")
    button = types.InlineKeyboardButton("Запустить приложение", web_app=web_app)
    markup.add(button)

    bot.send_message(
        message.chat.id,
        "Добро пожаловать! Нажмите кнопку ниже, чтобы запустить наше приложение.",
        reply_markup=markup
    )

if __name__ == '__main__':
    bot.polling(none_stop=True)
