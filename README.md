# 🛍️ MRKT [@mrkt](https://t.me/mrkt/app?startapp=252453226)

[![Market](https://res.cloudinary.com/dkgz59pmw/image/upload/v1736756459/knpk224-28px-market_ksivis.svg)](https://t.me/MaineMarketBot?start=8HVF7S9K)
[![Channel](https://res.cloudinary.com/dkgz59pmw/image/upload/v1736756459/knpk224-28px-channel_psjoqn.svg)](https://t.me/+vpXdTJ_S3mo0ZjIy)
[![Chat](https://res.cloudinary.com/dkgz59pmw/image/upload/v1736756459/knpk224-28px-chat_ixoikd.svg)](https://t.me/+wWQuct9bljQ0ZDA6)

> **Автоматизированный бот для [@mrkt](https://t.me/mrkt/app?startapp=252453226) в Telegram**

---

## 📑 Оглавление

1. [Описание](#📜-описание)
2. [Ключевые особенности](#🌟-ключевые-особенности)
3. [Установка](#🛠️-установка)
   - [Быстрый старт](#быстрый-старт)
   - [Ручная установка](#ручная-установка)
4. [Настройки](#⚙️-настройки)
5. [Поддержка и донаты](#💰-поддержка-и-донаты)
6. [Контакты](#📞-контакты)
7. [Дисклеймер](#⚠️-дисклеймер)

---

## 📜 Описание

**MRKT bot** — это автоматизированный бот для участия в бесплатных розыгрышах [@mrkt](https://t.me/mrkt/app?startapp=252453226). Поддерживает многопоточность, интеграцию прокси и автоматическое управление игровыми действиями.

---

## 🌟 Ключевые особенности

- 🔄 **Многопоточность** — одновременная работа с несколькими аккаунтами  
- 🔐 **Прокси-поддержка** — безопасная работа через прокси  
- 🎯 **Управление квестами** — автоматическое выполнение заданий  
- 📊 **Статистика** — подробная аналитика сессий  

---

## 🛠️ Установка

### Быстрый старт

```bash
git clone https://github.com/mainiken/mrkt.git
cd mrkt
pip install -r requirements.txt
```

Создайте файл `.env`:

```bash
API_ID=ваш_api_id
API_HASH=ваш_api_hash
```

### Ручная установка

#### Linux

```bash
sudo sh install.sh
python3 -m venv venv
source venv/bin/activate
pip3 install -r requirements.txt
cp .env-example .env
nano .env  # Укажите свои API_ID и API_HASH
python3 main.py
```

#### Windows

```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
copy .env-example .env
python main.py
```

---

## ⚙️ Настройки

| Параметр                  | Значение по умолчанию      | Описание                                 |
|---------------------------|----------------------------|------------------------------------------|
| `API_ID`                 |                            | Идентификатор Telegram API               |
| `API_HASH`               |                            | Хэш Telegram API                         |
| `GLOBAL_CONFIG_PATH`     |                            | Путь к конфигу (по умолчанию — TG_FARM)  |
| `FIX_CERT`               | `False`                    | Исправление SSL-сертификатов             |
| `SESSION_START_DELAY`    | `360`                      | Задержка старта сессии (в секундах)      |
| `REF_ID`                 |                            | ID реферала                              |
| `USE_PROXY`              | `True`                     | Использование прокси                     |
| `SESSIONS_PER_PROXY`     | `1`                        | Кол-во сессий на прокси                  |
| `DISABLE_PROXY_REPLACE`  | `False`                    | Отключение замены прокси                 |
| `BLACKLISTED_SESSIONS`   | `""`                       | Исключённые сессии через запятую         |
| `DEBUG_LOGGING`          | `False`                    | Включить логгинг                         |
| `DEVICE_PARAMS`          | `False`                    | Кастомные параметры устройства           |
| `AUTO_UPDATE`            | `True`                     | Автообновления                           |
| `CHECK_UPDATE_INTERVAL`  | `300`                      | Интервал обновлений (в секундах)         |
| `NOTIFICATION_BOT_TOKEN` |                            | Токен Telegram-бота для уведомлений      |
| `NOTIFICATION_CHAT_ID`   |                            | Chat ID для получения уведомлений         |

### Настройка Telegram-уведомлений

1. **Создайте Telegram-бота**
   - Откройте Telegram и найдите @BotFather
   - Отправьте команду `/newbot` и следуйте инструкциям
   - Скопируйте выданный токен (например, `123456789:ABCDEF...`)

2. **Узнайте свой chat_id**
   - Напишите любое сообщение вашему новому боту
   - Перейдите по ссылке: [https://api.telegram.org/bot<ВАШ_ТОКЕН>/getUpdates](https://api.telegram.org/bot<ВАШ_ТОКЕН>/getUpdates)
   - Найдите в ответе поле `chat":{"id":<ВАШ_CHAT_ID>,...}`

3. **Заполните .env**
   - Откройте файл `.env`
   - Укажите значения:
     ```env
     NOTIFICATION_BOT_TOKEN=ваш_токен_бота
     NOTIFICATION_CHAT_ID=ваш_chat_id
     ```

**Пример:**
```env
NOTIFICATION_BOT_TOKEN=123456789:ABCDEF1234567890abcdef
NOTIFICATION_CHAT_ID=987654321
```

---

## 💰 Поддержка и донаты

Поддержите разработку:

| Валюта        | Адрес |
|---------------|-------|
| **Bitcoin**   | `bc1pfuhstqcwwzmx4y9jx227vxcamldyx233tuwjy639fyspdrug9jjqer6aqe` |
| **Ethereum**  | `0x9c7ee1199f3fe431e45d9b1ea26c136bd79d8b54` |
| **TON**       | `UQBpZGp55xrezubdsUwuhLFvyqy6gldeo-h22OkDk006e1CL` |
| **BNB**       | `0x9c7ee1199f3fe431e45d9b1ea26c136bd79d8b54` |
| **Solana**    | `HXjHPdJXyyddd7KAVrmDg4o8pRL8duVRMCJJF2xU8JbK` |

---

## 📞 Контакты

- Telegram: [Наш канал](https://t.me/+vpXdTJ_S3mo0ZjIy)

---

## ⚠️ Дисклеймер

Программное обеспечение предоставляется «как есть», без каких-либо гарантий. Использование происходит **на ваш страх и риск**.

Разработчик не несет ответственности за:

- Потерю аккаунтов  
- Блокировки Telegram  
- Нарушения правил сторонних сервисов  

Пользуйтесь согласно законодательству вашей страны и правилам сервисов.
