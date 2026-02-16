# telegram_transcript_a_reply_into_message_command

Клиент на **Telethon**, который слушает **исходящие** сообщения вашего аккаунта. Если вы отправляете команду **/tr**, **/ts** или **/transcription** (три команды равнозначны) в ответ (reply) на сообщение с медиа (аудио, голосовое, видео), клиент:

1. Скачивает медиа
2. Конвертирует в WAV и распознаёт речь через **faster-whisper**
3. Редактирует ваше сообщение с командой и подставляет туда готовую транскрипцию

То есть вы отвечаете на голосовое командой `/tr` — и то же сообщение превращается в текст транскрипции.

## Как пользоваться

- В **ответ** на аудио/голосовое/видео отправьте: **/tr**, **/ts** или **/transcription**
- Опционально можно задать параметры:  
  **/tr model=tiny lang=en**  
  **/tr model=large lang=ru tz=Europe/Moscow**

Параметры:

| Параметр | Описание | По умолчанию |
|----------|----------|--------------|
| `model` | Модель Whisper (tiny, base, small, medium, large, turbo и др.) | `large` (или `DEFAULT_MODEL_NAME` из env) |
| `lang` | Язык: один (`ru`) или список допустимых (`ru,en`) | `ru` (или `DEFAULT_LANG`) |
| `tz` | Таймзона для дат в прогрессе (например Europe/Moscow) | `Europe/Moscow` (или `TZ` из env) |

## Запуск

1. **Секреты** — скопируйте `secrets/telegram.env.example` в `secrets/telegram.env` и заполните:
   - `TELEGRAM_API_ID`, `TELEGRAM_API_HASH` — с [my.telegram.org](https://my.telegram.org)
   - `TELEGRAM_PHONE` — номер в формате +79...
   - `TELEGRAM_SESSION_NAME` — имя сессии (например имя пользователя)

2. **Docker (рекомендуется):**
   ```bash
   docker-compose up -d
   ```
   Модели Whisper кэшируются в volume `whisper_models`, сессия — в `telegram_session`. Временные файлы — в tmpfs.

3. **Локально:**  
   `pip install -r requirements.txt`, затем `python telegram_transcript_a_reply_into_message_command.py`  
   Нужны ffmpeg и заполненный `secrets/telegram.env`.

При первом запуске программа может запросить код из Telegram и (при включённой 2FA) пароль — их можно ввести через stdin или в переменной `TELEGRAM_PASSWORD`.

## Переменные окружения (опционально)

- `DEFAULT_MODEL_NAME`, `DEFAULT_LANG`, `TZ` — значения по умолчанию для команды (TZ также используется для дат в логах)
- `LOW_PRIORITY_EDIT_INTERVAL_SECONDS` — интервал обновления прогресса (по умолчанию 120 с)
- `WHISPER_DEVICE` (cpu/cuda), `WHISPER_COMPUTE_TYPE` (int8/float16 и др.)
- `LOG_LEVEL` — уровень логирования (DEBUG по умолчанию)
- `TEMP_DIR`, `MODEL_CACHE_DIR`, `SESSION_DIR` — пути для временных файлов, моделей и сессии
