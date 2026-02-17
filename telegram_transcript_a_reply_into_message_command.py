#!/usr/bin/env python3
# telegram_transcript_a_reply_into_message_command.py
import asyncio
import json
import os
import re
import shlex
import signal
import sys
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo
from typing import Dict, Optional, Tuple, List, Set, Any

from loguru import logger
from telethon import TelegramClient, events
from telethon.errors import FloodWaitError, MessageEditTimeExpiredError, MessageNotModifiedError
from telethon.tl.functions.messages import EditMessageRequest
from telethon.tl.types import MessageEntityBlockquote

from faster_whisper import WhisperModel


APP_NAME = "telegram_transcript_a_reply_into_message_command"

# Уровень логирования: DEBUG по умолчанию (LOG_LEVEL=INFO для продакшена)
LOG_LEVEL = os.getenv("LOG_LEVEL", "DEBUG").upper()
logger.remove()
logger.add(sys.stderr, level=LOG_LEVEL, format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> - <level>{message}</level>")

# Defaults (можно переопределять env-ами и через команду)
DEFAULT_MODEL_NAME = os.getenv("DEFAULT_MODEL_NAME", "large")
DEFAULT_LANG = os.getenv("DEFAULT_LANG", "ru")
TZ = os.getenv("TZ", "Europe/Moscow")
LOW_PRIORITY_EDIT_INTERVAL_SECONDS = int(os.getenv("LOW_PRIORITY_EDIT_INTERVAL_SECONDS", "120"))
STOP_GRACE_PERIOD_SECONDS = int(os.getenv("STOP_GRACE_PERIOD", "3500"))  # ~ 1 час — ожидание завершения задач при Ctrl+C

TEMP_DIR = Path(os.getenv("TEMP_DIR", "./.tmp")).resolve()
MODEL_CACHE_DIR = Path(os.getenv("MODEL_CACHE_DIR", "./.models")).resolve()
SESSION_DIR = Path(os.getenv("SESSION_DIR", "./.session")).resolve()

# Whisper runtime настройки
WHISPER_DEVICE = os.getenv("WHISPER_DEVICE", "cpu")           # cpu / cuda
WHISPER_COMPUTE_TYPE = os.getenv("WHISPER_COMPUTE_TYPE", "int8")  # int8 / float16 / int8_float16 etc.

TELEGRAM_MAX_MESSAGE_LEN = 4096  # безопасно считать 4096

# Порядок моделей Whisper по качеству (от худшего к лучшему)
MODEL_ORDER = ("tiny", "base", "small", "medium", "turbo", "large")
RESUME_UPGRADE_MAX_AGE_DAYS = 7
RESUME_SEMAPHORE_LIMIT = 3

# Файл подписок для /tr (в SESSION_DIR — переживает docker compose down/up)
TR_SUBSCRIPTIONS_FILE = Path(os.getenv("TR_SUBSCRIPTIONS_FILE", str(SESSION_DIR / "tr_subscriptions.json"))).resolve()

# Ключи подписок по типам медиа
SUBSCRIBE_RECORD_AUDIO = "subscribe_record_audio"  # голосовые
SUBSCRIBE_RECORD_VIDEO = "subscribe_record_video"  # видеосообщения
SUBSCRIBE_AUDIO = "subscribe_audio"                # музыка/аудио
SUBSCRIBE_VIDEO = "subscribe_video"               # видео
SUBSCRIBE_KEYS = (SUBSCRIBE_RECORD_AUDIO, SUBSCRIBE_RECORD_VIDEO, SUBSCRIBE_AUDIO, SUBSCRIBE_VIDEO)


def _parse_bool(v: Optional[str]) -> Optional[bool]:
    if v is None:
        return None
    v = (v or "").strip().lower()
    if v in ("true", "1", "yes", "on"):
        return True
    if v in ("false", "0", "no", "off"):
        return False
    return None


def load_tr_subscriptions() -> Dict[str, Dict[str, Any]]:
    """Загружает подписки из JSON. Ключ — str(chat_id). Значение: флаги SUBSCRIBE_KEYS и опционально "name" — название чата."""
    if not TR_SUBSCRIPTIONS_FILE.exists():
        return {}
    try:
        data = json.loads(TR_SUBSCRIPTIONS_FILE.read_text(encoding="utf-8"))
        chats = data.get("chats", {})
        out = {}
        for cid, sub in chats.items():
            if not isinstance(sub, dict):
                continue
            row = {k: bool(sub.get(k, False)) for k in SUBSCRIBE_KEYS}
            if "name" in sub and isinstance(sub.get("name"), str):
                row["name"] = sub["name"]
            out[str(cid)] = row
        return out
    except Exception as e:
        logger.warning("failed to load tr_subscriptions: {}", e)
        return {}


def save_tr_subscriptions(subscriptions: Dict[str, Dict[str, Any]]) -> None:
    """Сохраняет подписки в JSON (включая название чата "name" для удобства)."""
    TR_SUBSCRIPTIONS_FILE.parent.mkdir(parents=True, exist_ok=True)
    TR_SUBSCRIPTIONS_FILE.write_text(
        json.dumps({"chats": subscriptions}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def now_local_str() -> str:
    return datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %z")


def _chat_display_name(chat_or_dialog) -> str:
    """Название чата для логов: title, first_name или id."""
    if chat_or_dialog is None:
        return "?"
    name = getattr(chat_or_dialog, "title", None) or getattr(chat_or_dialog, "first_name", None) or getattr(chat_or_dialog, "name", None)
    if name:
        return str(name)
    return str(getattr(chat_or_dialog, "id", "?"))


def _msg_date_str(dt: Optional[datetime]) -> str:
    """Дата сообщения для логов в таймзоне TZ (или —)."""
    if not dt:
        return "—"
    try:
        dt_aware = dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        return datetime_in_tz(dt_aware, TZ)
    except Exception:
        return dt.isoformat() if hasattr(dt, "isoformat") else str(dt)


def now_in_tz(tz_name: str) -> str:
    """Текущее время в указанной таймзоне (например Europe/Moscow). При ошибке — Europe/Moscow."""
    try:
        zi = ZoneInfo(tz_name)
        return datetime.now(zi).strftime("%Y-%m-%d %H:%M:%S %z")
    except Exception:
        zi = ZoneInfo(TZ)
        return datetime.now(zi).strftime("%Y-%m-%d %H:%M:%S %z")


def datetime_in_tz(dt: datetime, tz_name: str) -> str:
    """Форматирует datetime в указанной таймзоне."""
    try:
        zi = ZoneInfo(tz_name)
        return dt.astimezone(zi).strftime("%Y-%m-%d %H:%M:%S %z")
    except Exception:
        zi = ZoneInfo(TZ)
        return dt.astimezone(zi).strftime("%Y-%m-%d %H:%M:%S %z")


def load_env_file_if_exists(path: Path) -> None:
    """
    Примитивный загрузчик KEY=VALUE (без кавычек/экранирований).
    """
    if not path.exists():
        logger.debug("env file not found: {}", path)
        return
    logger.debug("loading env from {}", path)
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip()
        v = v.strip()
        if k and k not in os.environ:
            os.environ[k] = v
            logger.debug("env set {} (from file)", k)


def ensure_secrets_example(secrets_dir: Path) -> None:
    secrets_dir.mkdir(parents=True, exist_ok=True)
    example = secrets_dir / "telegram.env.example"
    if not example.exists():
        logger.info("creating example secrets file: {}", example)
        example.write_text(
            "\n".join([
                "# Скопируйте в telegram.env и заполните значения",
                "TELEGRAM_API_ID=123456",
                "TELEGRAM_API_HASH=0123456789abcdef0123456789abcdef",
                "TELEGRAM_PHONE=+31600000000",
                "# Если включена 2FA (пароль в Telegram) — можно задать тут, либо ввести интерактивно:",
                "TELEGRAM_PASSWORD=",
                "# Имя session-файла (будет лежать в SESSION_DIR). Можно указать @username для удобства:",
                "TELEGRAM_SESSION_NAME=SG_Muwa",
                "",
                "# (Опционально) дефолты:",
                "# DEFAULT_MODEL_NAME=large",
                "# DEFAULT_LANG=ru",
                "# TZ=Europe/Moscow",
                "",
                "# (Опционально) whisper runtime:",
                "# WHISPER_DEVICE=cpu",
                "# WHISPER_COMPUTE_TYPE=int8",
                "",
            ]) + "\n",
            encoding="utf-8"
        )


def require_env(keys: List[str]) -> Tuple[bool, List[str]]:
    missing = [k for k in keys if not os.getenv(k)]
    return (len(missing) == 0), missing


def parse_command(text: str) -> Optional[dict]:
    """
    /tr model=large lang=ru,en
    /transcription model=tiny lang=en
    /tr subscribe=True help=True ...
    /tr_show_list — показать список чатов с подпиской
    /tr_show_tasks — показать текущие задания
    """
    if not text:
        return None
    t = text.strip()
    if t.startswith("/tr_show_tasks") or t.startswith("/tr_show_tasks@"):
        return {"cmd": "/tr_show_tasks", "show_tasks": True}
    if t.startswith("/tr_show_list") or t.startswith("/tr_show_list@"):
        args_show: Dict[str, Any] = {"cmd": "/tr_show_list", "show_list": True, "format": "text"}
        try:
            parts_show = shlex.split(t)
            for p in parts_show[1:]:
                if "=" in p:
                    k, v = p.split("=", 1)
                    k, v = k.strip().lower(), v.strip().lower()
                    if k == "format" and v in ("text", "json"):
                        args_show["format"] = v
        except Exception:
            pass
        return args_show
    if not (t.startswith("/tr") or t.startswith("/transcription") or t.startswith("/ts")):
        return None

    parts = shlex.split(t)
    cmd = parts[0]
    if cmd not in ("/tr", "/transcription", "/ts"):
        if cmd.startswith("/tr@"):
            cmd = "/tr"
        elif cmd.startswith("/transcription@"):
            cmd = "/transcription"
        elif cmd.startswith("/ts@"):
            cmd = "/ts"
        else:
            return None

    args: Dict[str, Any] = {
        "cmd": cmd,
        "model": None,
        "lang": None,
        "tz": None,
        "subscribe": None,
        "subscribe_record_audio": None,
        "subscribe_record_video": None,
        "subscribe_audio": None,
        "subscribe_video": None,
        "destruct_message": False,
        "help": False,
    }
    for p in parts[1:]:
        if "=" not in p:
            continue
        k, v = p.split("=", 1)
        k = k.strip().lower()
        v = v.strip()
        if k == "model":
            args["model"] = v
        elif k == "lang":
            args["lang"] = v
        elif k == "tz":
            args["tz"] = v
        elif k == "subscribe":
            args["subscribe"] = _parse_bool(v)
        elif k == "subscribe_record_audio":
            args["subscribe_record_audio"] = _parse_bool(v)
        elif k == "subscribe_record_video":
            args["subscribe_record_video"] = _parse_bool(v)
        elif k == "subscribe_audio":
            args["subscribe_audio"] = _parse_bool(v)
        elif k == "subscribe_video":
            args["subscribe_video"] = _parse_bool(v)
        elif k == "destruct_message":
            args["destruct_message"] = _parse_bool(v) is True
        elif k == "help":
            args["help"] = _parse_bool(v) is True
    logger.debug("parsed command: {}", args)
    return args


def _message_media_type(msg) -> Optional[str]:
    """Тип медиа сообщения: subscribe_record_audio, subscribe_record_video, subscribe_audio, subscribe_video или None."""
    if not getattr(msg, "media", None):
        return None
    if getattr(msg, "voice", None):
        return SUBSCRIBE_RECORD_AUDIO
    if getattr(msg, "video_note", None):
        return SUBSCRIBE_RECORD_VIDEO
    if getattr(msg, "audio", None):
        return SUBSCRIBE_AUDIO
    if getattr(msg, "video", None):
        return SUBSCRIBE_VIDEO
    return None


def get_tr_help_text() -> str:
    return """🤖 Помощь по командам: /tr, /ts, /transcription — три команды, делают одно и то же.

Команды (reply на медиа): /tr, /ts, /transcription

Параметры (key=value):
• model — модель Whisper (tiny, large, turbo…). По умолчанию: large
• lang — язык (ru, en или ru,en). По умолчанию: ru
• tz — таймзона для дат (Europe/Moscow и т.д.). По умолчанию: из env

Подписки и опции (для /tr, /ts, /transcription):
• subscribe=True — подписать чат на все типы медиа (авто /tr на каждое новое медиа)
• subscribe=False — отписать чат
• subscribe_record_audio=True/False — голосовые сообщения
• subscribe_record_video=True/False — видеосообщения
• subscribe_audio=True/False — музыка/аудио
• subscribe_video=True/False — видео
• destruct_message=True — не транскрибировать, удалить отправленное сообщение (удобно для подписки)
• help=True — эта справка (без транскрипции)
• /tr_show_list — список чатов с подпиской (format=text | format=json, по умолчанию text)
• /tr_show_tasks — текущие задания (транскрипция, апгрейд, scheduler и т.д.)"""


# Краткие названия режимов подписки для вывода в /tr_show_list
SUBSCRIBE_KEY_LABELS = {
    SUBSCRIBE_RECORD_AUDIO: "голосовые",
    SUBSCRIBE_RECORD_VIDEO: "видеосообщения",
    SUBSCRIBE_AUDIO: "аудио",
    SUBSCRIBE_VIDEO: "видео",
}


async def fill_missing_chat_names(client: TelegramClient, subscriptions: Dict[str, Dict[str, Any]]) -> None:
    """Для чатов без названия запрашивает его через API и сохраняет обновлённый JSON."""
    updated = False
    for ckey in list(subscriptions.keys()):
        sub = subscriptions[ckey]
        if sub.get("name") and str(sub.get("name", "")).strip():
            continue
        try:
            peer_id = int(ckey)
            entity = await client.get_entity(peer_id)
            name = _chat_display_name(entity)
            if name and name != "?":
                sub["name"] = name
                updated = True
                logger.debug("tr_show_list: filled name for chat_id={}: {}", ckey, name)
        except Exception as e:
            logger.debug("tr_show_list: could not get name for chat_id={}: {}", ckey, e)
    if updated:
        save_tr_subscriptions(subscriptions)


def get_tr_show_list_text(subscriptions: Dict[str, Dict[str, Any]]) -> str:
    """Формирует текст списка чатов с подпиской (для команды /tr_show_list format=text)."""
    if not subscriptions:
        return "Чаты с подпиской на авто-транскрипцию:\n\n(пусто)"
    lines = ["Чаты с подпиской на авто-транскрипцию:", ""]
    for ckey in sorted(subscriptions.keys(), key=lambda x: (subscriptions[x].get("name") or "").lower()):
        sub = subscriptions[ckey]
        name = sub.get("name") or "(без названия)"
        active = [SUBSCRIBE_KEY_LABELS.get(k, k) for k in SUBSCRIBE_KEYS if sub.get(k)]
        mode = ", ".join(active) if active else "—"
        lines.append(f"• {name} (id={ckey})")
        lines.append(f"  Режим: {mode}")
        lines.append("")
    return "\n".join(lines).strip()


def get_tr_show_list_json(subscriptions: Dict[str, Dict[str, Any]]) -> str:
    """Формирует JSON списка чатов с подпиской (для команды /tr_show_list format=json)."""
    return json.dumps({"chats": subscriptions}, ensure_ascii=False, indent=2)


def get_tr_show_tasks_text() -> str:
    """Формирует текст списка текущих заданий (для команды /tr_show_tasks). Вызывать из async-контекста."""
    loop = asyncio.get_running_loop()
    current = asyncio.current_task()
    tasks = [t for t in asyncio.all_tasks(loop) if t is not current and not t.done()]

    def _label(t: asyncio.Task) -> str:
        if hasattr(t, "get_name"):
            name = t.get_name()
            if name:
                return name
        return repr(t.get_coro())

    if not tasks:
        return "Текущие задания:\n\n(нет)"
    lines = ["Текущие задания:", ""]
    for t in sorted(tasks, key=_label):
        lines.append("• " + _label(t))
    return "\n".join(lines)


def normalize_lang(lang_value: Optional[str]) -> Tuple[Optional[str], Optional[List[str]]]:
    """
    Возвращает:
      - language_to_force: str|None (если один язык)
      - allowed_list: list|None (если было ru,en)
    faster_whisper не принимает список — поэтому при нескольких языках включаем автоопределение.
    """
    if not lang_value:
        return DEFAULT_LANG, None

    items = [x.strip() for x in lang_value.split(",") if x.strip()]
    if len(items) == 1:
        return items[0], None
    return None, items


def _utf16_len(s: str) -> int:
    """Длина строки в единицах UTF-16 (для offset/length в Telegram API)."""
    return sum(2 if ord(c) > 0xFFFF else 1 for c in s)


def model_quality_rank(model_name: str) -> int:
    """Индекс модели в MODEL_ORDER (0 = tiny, 5 = large). -1 для неизвестной модели."""
    if not model_name:
        return -1
    name = (model_name or "").strip().lower()
    try:
        return MODEL_ORDER.index(name)
    except ValueError:
        return -1


def parse_transcription_message_model(text: str) -> Optional[str]:
    """
    Извлекает имя модели из текста сообщения транскрипции.
    Если есть «🤖 Транскрипция (model X):» — возвращает X.
    Если только «🤖 Транскрипция:» без (model ...) — считаем small (старая версия).
    Иначе None (не наше сообщение).
    """
    if not text or "🤖 Транскрипция" not in text:
        return None
    match = re.search(r"🤖 Транскрипция\s*\(model\s+(\w+)\)\s*:", text)
    if match:
        return match.group(1).strip().lower()
    if "🤖 Транскрипция:" in text and "(model " not in text:
        return "small"
    return None


def make_transcription_message(text: str, model_name: str) -> Tuple[str, List]:
    """
    Текст финального сообщения с транскрипцией и entities для цитаты в формате Telegram.
    Возвращает (plain_text, entities) для передачи в edit_message(..., entities=entities).
    model_name обязателен и указывается в префиксе.
    """
    prefix = f"🤖 Транскрипция (model {model_name}):\n"
    body = (text or "").strip()
    if not body:
        body = " "
    full_text = prefix + body
    # Blockquote в формате Telegram — одна entity на весь текст транскрипции
    offset = _utf16_len(prefix)
    length = _utf16_len(body)
    entities = [MessageEntityBlockquote(collapsed=True, offset=offset, length=length)]
    return full_text, entities


@dataclass
class JobState:
    chat_id: int
    cmd_msg_id: int
    stage: str  # download/convert/transcribe/done/error
    pct: Optional[int] = None
    done_ts: Optional[str] = None
    note: Optional[str] = None  # e.g. "прогресс неизвестен"


class LowPriorityEditScheduler:
    """
    Глобальный шедулер: low-priority edit выполняется не чаще 1 раза в N секунд (глобально).
    На одно telegram-сообщение в очереди не более одного запроса: новые обновления затирают старые.
    После high-priority edit для сообщения очередь по этому сообщению очищается.
    При передаче shutdown_event цикл run() завершается при срабатывании события (плавное выключение).
    """
    def __init__(
        self,
        client: TelegramClient,
        interval_seconds: int,
        shutdown_event: Optional[asyncio.Event] = None,
    ):
        self.client = client
        self.interval = max(1, interval_seconds)
        self._shutdown_event = shutdown_event
        self._pending: Dict[Tuple[int, int], str] = {}
        self._meta: Dict[Tuple[int, int], Tuple[Optional[str], Optional[str]]] = {}  # (chat_title, msg_date_str) для логов
        self._in_queue: Set[Tuple[int, int]] = set()  # ключи, которые уже в _q (не более 1 на сообщение)
        self._cancelled: Set[Tuple[int, int]] = set()  # после high-priority edit — не применять low-priority
        self._q: asyncio.Queue[Tuple[int, int]] = asyncio.Queue()
        self._last_edit_at = 0.0
        self._loop = asyncio.get_running_loop()

    def request(
        self,
        chat_id: int,
        msg_id: int,
        text: str,
        chat_title: Optional[str] = None,
        msg_date_str: Optional[str] = None,
    ) -> None:
        key = (chat_id, msg_id)
        self._pending[key] = text
        self._meta[key] = (chat_title, msg_date_str)
        chat_label = chat_title or str(chat_id)
        date_label = msg_date_str if msg_date_str is not None else "—"
        if key not in self._in_queue:
            self._in_queue.add(key)
            self._q.put_nowait(key)
            logger.debug(
                "scheduler: enqueued low-priority edit chat_id={} chat={} msg_id={} msg_date={} text={}",
                chat_id, chat_label, msg_id, date_label, text,
            )
        else:
            logger.debug(
                "scheduler: updated pending edit chat_id={} chat={} msg_id={} msg_date={} text={} (already in queue)",
                chat_id, chat_label, msg_id, date_label, text,
            )

    def request_threadsafe(
        self,
        chat_id: int,
        msg_id: int,
        text: str,
        chat_title: Optional[str] = None,
        msg_date_str: Optional[str] = None,
    ) -> None:
        self._loop.call_soon_threadsafe(self.request, chat_id, msg_id, text, chat_title, msg_date_str)

    def clear_for_message(self, chat_id: int, msg_id: int) -> None:
        """Очистить очередь low-priority для данного сообщения и пометить как отменённое (вызывать перед high-priority edit)."""
        key = (chat_id, msg_id)
        self._pending.pop(key, None)
        chat_title, msg_date_str = self._meta.pop(key, (None, None))
        self._in_queue.discard(key)
        self._cancelled.add(key)
        chat_label = chat_title or str(chat_id)
        date_label = msg_date_str if msg_date_str is not None else "—"
        logger.debug("scheduler: cleared and cancelled chat_id={} chat={} msg_id={} msg_date={}", chat_id, chat_label, msg_id, date_label)

    def clear_for_message_threadsafe(self, chat_id: int, msg_id: int) -> None:
        self._loop.call_soon_threadsafe(self.clear_for_message, chat_id, msg_id)

    async def _safe_edit(self, chat_id: int, msg_id: int, text: str, chat_label: str = "", date_label: str = "—") -> None:
        # Telegram может ругаться на "message not modified" или MessageEditTimeExpiredError
        try:
            logger.debug("scheduler: editing chat_id={} chat={} msg_id={} msg_date={}", chat_id, chat_label or chat_id, msg_id, date_label)
            await self.client.edit_message(chat_id, msg_id, text)
        except MessageNotModifiedError:
            logger.debug("scheduler: message not modified chat_id={} chat={} msg_id={} msg_date={}", chat_id, chat_label or chat_id, msg_id, date_label)
            return
        except FloodWaitError as e:
            logger.warning("scheduler: FloodWait {}s for chat_id={} chat={} msg_id={} msg_date={}", e.seconds, chat_id, chat_label or chat_id, msg_id, date_label)
            await asyncio.sleep(int(e.seconds) + 1)
            await self.client.edit_message(chat_id, msg_id, text)
        except Exception as e:
            logger.warning("scheduler: edit failed chat_id={} chat={} msg_id={} msg_date={}: {}", chat_id, chat_label or chat_id, msg_id, date_label, e)
            raise

    async def run(self) -> None:
        while True:
            if self._shutdown_event is not None:
                get_task = asyncio.create_task(self._q.get())
                shutdown_task = asyncio.create_task(self._shutdown_event.wait())
                done, pending = await asyncio.wait(
                    [get_task, shutdown_task],
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if shutdown_task in done:
                    get_task.cancel()
                    try:
                        await get_task
                    except asyncio.CancelledError:
                        pass
                    logger.debug("scheduler: shutdown requested, exiting run loop")
                    return
                key = get_task.result()
            else:
                key = await self._q.get()
            self._in_queue.discard(key)

            # глобальный rate-limit — спим до момента, когда можно редактировать
            now = time.monotonic()
            wait = self.interval - (now - self._last_edit_at)
            if wait > 0:
                if self._shutdown_event is not None:
                    try:
                        await asyncio.wait_for(self._shutdown_event.wait(), timeout=wait)
                        logger.debug("scheduler: shutdown requested during wait, exiting")
                        return
                    except asyncio.TimeoutError:
                        pass
                else:
                    await asyncio.sleep(wait)

            # Забираем текст только после sleep: если за это время был high-priority edit,
            # clear_for_message очистил _pending — не перезаписываем итоговое сообщение
            text = self._pending.pop(key, None)
            if not text:
                continue
            # Даже если текст есть (задача могла вытащить его до clear_for_message),
            # проверяем отмену: после high-priority edit этот ключ в _cancelled
            if key in self._cancelled:
                self._cancelled.discard(key)
                chat_title, msg_date_str = self._meta.pop(key, (None, None))
                chat_label = chat_title or str(key[0])
                date_label = msg_date_str if msg_date_str is not None else "—"
                logger.debug("scheduler: skip cancelled edit chat_id={} chat={} msg_id={} msg_date={}", key[0], chat_label, key[1], date_label)
                continue

            chat_title, msg_date_str = self._meta.pop(key, (None, None))
            chat_label = chat_title or str(key[0])
            date_label = msg_date_str if msg_date_str is not None else "—"
            try:
                await self._safe_edit(key[0], key[1], text, chat_label, date_label)
            except Exception as e:
                logger.warning("scheduler: edit error, skipping message chat_id={} chat={} msg_id={} msg_date={}: {}", key[0], chat_label, key[1], date_label, e)
            else:
                self._last_edit_at = time.monotonic()


class WhisperModelCache:
    def __init__(self):
        self._models: Dict[str, WhisperModel] = {}

    def get(self, model_name: str) -> WhisperModel:
        if model_name not in self._models:
            logger.info("loading whisper model: {} (device={}, compute_type={})", model_name, WHISPER_DEVICE, WHISPER_COMPUTE_TYPE)
            # download_root позволяет кэшировать модели в volume mount
            self._models[model_name] = WhisperModel(
                model_name,
                device=WHISPER_DEVICE,
                compute_type=WHISPER_COMPUTE_TYPE,
                download_root=str(MODEL_CACHE_DIR),
            )
            logger.debug("whisper model loaded: {}", model_name)
        return self._models[model_name]


async def ffprobe_duration_seconds(input_path: Path) -> Optional[float]:
    """
    Возвращает duration в секундах, если возможно.
    Требует ffprobe (в Dockerfile ставим ffmpeg пакет).
    """
    proc = await asyncio.create_subprocess_exec(
        "ffprobe",
        "-v", "error",
        "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1",
        str(input_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    out, _ = await proc.communicate()
    if proc.returncode != 0:
        logger.debug("ffprobe failed for {} returncode={}", input_path, proc.returncode)
        return None
    try:
        dur = float(out.decode("utf-8").strip())
        logger.debug("ffprobe duration for {}: {}s", input_path, dur)
        return dur
    except Exception as e:
        logger.debug("ffprobe parse error for {}: {}", input_path, e)
        return None


async def ffmpeg_convert_to_wav(
    input_path: Path,
    output_path: Path,
    on_progress_pct,  # callable(int|None, note|None)
) -> None:
    """
    Конвертируем любое медиа в WAV 16kHz mono PCM.
    Пытаемся парсить прогресс через -progress pipe:1.
    """
    dur = await ffprobe_duration_seconds(input_path)
    if dur is None or dur <= 0:
        logger.debug("ffmpeg_convert: duration unknown for {}", input_path)
        on_progress_pct(None, "прогресс неизвестен")

    logger.debug("ffmpeg_convert: {} -> {}", input_path, output_path)
    proc = await asyncio.create_subprocess_exec(
        "ffmpeg",
        "-y",
        "-i", str(input_path),
        "-vn",
        "-acodec", "pcm_s16le",
        "-ar", "16000",
        "-ac", "1",
        str(output_path),
        "-progress", "pipe:1",
        "-nostats",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
    )

    last_pct = -1
    if proc.stdout is not None:
        async for raw in proc.stdout:
            line = raw.decode("utf-8", errors="ignore").strip()
            # пример: out_time_ms=12345678
            if line.startswith("out_time_ms=") and dur and dur > 0:
                try:
                    out_time_ms = int(line.split("=", 1)[1])
                    sec = out_time_ms / 1_000_000.0
                    pct = int(min(99, max(0, (sec / dur) * 100)))
                    if pct != last_pct:
                        last_pct = pct
                        on_progress_pct(pct, None)
                except Exception:
                    pass
            elif line.startswith("progress=") and line.endswith("end"):
                on_progress_pct(100, None)

    rc = await proc.wait()
    if rc != 0:
        logger.error("ffmpeg failed exit_code={} {} -> {}", rc, input_path, output_path)
        raise RuntimeError(f"ffmpeg failed with code {rc}")
    logger.debug("ffmpeg_convert done: {}", output_path)


def build_progress_text(stage: str, pct: Optional[int], done_ts: Optional[str], note: Optional[str]) -> str:
    prefix = "/transcription 🤖 Транскрипция: "
    if stage == "download":
        p = "Скачивание медиа"
        ts_label = "Дата завершения скачивания"
    elif stage == "convert":
        p = "Конвертация медиа"
        ts_label = "Дата завершения конвектирования"
    elif stage == "transcribe":
        p = "Извлечение текста"
        ts_label = "Дата завершения"
    else:
        p = stage
        ts_label = "Дата"

    if note:
        # если прогресс неизвестен — именно так и пишем
        body = f"{prefix}{p} ({note})"
        if done_ts:
            body += f"\n{ts_label}: {done_ts}"
        else:
            body += f"\n{ts_label}: —"
        return body

    pct_str = f"{pct}%" if pct is not None else "0%"
    body = f"{prefix}{p} {pct_str}"
    body += f"\n{ts_label}: {done_ts if done_ts else '—'}"
    return body[:TELEGRAM_MAX_MESSAGE_LEN]


async def safe_edit_high_priority(
    client: TelegramClient,
    chat_id: int,
    msg_id: int,
    text: str,
    scheduler: Optional["LowPriorityEditScheduler"] = None,
    file: Optional[Path] = None,
    entities: Optional[List] = None,
    chat_title: Optional[str] = None,
    msg_date_str: Optional[str] = None,
) -> None:
    chat_label = chat_title or str(chat_id)
    date_label = msg_date_str if msg_date_str is not None else "—"
    if scheduler is not None:
        scheduler.clear_for_message(chat_id, msg_id)
    text_trimmed = text[:TELEGRAM_MAX_MESSAGE_LEN]
    try:
        logger.debug(
            "high_priority edit chat_id={} chat={} msg_id={} msg_date={} file={} entities={}, text={}",
            chat_id, chat_label, msg_id, date_label, file, entities is not None, text,
        )
        if entities is not None and file is None:
            # client.edit_message() не принимает entities — используем низкоуровневый API
            peer = await client.get_input_entity(chat_id)
            await client(EditMessageRequest(peer=peer, id=msg_id, message=text_trimmed, entities=entities))
        else:
            edit_kw: dict = {}
            if file is not None:
                edit_kw["file"] = str(file)
            await client.edit_message(chat_id, msg_id, text_trimmed, **edit_kw)
    except MessageNotModifiedError:
        logger.debug("high_priority edit: message not modified chat_id={} chat={} msg_id={} msg_date={}", chat_id, chat_label, msg_id, date_label)
        return
    except FloodWaitError as e:
        logger.warning("high_priority edit: FloodWait {}s chat_id={} chat={} msg_id={} msg_date={}", e.seconds, chat_id, chat_label, msg_id, date_label)
        await asyncio.sleep(int(e.seconds) + 1)
        if entities is not None and file is None:
            peer = await client.get_input_entity(chat_id)
            await client(EditMessageRequest(peer=peer, id=msg_id, message=text_trimmed, entities=entities))
        else:
            edit_kw = {}
            if file is not None:
                edit_kw["file"] = str(file)
            await client.edit_message(chat_id, msg_id, text_trimmed, **edit_kw)
    except Exception as e:
        logger.warning("high_priority edit failed chat_id={} chat={} msg_id={} msg_date={}: {}", chat_id, chat_label, msg_id, date_label, e)
        raise


def format_error(err: Exception) -> str:
    tb = "".join(traceback.format_exception_only(type(err), err)).strip()
    if not tb:
        tb = str(err)
    # чтобы не разнести лимит
    tb = tb[:2000]
    return "🤖 Транскрипция провалена из-за ошибки:\n```\n" + tb + "\n```"


def _text_starts_with_transcription_command(text: str) -> bool:
    """Текст начинается с /transcription, /tr или /ts (с учётом @bot)."""
    if not text:
        return False
    t = text.strip()
    return (
        t.startswith("/transcription") or t.startswith("/tr") or t.startswith("/ts")
        or t.startswith("/transcription@") or t.startswith("/tr@") or t.startswith("/ts@")
    )


def _is_unfinished_transcription_message(text: str) -> bool:
    """Сообщение выглядит как незавершённая транскрипция (прогресс без финального результата)."""
    if not text or "🤖 Транскрипция:" not in text:
        return False
    if "Скачивание медиа" in text or "Конвертация медиа" in text or "Извлечение текста" in text:
        return True
    if "Дата завершения: —" in text:
        return True
    if "%" in text and "🤖 Транскрипция:" in text:
        return True
    return False


def _is_completed_transcription_worse_than_default(text: str) -> bool:
    """Завершённое сообщение транскрипции, у которого модель хуже DEFAULT_MODEL_NAME."""
    if not text or "🤖 Транскрипция" not in text:
        return False
    if "Скачивание медиа" in text or "Конвертация медиа" in text or "Извлечение текста" in text:
        return False
    if "Дата завершения: —" in text:
        return False
    msg_model = parse_transcription_message_model(text)
    if msg_model is None:
        return False
    default_rank = model_quality_rank(DEFAULT_MODEL_NAME)
    msg_rank = model_quality_rank(msg_model)
    if default_rank < 0 or msg_rank < 0:
        return False
    return msg_rank < default_rank


async def process_transcription_job(
    client: TelegramClient,
    scheduler: LowPriorityEditScheduler,
    model_cache: WhisperModelCache,
    chat_id: int,
    cmd_msg_id: int,
    reply_msg,
    model_name: str,
    lang_force: Optional[str],
    lang_allowed: Optional[List[str]],
    tz_name: str,
    is_resume: bool = False,
    chat_title: Optional[str] = None,
    cmd_msg_date: Optional[datetime] = None,
) -> None:
    # temp workspace
    TEMP_DIR.mkdir(parents=True, exist_ok=True)
    job_dir = TEMP_DIR / f"job_{chat_id}_{cmd_msg_id}"
    job_dir.mkdir(parents=True, exist_ok=True)
    chat_label = chat_title or str(chat_id)
    msg_date_str = _msg_date_str(cmd_msg_date)
    logger.info(
        "transcription job started chat_id={} chat={} cmd_msg_id={} cmd_msg_date={} model={} is_resume={}",
        chat_id, chat_label, cmd_msg_id, msg_date_str, model_name, is_resume,
    )

    src_path = job_dir / "source"
    wav_path = job_dir / "audio.wav"
    txt_path = job_dir / "transcription.txt"

    state = JobState(chat_id=chat_id, cmd_msg_id=cmd_msg_id, stage="download", pct=0, done_ts=None, note=None)

    def low_update():
        scheduler.request_threadsafe(
            state.chat_id,
            state.cmd_msg_id,
            build_progress_text(state.stage, state.pct, state.done_ts, state.note),
            chat_label,
            msg_date_str,
        )

    async def try_high_edit(text: str, file: Optional[Path] = None, entities: Optional[List] = None) -> bool:
        """Выполняет high-priority edit. При ошибке редактирования возвращает False (нужно прервать job)."""
        try:
            await safe_edit_high_priority(
                client, chat_id, cmd_msg_id, text,
                scheduler=scheduler, file=file, entities=entities,
                chat_title=chat_label, msg_date_str=msg_date_str,
            )
            return True
        except Exception as e:
            logger.info(
                "message no longer editable, aborting job chat_id={} chat={} msg_id={} msg_date={}: {}",
                chat_id, chat_label, cmd_msg_id, msg_date_str, e,
            )
            return False

    try:
        # high priority: старт скачивания (для resume не делаем первый edit, только low_update при прогрессе)
        logger.debug("job {}: stage download 0%", job_dir.name)
        if not is_resume:
            if not await try_high_edit(build_progress_text("download", 0, None, None)):
                logger.debug("job {}: first edit failed, aborting", job_dir.name)
                return
        else:
            logger.debug("job {}: is_resume=True, skipping first high-priority edit, using low_update only", job_dir.name)
            low_update()

        # --- DOWNLOAD ---
        total = getattr(getattr(reply_msg, "file", None), "size", None)
        last_pct = -1
        download_start = time.monotonic()

        def dl_progress(current: int, total_bytes: int):
            nonlocal last_pct
            t = total_bytes if total_bytes else total
            if t and t > 0:
                pct = int(min(99, max(0, (current / t) * 100)))
                # Предсказание даты завершения по скорости
                done_ts_predicted: Optional[str] = None
                if current > 0:
                    elapsed = time.monotonic() - download_start
                    if elapsed >= 0.5:
                        speed = current / elapsed
                        remaining = t - current
                        if speed > 0:
                            eta_sec = remaining / speed
                            done_ts_predicted = datetime_in_tz(
                                datetime.now().astimezone() + timedelta(seconds=eta_sec),
                                tz_name,
                            )
                state.done_ts = done_ts_predicted
                if pct != last_pct:
                    last_pct = pct
                    state.pct = pct
                    state.note = None
                    low_update()
            else:
                state.pct = None
                state.note = "прогресс неизвестен"
                state.done_ts = None
                low_update()

        downloaded_path = await client.download_media(
            reply_msg,
            file=str(src_path),
            progress_callback=dl_progress
        )
        if not downloaded_path:
            logger.error("job {}: download failed", job_dir.name)
            raise RuntimeError("Не удалось скачать медиа из reply-сообщения")

        logger.debug("job {}: download done -> {}", job_dir.name, downloaded_path)
        state.stage = "download"
        state.pct = 100
        state.done_ts = now_in_tz(tz_name)
        state.note = None
        # переход делаем low-priority (как вы описали)
        low_update()

        # --- CONVERT ---
        logger.debug("job {}: stage convert", job_dir.name)
        state.stage = "convert"
        state.pct = 0
        state.done_ts = None
        state.note = None
        low_update()

        def cvt_progress(pct: Optional[int], note: Optional[str]):
            state.pct = pct
            state.note = note
            low_update()

        await ffmpeg_convert_to_wav(Path(downloaded_path), wav_path, cvt_progress)
        logger.debug("job {}: convert done", job_dir.name)
        state.stage = "convert"
        state.pct = 100
        state.done_ts = now_in_tz(tz_name)
        state.note = None
        low_update()

        # --- TRANSCRIBE ---
        logger.debug("job {}: stage transcribe model={}", job_dir.name, model_name)
        state.stage = "transcribe"
        state.pct = 0
        state.done_ts = None
        state.note = None
        low_update()

        duration = await ffprobe_duration_seconds(wav_path)
        if not duration or duration <= 0:
            duration = None

        transcribe_start = time.monotonic()

        def transcribe_blocking() -> Tuple[str, dict]:
            model = model_cache.get(model_name)
            segments, info = model.transcribe(
                str(wav_path),
                language=lang_force,     # None => auto
                task="transcribe",
                vad_filter=True,
            )
            out_chunks = []
            last_pct_local = -1

            for seg in segments:
                out_chunks.append(seg.text)
                if duration:
                    pct = int(min(99, max(0, (seg.end / duration) * 100)))
                    if pct != last_pct_local:
                        last_pct_local = pct
                        # Предсказание даты завершения транскрипции (как при скачивании)
                        done_ts_predicted: Optional[str] = None
                        if pct > 0:
                            elapsed = time.monotonic() - transcribe_start
                            if elapsed >= 0.5:
                                eta_sec = (100 - pct) / pct * elapsed
                                done_ts_predicted = datetime_in_tz(
                                    datetime.now().astimezone() + timedelta(seconds=eta_sec),
                                    tz_name,
                                )
                        scheduler.request_threadsafe(
                            state.chat_id,
                            state.cmd_msg_id,
                            build_progress_text("transcribe", pct, done_ts_predicted, None),
                            chat_label,
                            msg_date_str,
                        )
            return "".join(out_chunks).strip(), {
                "language": getattr(info, "language", None),
                "language_probability": getattr(info, "language_probability", None),
            }

        text, meta = await asyncio.to_thread(transcribe_blocking)
        logger.debug("job {}: transcribe done detected_lang={} len={}", job_dir.name, meta.get("language"), len(text or ""))

        # если пользователь задавал список языков — проверим detected (если есть)
        detected = meta.get("language")
        if lang_allowed and detected and detected not in lang_allowed:
            # не падаем, просто добавим пометку в конце файла/текста при необходимости
            text = (text + f"\n\n[detected_language={detected} not_in_allowed={','.join(lang_allowed)}]").strip()

        state.stage = "transcribe"
        state.pct = 100
        state.done_ts = now_in_tz(tz_name)
        state.note = None
        low_update()

        # --- FINAL EDIT (high priority) ---
        final_msg, quote_entities = make_transcription_message(text, model_name)

        if len(final_msg) <= TELEGRAM_MAX_MESSAGE_LEN:
            logger.info("job {}: sending final message (inline)", job_dir.name)
            if not await try_high_edit(final_msg, entities=quote_entities):
                logger.debug("job {}: final edit failed, aborting without error message", job_dir.name)
                return
        else:
            logger.info("job {}: sending final message as file (message too long)", job_dir.name)
            txt_path.write_text(text, encoding="utf-8")
            attach_msg, _ = make_transcription_message("(прикреплена файлом)", model_name)
            if not await try_high_edit(attach_msg, file=txt_path):
                logger.debug("job {}: final edit (file) failed, aborting", job_dir.name)
                return
        logger.info(
            "transcription job completed chat_id={} chat={} cmd_msg_id={} cmd_msg_date={} is_resume={}",
            chat_id, chat_label, cmd_msg_id, msg_date_str, is_resume,
        )

    except Exception as e:
        logger.exception(
            "job chat_id={} chat={} cmd_msg_id={} cmd_msg_date={} failed: {}",
            chat_id, chat_label, cmd_msg_id, msg_date_str, e,
        )
        await try_high_edit(format_error(e))
    finally:
        # гарантированная уборка временных файлов/директории
        try:
            for p in job_dir.rglob("*"):
                try:
                    p.unlink()
                except Exception:
                    pass
            try:
                job_dir.rmdir()
            except Exception:
                pass
        except Exception:
            pass
        logger.debug("job {}: temp dir removed", job_dir.name)


async def process_upgrade_job(
    client: TelegramClient,
    scheduler: LowPriorityEditScheduler,
    model_cache: WhisperModelCache,
    chat_id: int,
    cmd_msg_id: int,
    reply_msg,
    chat_title: Optional[str] = None,
    cmd_msg_date: Optional[datetime] = None,
) -> None:
    """
    Улучшение качества: скачать медиа из reply, конвертировать, транскрибировать DEFAULT моделью.
    Редактировать сообщение только один раз в конце готовым результатом (без промежуточных правок).
    """
    TEMP_DIR.mkdir(parents=True, exist_ok=True)
    job_dir = TEMP_DIR / f"upgrade_{chat_id}_{cmd_msg_id}"
    job_dir.mkdir(parents=True, exist_ok=True)
    chat_label = chat_title or str(chat_id)
    msg_date_str = _msg_date_str(cmd_msg_date)
    logger.info(
        "upgrade job started chat_id={} chat={} cmd_msg_id={} cmd_msg_date={}",
        chat_id, chat_label, cmd_msg_id, msg_date_str,
    )

    src_path = job_dir / "source"
    wav_path = job_dir / "audio.wav"
    txt_path = job_dir / "transcription.txt"
    model_name = DEFAULT_MODEL_NAME
    lang_force, lang_allowed = normalize_lang(DEFAULT_LANG)
    tz_name = TZ

    async def try_high_edit(text: str, file: Optional[Path] = None, entities: Optional[List] = None) -> bool:
        try:
            await safe_edit_high_priority(
                client, chat_id, cmd_msg_id, text,
                scheduler=scheduler, file=file, entities=entities,
                chat_title=chat_label, msg_date_str=msg_date_str,
            )
            return True
        except Exception as e:
            logger.info(
                "message no longer editable, aborting upgrade job chat_id={} chat={} msg_id={} msg_date={}: {}",
                chat_id, chat_label, cmd_msg_id, msg_date_str, e,
            )
            return False

    try:
        logger.debug("upgrade job {}: downloading media", job_dir.name)
        downloaded_path = await client.download_media(reply_msg, file=str(src_path))
        if not downloaded_path:
            raise RuntimeError("Не удалось скачать медиа из reply-сообщения")
        logger.debug("upgrade job {}: download done -> {}", job_dir.name, downloaded_path)

        logger.debug("upgrade job {}: converting to wav", job_dir.name)
        await ffmpeg_convert_to_wav(Path(downloaded_path), wav_path, lambda p, n: None)
        logger.debug("upgrade job {}: convert done", job_dir.name)

        logger.debug("upgrade job {}: transcribing with model={}", job_dir.name, model_name)
        def transcribe_blocking() -> Tuple[str, dict]:
            model = model_cache.get(model_name)
            segments, info = model.transcribe(
                str(wav_path),
                language=lang_force,
                task="transcribe",
                vad_filter=True,
            )
            out_chunks = [seg.text for seg in segments]
            return "".join(out_chunks).strip(), {
                "language": getattr(info, "language", None),
                "language_probability": getattr(info, "language_probability", None),
            }

        text, meta = await asyncio.to_thread(transcribe_blocking)
        logger.debug("upgrade job {}: transcribe done len={} detected_lang={}", job_dir.name, len(text or ""), meta.get("language"))

        if lang_allowed and meta.get("language") and meta["language"] not in lang_allowed:
            text = (text + f"\n\n[detected_language={meta['language']} not_in_allowed={','.join(lang_allowed)}]").strip()

        final_msg, quote_entities = make_transcription_message(text, model_name)
        if len(final_msg) <= TELEGRAM_MAX_MESSAGE_LEN:
            logger.info(
                "upgrade job {}: sending final message (inline) chat_id={} chat={} cmd_msg_id={} cmd_msg_date={}",
                job_dir.name, chat_id, chat_label, cmd_msg_id, msg_date_str,
            )
            await try_high_edit(final_msg, entities=quote_entities)
        else:
            logger.info(
                "upgrade job {}: sending final message as file (too long) chat_id={} chat={} cmd_msg_id={} cmd_msg_date={}",
                job_dir.name, chat_id, chat_label, cmd_msg_id, msg_date_str,
            )
            txt_path.write_text(text, encoding="utf-8")
            attach_msg, _ = make_transcription_message("(прикреплена файлом)", model_name)
            await try_high_edit(attach_msg, file=txt_path)
        logger.info("upgrade job completed chat_id={} chat={} cmd_msg_id={} cmd_msg_date={}", chat_id, chat_label, cmd_msg_id, msg_date_str)
    except Exception as e:
        logger.exception(
            "upgrade job chat_id={} chat={} cmd_msg_id={} cmd_msg_date={} failed: {}",
            chat_id, chat_label, cmd_msg_id, msg_date_str, e,
        )
        await try_high_edit(format_error(e))
    finally:
        try:
            for p in job_dir.rglob("*"):
                try:
                    p.unlink()
                except Exception:
                    pass
            try:
                job_dir.rmdir()
            except Exception:
                pass
        except Exception:
            pass
        logger.debug("upgrade job {}: temp dir removed", job_dir.name)


async def startup_scan_and_resume(
    client: TelegramClient,
    scheduler: LowPriorityEditScheduler,
    model_cache: WhisperModelCache,
    shutdown_requested: Optional[List[bool]] = None,
) -> None:
    """
    Фоновая задача при старте: найти за последние 7 дней незавершённые транскрипции и
    завершённые с моделью хуже DEFAULT, поставить их в очередь на дообработку/улучшение.
    Ограничение по одновременным задачам — семафор, чтобы не перегружать Telegram.
    """
    if shutdown_requested and shutdown_requested[0]:
        return
    logger.info(
        "startup scan: starting (max_age_days={}, semaphore_limit={})",
        RESUME_UPGRADE_MAX_AGE_DAYS,
        RESUME_SEMAPHORE_LIMIT,
    )
    cutoff_utc = (datetime.now().astimezone() - timedelta(days=RESUME_UPGRADE_MAX_AGE_DAYS)).astimezone(timezone.utc)
    logger.debug("startup scan: cutoff_utc={}", cutoff_utc.isoformat())
    resume_sem = asyncio.Semaphore(RESUME_SEMAPHORE_LIMIT)
    upgrade_sem = asyncio.Semaphore(RESUME_SEMAPHORE_LIMIT)

    async def run_resume(chat_id: int, cmd_msg_id: int, reply_msg, chat_title: str = "", cmd_msg_date: Optional[datetime] = None) -> None:
        async with resume_sem:
            logger.debug(
                "startup scan: run_resume started chat_id={} chat={} cmd_msg_id={} cmd_msg_date={}",
                chat_id, chat_title or chat_id, cmd_msg_id, _msg_date_str(cmd_msg_date),
            )
            await process_transcription_job(
                client=client,
                scheduler=scheduler,
                model_cache=model_cache,
                chat_id=chat_id,
                cmd_msg_id=cmd_msg_id,
                reply_msg=reply_msg,
                model_name=DEFAULT_MODEL_NAME,
                lang_force=normalize_lang(DEFAULT_LANG)[0],
                lang_allowed=normalize_lang(DEFAULT_LANG)[1],
                tz_name=TZ,
                is_resume=True,
                chat_title=chat_title or None,
                cmd_msg_date=cmd_msg_date,
            )

    async def run_upgrade(chat_id: int, cmd_msg_id: int, reply_msg, chat_title: str = "", cmd_msg_date: Optional[datetime] = None) -> None:
        async with upgrade_sem:
            logger.debug(
                "startup scan: run_upgrade started chat_id={} chat={} cmd_msg_id={} cmd_msg_date={}",
                chat_id, chat_title or chat_id, cmd_msg_id, _msg_date_str(cmd_msg_date),
            )
            await process_upgrade_job(
                client=client,
                scheduler=scheduler,
                model_cache=model_cache,
                chat_id=chat_id,
                cmd_msg_id=cmd_msg_id,
                reply_msg=reply_msg,
                chat_title=chat_title or None,
                cmd_msg_date=cmd_msg_date,
            )

    # (chat_id, cmd_msg_id, reply_id, chat_title, msg_date)
    to_resume: List[Tuple[int, int, int, str, Optional[datetime]]] = []
    to_upgrade: List[Tuple[int, int, int, str, Optional[datetime]]] = []

    try:
        dialogs_scanned = 0
        async for dialog in client.iter_dialogs():
            if not getattr(dialog, "entity", None):
                logger.debug("startup scan: skip dialog (no entity) id={}", getattr(dialog, "id", None))
                continue
            dialogs_scanned += 1
            chat_title = _chat_display_name(dialog)
            try:
                logger.debug("startup scan: iter_messages dialog.entity={} from_user=me limit=200", dialog.entity)
                async for message in client.iter_messages(dialog.entity, from_user="me", limit=200):
                    msg_dt = message.date if message.date and message.date.tzinfo else (message.date.replace(tzinfo=timezone.utc) if message.date else None)
                    if not msg_dt or msg_dt < cutoff_utc:
                        break
                    if not getattr(message, "out", True):
                        continue
                    text = (message.text or message.message or "") if hasattr(message, "text") else (getattr(message, "message", "") or "")
                    if not text.strip():
                        continue
                    reply_id = getattr(message, "reply_to_msg_id", None)
                    if not reply_id:
                        continue
                    has_transcription = "🤖 Транскрипция:" in text or "🤖 Транскрипция (model" in text
                    if _text_starts_with_transcription_command(text) and has_transcription:
                        if _is_unfinished_transcription_message(text):
                            to_resume.append((dialog.id, message.id, reply_id, chat_title, message.date))
                            logger.debug(
                                "startup scan: candidate resume chat_id={} chat={} cmd_msg_id={} cmd_msg_date={} reply_id={}",
                                dialog.id, chat_title, message.id, _msg_date_str(message.date), reply_id,
                            )
                        elif _is_completed_transcription_worse_than_default(text):
                            msg_model = parse_transcription_message_model(text)
                            to_upgrade.append((dialog.id, message.id, reply_id, chat_title, message.date))
                            logger.debug(
                                "startup scan: candidate upgrade chat_id={} chat={} cmd_msg_id={} cmd_msg_date={} reply_id={} (msg_model={} < default={})",
                                dialog.id, chat_title, message.id, _msg_date_str(message.date), reply_id, msg_model, DEFAULT_MODEL_NAME,
                            )
            except Exception as e:
                logger.warning("startup scan: error iterating dialog {}: {}", getattr(dialog, "name", dialog.id), e)
                continue

        logger.info("startup scan: dialogs_scanned={} to_resume={} to_upgrade={}", dialogs_scanned, len(to_resume), len(to_upgrade))

        for chat_id, cmd_msg_id, reply_id, chat_title, cmd_msg_date in to_resume:
            try:
                reply_msg = await client.get_messages(chat_id, ids=reply_id)
                if not reply_msg or not getattr(reply_msg, "media", None):
                    logger.debug(
                        "startup scan: skip resume chat_id={} chat={} cmd_msg_id={} cmd_msg_date={} (no reply or no media)",
                        chat_id, chat_title, cmd_msg_id, _msg_date_str(cmd_msg_date),
                    )
                    continue
                logger.info(
                    "startup scan: scheduling resume chat_id={} chat={} cmd_msg_id={} cmd_msg_date={}",
                    chat_id, chat_title, cmd_msg_id, _msg_date_str(cmd_msg_date),
                )
                asyncio.create_task(
                    run_resume(chat_id, cmd_msg_id, reply_msg, chat_title, cmd_msg_date),
                    name=f"resume_{chat_id}_{cmd_msg_id}",
                )
                await asyncio.sleep(0.1)
            except Exception as e:
                logger.warning(
                    "startup resume: failed to get reply chat_id={} chat={} msg_id={} msg_date={}: {}",
                    chat_id, chat_title, cmd_msg_id, _msg_date_str(cmd_msg_date), e,
                )

        for chat_id, cmd_msg_id, reply_id, chat_title, cmd_msg_date in to_upgrade:
            try:
                reply_msg = await client.get_messages(chat_id, ids=reply_id)
                if not reply_msg or not getattr(reply_msg, "media", None):
                    logger.debug(
                        "startup scan: skip upgrade chat_id={} chat={} cmd_msg_id={} cmd_msg_date={} (no reply or no media)",
                        chat_id, chat_title, cmd_msg_id, _msg_date_str(cmd_msg_date),
                    )
                    continue
                logger.info(
                    "startup scan: scheduling upgrade chat_id={} chat={} cmd_msg_id={} cmd_msg_date={}",
                    chat_id, chat_title, cmd_msg_id, _msg_date_str(cmd_msg_date),
                )
                asyncio.create_task(
                    run_upgrade(chat_id, cmd_msg_id, reply_msg, chat_title, cmd_msg_date),
                    name=f"upgrade_{chat_id}_{cmd_msg_id}",
                )
                await asyncio.sleep(0.1)
            except Exception as e:
                logger.warning(
                    "startup upgrade: failed to get reply chat_id={} chat={} msg_id={} msg_date={}: {}",
                    chat_id, chat_title, cmd_msg_id, _msg_date_str(cmd_msg_date), e,
                )

        logger.info("startup scan: finished (resume_scheduled={} upgrade_scheduled={})", len(to_resume), len(to_upgrade))
    except Exception as e:
        logger.exception("startup_scan_and_resume failed: {}", e)


async def main() -> None:
    # 1) secrets bootstrap
    secrets_dir = Path("./secrets").resolve()
    ensure_secrets_example(secrets_dir)

    # 2) load secrets (dev-friendly): ./secrets/telegram.env
    env_path = secrets_dir / "telegram.env"
    load_env_file_if_exists(env_path)

    required_secrets = ["TELEGRAM_API_ID", "TELEGRAM_API_HASH", "TELEGRAM_PHONE", "TELEGRAM_SESSION_NAME"]
    ok, missing = require_env(required_secrets)
    if not ok:
        logger.warning("missing secrets: {}", ", ".join(missing))
        logger.info("enter values via stdin (one per line) or fill {}", env_path)
        for key in required_secrets:
            if not os.getenv(key):
                logger.info("{}: (waiting stdin)", key)
                try:
                    value = sys.stdin.readline()
                except (EOFError, KeyboardInterrupt):
                    value = ""
                if value is not None:
                    value = value.strip()
                if value:
                    os.environ[key] = value
                    logger.debug("{} set from stdin", key)
        ok, missing = require_env(required_secrets)
        if not ok:
            logger.error("still missing: {}. Exit.", ", ".join(missing))
            return

    api_id = int(os.environ["TELEGRAM_API_ID"])
    api_hash = os.environ["TELEGRAM_API_HASH"]
    phone = os.environ["TELEGRAM_PHONE"]
    tg_password = os.getenv("TELEGRAM_PASSWORD", "")
    session_name = os.environ["TELEGRAM_SESSION_NAME"]

    # prepare dirs
    logger.debug("dirs: MODEL_CACHE={} SESSION={} TEMP={}", MODEL_CACHE_DIR, SESSION_DIR, TEMP_DIR)
    MODEL_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    SESSION_DIR.mkdir(parents=True, exist_ok=True)
    TEMP_DIR.mkdir(parents=True, exist_ok=True)

    session_path = SESSION_DIR / session_name
    logger.debug("session path: {}", session_path)

    client = TelegramClient(str(session_path), api_id, api_hash)
    await client.connect()
    logger.debug("telegram client connected")

    if not await client.is_user_authorized():
        logger.info("authorization required for phone {}", phone)
        await client.send_code_request(phone)
        code = input("Введите код из Telegram: ").strip()
        try:
            await client.sign_in(phone=phone, code=code)
        except Exception:
            # возможно включена 2FA
            if not tg_password:
                tg_password = input("Введите пароль 2FA (если включен): ").strip()
            await client.sign_in(password=tg_password)

    me = await client.get_me()
    username = getattr(me, "username", None)
    logger.info("logged in as @{}", username if username else f"id={me.id}")
    logger.info("waiting for outgoing /tr, /transcription, /ts commands")

    shutdown_requested: List[bool] = [False]
    shutdown_event = asyncio.Event()

    scheduler = LowPriorityEditScheduler(
        client, LOW_PRIORITY_EDIT_INTERVAL_SECONDS, shutdown_event=shutdown_event
    )
    logger.debug("low-priority edit interval: {}s", LOW_PRIORITY_EDIT_INTERVAL_SECONDS)
    model_cache = WhisperModelCache()

    def request_shutdown() -> None:
        shutdown_requested[0] = True
        shutdown_event.set()
        logger.info("shutdown requested, grace_period={}s (no new messages will be processed)", STOP_GRACE_PERIOD_SECONDS)

    loop = asyncio.get_running_loop()
    try:
        loop.add_signal_handler(signal.SIGINT, request_shutdown)
        loop.add_signal_handler(signal.SIGTERM, request_shutdown)
    except NotImplementedError:
        signal.signal(signal.SIGINT, lambda s, f: loop.call_soon_threadsafe(request_shutdown))
        signal.signal(signal.SIGTERM, lambda s, f: loop.call_soon_threadsafe(request_shutdown))

    asyncio.create_task(scheduler.run(), name="scheduler")
    asyncio.create_task(startup_scan_and_resume(client, scheduler, model_cache, shutdown_requested), name="startup_scan")

    tr_subscriptions = load_tr_subscriptions()

    @client.on(events.NewMessage(outgoing=True))
    async def handler(event: events.NewMessage.Event):
        if shutdown_requested[0]:
            return
        chat_title = _chat_display_name(event.chat)
        msg_date_str = _msg_date_str(getattr(event.message, "date", None))
        logger.debug(
            "outgoing message: chat_id={} chat={} msg_id={} msg_date={} text={!r}",
            event.chat_id, chat_title, event.message.id, msg_date_str, (event.raw_text or "")[:80],
        )
        cmd = parse_command(event.raw_text)
        if not cmd:
            return

        chat_id = event.chat_id
        cmd_msg_id = event.message.id
        cmd_msg_date = getattr(event.message, "date", None)
        logger.info(
            "command received: chat_id={} chat={} msg_id={} msg_date={} cmd={}",
            chat_id, chat_title, cmd_msg_id, msg_date_str, cmd,
        )

        if cmd.get("show_list"):
            await fill_missing_chat_names(client, tr_subscriptions)
            fmt = cmd.get("format") or "text"
            if fmt == "json":
                list_text = get_tr_show_list_json(tr_subscriptions)
            else:
                list_text = get_tr_show_list_text(tr_subscriptions)
            await safe_edit_high_priority(
                client, chat_id, cmd_msg_id,
                list_text[:TELEGRAM_MAX_MESSAGE_LEN],
                scheduler=scheduler,
                chat_title=chat_title, msg_date_str=msg_date_str,
            )
            return

        if cmd.get("show_tasks"):
            tasks_text = get_tr_show_tasks_text()
            await safe_edit_high_priority(
                client, chat_id, cmd_msg_id,
                tasks_text[:TELEGRAM_MAX_MESSAGE_LEN],
                scheduler=scheduler,
                chat_title=chat_title, msg_date_str=msg_date_str,
            )
            return

        if cmd.get("help"):
            await safe_edit_high_priority(
                client, chat_id, cmd_msg_id,
                get_tr_help_text()[:TELEGRAM_MAX_MESSAGE_LEN],
                scheduler=scheduler,
                chat_title=chat_title, msg_date_str=msg_date_str,
            )
            return
        sub = cmd.get("subscribe")
        sub_ra = cmd.get("subscribe_record_audio")
        sub_rv = cmd.get("subscribe_record_video")
        sub_a = cmd.get("subscribe_audio")
        sub_v = cmd.get("subscribe_video")
        if sub is not None or sub_ra is not None or sub_rv is not None or sub_a is not None or sub_v is not None:
            ckey = str(chat_id)
            current = dict(tr_subscriptions.get(ckey, {k: False for k in SUBSCRIBE_KEYS}))
            for k in SUBSCRIBE_KEYS:
                if k not in current:
                    current[k] = False
            if sub is True:
                for k in SUBSCRIBE_KEYS:
                    current[k] = True
            elif sub is False:
                for k in SUBSCRIBE_KEYS:
                    current[k] = False
            if sub_ra is not None:
                current[SUBSCRIBE_RECORD_AUDIO] = sub_ra
            if sub_rv is not None:
                current[SUBSCRIBE_RECORD_VIDEO] = sub_rv
            if sub_a is not None:
                current[SUBSCRIBE_AUDIO] = sub_a
            if sub_v is not None:
                current[SUBSCRIBE_VIDEO] = sub_v
            current["name"] = chat_title
            if any(current.get(k, False) for k in SUBSCRIBE_KEYS):
                tr_subscriptions[ckey] = current
            else:
                tr_subscriptions.pop(ckey, None)
            save_tr_subscriptions(tr_subscriptions)
            logger.debug("tr subscriptions updated for chat_id={} chat={}: {}", chat_id, chat_title, current)
        if cmd.get("destruct_message"):
            # Удаляется только одно сообщение — то, что вызвало команду (один чат, один msg_id).
            try:
                await event.message.delete()
                logger.debug(
                    "deleted message chat_id={} chat={} msg_id={} msg_date={}",
                    chat_id, chat_title, cmd_msg_id, msg_date_str,
                )
            except Exception as e:
                logger.warning(
                    "failed to delete message chat_id={} chat={} msg_id={} msg_date={}: {}",
                    chat_id, chat_title, cmd_msg_id, msg_date_str, e,
                )
            return

        if not event.is_reply:
            no_subscribe_update = (
                cmd.get("subscribe") is None
                and cmd.get("subscribe_record_audio") is None
                and cmd.get("subscribe_record_video") is None
                and cmd.get("subscribe_audio") is None
                and cmd.get("subscribe_video") is None
            )
            if no_subscribe_update:
                await safe_edit_high_priority(
                    client, chat_id, cmd_msg_id,
                    "🤖 Транскрипция провалена из-за ошибки:\n```\nКоманда должна быть reply на сообщение с медиа\n```",
                    scheduler=scheduler,
                    chat_title=chat_title, msg_date_str=msg_date_str,
                )
            return

        if shutdown_requested[0]:
            return

        reply_msg = await event.get_reply_message()
        if not reply_msg or not getattr(reply_msg, "media", None):
            return

        model_name = cmd.get("model") or DEFAULT_MODEL_NAME
        lang_force, lang_allowed = normalize_lang(cmd.get("lang"))
        tz_name = cmd.get("tz") or TZ
        logger.debug("starting transcription task: model={} lang_force={} lang_allowed={} tz={}", model_name, lang_force, lang_allowed, tz_name)

        # отдельная задача на обработку, чтобы не блокировать обработчик событий
        asyncio.create_task(
            process_transcription_job(
                client=client,
                scheduler=scheduler,
                model_cache=model_cache,
                chat_id=chat_id,
                cmd_msg_id=cmd_msg_id,
                reply_msg=reply_msg,
                model_name=model_name,
                lang_force=lang_force,
                lang_allowed=lang_allowed,
                tz_name=tz_name,
                chat_title=chat_title,
                cmd_msg_date=cmd_msg_date,
            ),
            name=f"transcription_{chat_id}_{cmd_msg_id}",
        )

    @client.on(events.NewMessage(incoming=True, outgoing=False))
    async def incoming_handler(event: events.NewMessage.Event):
        """В подписанных чатах на новое медиа отправляем сообщение с прогрессом и запускаем транскрипцию (без команды /tr)."""
        if shutdown_requested[0]:
            return
        chat_id = event.chat_id
        chat_title = _chat_display_name(event.chat)
        ckey = str(chat_id)
        if ckey not in tr_subscriptions:
            return
        sub = tr_subscriptions[ckey]
        if not any(sub.get(k, False) for k in SUBSCRIBE_KEYS):
            return
        media_type = _message_media_type(event.message)
        if media_type is None:
            return
        if not sub.get(media_type, False):
            return
        try:
            initial_text = build_progress_text("download", 0, None, None)
            sent_msg = await client.send_message(
                chat_id,
                initial_text,
                reply_to=event.message.id,
                silent=True,
            )
            logger.debug(
                "subscription: sent progress message chat_id={} chat={} cmd_msg_id={} media_type={}",
                chat_id, chat_title, sent_msg.id, media_type,
            )
            asyncio.create_task(
                process_transcription_job(
                    client=client,
                    scheduler=scheduler,
                    model_cache=model_cache,
                    chat_id=chat_id,
                    cmd_msg_id=sent_msg.id,
                    reply_msg=event.message,
                    model_name=DEFAULT_MODEL_NAME,
                    lang_force=normalize_lang(DEFAULT_LANG)[0],
                    lang_allowed=normalize_lang(DEFAULT_LANG)[1],
                    tz_name=TZ,
                    is_resume=False,
                    chat_title=chat_title,
                    cmd_msg_date=getattr(sent_msg, "date", None),
                ),
                name=f"subscription_transcription_{chat_id}_{sent_msg.id}",
            )
        except Exception as e:
            logger.warning("subscription: send or start job failed chat_id={} chat={}: {}", chat_id, chat_title, e)

    async def wait_shutdown_then_disconnect() -> None:
        await shutdown_event.wait()
        logger.info("disconnecting client...")
        await client.disconnect()

    await asyncio.gather(
        client.run_until_disconnected(),
        wait_shutdown_then_disconnect(),
    )

    loop = asyncio.get_running_loop()
    current = asyncio.current_task()
    tasks = [t for t in asyncio.all_tasks(loop) if t is not current and not t.done()]
    if tasks:
        def _task_label(task: asyncio.Task) -> str:
            if hasattr(task, "get_name"):
                name = task.get_name()
                if name:
                    return name
            return repr(task.get_coro())
        task_names = [_task_label(t) for t in tasks]
        names_str = ", ".join(str(n) for n in task_names)
        logger.info(
            "waiting up to {}s for {} task(s) to finish: {}",
            STOP_GRACE_PERIOD_SECONDS, len(tasks), names_str,
        )
        try:
            done, pending = await asyncio.wait(tasks, timeout=STOP_GRACE_PERIOD_SECONDS)
            if pending:
                pending_names = [_task_label(t) for t in pending]
                logger.info("cancelling {} task(s) after grace period: {}", len(pending), ", ".join(pending_names))
                for t in pending:
                    t.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
        except Exception as e:
            logger.warning("graceful wait error: {}", e)
    logger.info("stopped")


if __name__ == "__main__":
    asyncio.run(main())
