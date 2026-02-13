#!/usr/bin/env python3
# telegram_transcript_a_reply_into_message_command.py
import asyncio
import os
import re
import shlex
import sys
import time
import traceback
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple, List, Set

from loguru import logger
from telethon import TelegramClient, events
from telethon.errors import FloodWaitError, MessageNotModifiedError

from faster_whisper import WhisperModel


APP_NAME = "telegram_transcript_a_reply_into_message_command"

# Уровень логирования: DEBUG по умолчанию (LOG_LEVEL=INFO для продакшена)
LOG_LEVEL = os.getenv("LOG_LEVEL", "DEBUG").upper()
logger.remove()
logger.add(sys.stderr, level=LOG_LEVEL, format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> - <level>{message}</level>")

# Defaults (можно переопределять env-ами и через команду)
DEFAULT_MODEL_NAME = os.getenv("DEFAULT_MODEL_NAME", "large")
DEFAULT_LANG = os.getenv("DEFAULT_LANG", "ru")
LOW_PRIORITY_EDIT_INTERVAL_SECONDS = int(os.getenv("LOW_PRIORITY_EDIT_INTERVAL_SECONDS", "120"))

TEMP_DIR = Path(os.getenv("TEMP_DIR", "./.tmp")).resolve()
MODEL_CACHE_DIR = Path(os.getenv("MODEL_CACHE_DIR", "./.models")).resolve()
SESSION_DIR = Path(os.getenv("SESSION_DIR", "./.session")).resolve()

# Whisper runtime настройки
WHISPER_DEVICE = os.getenv("WHISPER_DEVICE", "cpu")           # cpu / cuda
WHISPER_COMPUTE_TYPE = os.getenv("WHISPER_COMPUTE_TYPE", "int8")  # int8 / float16 / int8_float16 etc.

TELEGRAM_MAX_MESSAGE_LEN = 4096  # безопасно считать 4096


def now_local_str() -> str:
    return datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %z")


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
    """
    if not text:
        return None
    t = text.strip()
    if not (t.startswith("/tr") or t.startswith("/transcription")):
        return None

    # normalize command token
    parts = shlex.split(t)
    cmd = parts[0]
    if cmd not in ("/tr", "/transcription"):
        # allow "/tr@botname" patterns? тут это userbot, но на всякий случай:
        if cmd.startswith("/tr@"):
            cmd = "/tr"
        elif cmd.startswith("/transcription@"):
            cmd = "/transcription"
        else:
            return None

    args = {"cmd": cmd, "model": None, "lang": None}
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
    logger.debug("parsed command: {}", args)
    return args


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


def make_quote_block(text: str) -> str:
    lines = text.strip().splitlines() if text else []
    if not lines:
        return "> "
    return "\n".join(["> " + ln for ln in lines])


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
    """
    def __init__(self, client: TelegramClient, interval_seconds: int):
        self.client = client
        self.interval = max(1, interval_seconds)
        self._pending: Dict[Tuple[int, int], str] = {}
        self._in_queue: Set[Tuple[int, int]] = set()  # ключи, которые уже в _q (не более 1 на сообщение)
        self._cancelled: Set[Tuple[int, int]] = set()  # после high-priority edit — не применять low-priority
        self._q: asyncio.Queue[Tuple[int, int]] = asyncio.Queue()
        self._last_edit_at = 0.0
        self._loop = asyncio.get_running_loop()

    def request(self, chat_id: int, msg_id: int, text: str) -> None:
        key = (chat_id, msg_id)
        self._pending[key] = text
        if key not in self._in_queue:
            self._in_queue.add(key)
            self._q.put_nowait(key)
            logger.debug("scheduler: enqueued low-priority edit chat_id={} msg_id={} text={}", chat_id, msg_id, text)
        else:
            logger.debug("scheduler: updated pending edit chat_id={} msg_id={} text={} (already in queue)", chat_id, msg_id, text)

    def request_threadsafe(self, chat_id: int, msg_id: int, text: str) -> None:
        self._loop.call_soon_threadsafe(self.request, chat_id, msg_id, text)

    def clear_for_message(self, chat_id: int, msg_id: int) -> None:
        """Очистить очередь low-priority для данного сообщения и пометить как отменённое (вызывать перед high-priority edit)."""
        key = (chat_id, msg_id)
        self._pending.pop(key, None)
        self._in_queue.discard(key)
        self._cancelled.add(key)
        logger.debug("scheduler: cleared and cancelled chat_id={} msg_id={}", chat_id, msg_id)

    def clear_for_message_threadsafe(self, chat_id: int, msg_id: int) -> None:
        self._loop.call_soon_threadsafe(self.clear_for_message, chat_id, msg_id)

    async def _safe_edit(self, chat_id: int, msg_id: int, text: str) -> None:
        # Telegram может ругаться на "message not modified"
        try:
            logger.debug("scheduler: editing chat_id={} msg_id={}", chat_id, msg_id)
            await self.client.edit_message(chat_id, msg_id, text)
        except MessageNotModifiedError:
            logger.debug("scheduler: message not modified chat_id={} msg_id={}", chat_id, msg_id)
            return
        except FloodWaitError as e:
            logger.warning("scheduler: FloodWait {}s for chat_id={} msg_id={}", e.seconds, chat_id, msg_id)
            await asyncio.sleep(int(e.seconds) + 1)
            await self.client.edit_message(chat_id, msg_id, text)

    async def run(self) -> None:
        while True:
            key = await self._q.get()
            self._in_queue.discard(key)

            # глобальный rate-limit — спим до момента, когда можно редактировать
            now = time.monotonic()
            wait = self.interval - (now - self._last_edit_at)
            if wait > 0:
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
                logger.debug("scheduler: skip cancelled edit chat_id={} msg_id={}", key[0], key[1])
                continue

            await self._safe_edit(key[0], key[1], text)
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
) -> None:
    if scheduler is not None:
        scheduler.clear_for_message(chat_id, msg_id)
    text_trimmed = text[:TELEGRAM_MAX_MESSAGE_LEN]
    try:
        logger.debug("high_priority edit chat_id={} msg_id={} file={}", chat_id, msg_id, file)
        if file is not None:
            await client.edit_message(chat_id, msg_id, text_trimmed, file=str(file))
        else:
            await client.edit_message(chat_id, msg_id, text_trimmed)
    except MessageNotModifiedError:
        logger.debug("high_priority edit: message not modified chat_id={} msg_id={}", chat_id, msg_id)
        return
    except FloodWaitError as e:
        logger.warning("high_priority edit: FloodWait {}s chat_id={} msg_id={}", e.seconds, chat_id, msg_id)
        await asyncio.sleep(int(e.seconds) + 1)
        if file is not None:
            await client.edit_message(chat_id, msg_id, text_trimmed, file=str(file))
        else:
            await client.edit_message(chat_id, msg_id, text_trimmed)


def format_error(err: Exception) -> str:
    tb = "".join(traceback.format_exception_only(type(err), err)).strip()
    if not tb:
        tb = str(err)
    # чтобы не разнести лимит
    tb = tb[:2000]
    return "🤖 Транскрипция провалена из-за ошибки:\n```\n" + tb + "\n```"


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
) -> None:
    # temp workspace
    TEMP_DIR.mkdir(parents=True, exist_ok=True)
    job_dir = TEMP_DIR / f"job_{chat_id}_{cmd_msg_id}"
    job_dir.mkdir(parents=True, exist_ok=True)
    logger.info("transcription job started chat_id={} cmd_msg_id={} model={} lang_force={} lang_allowed={}", chat_id, cmd_msg_id, model_name, lang_force, lang_allowed)

    src_path = job_dir / "source"
    wav_path = job_dir / "audio.wav"
    txt_path = job_dir / "transcription.txt"

    state = JobState(chat_id=chat_id, cmd_msg_id=cmd_msg_id, stage="download", pct=0, done_ts=None, note=None)

    def low_update():
        scheduler.request_threadsafe(
            state.chat_id,
            state.cmd_msg_id,
            build_progress_text(state.stage, state.pct, state.done_ts, state.note),
        )

    try:
        # 9) high priority: старт скачивания
        logger.debug("job {}: stage download 0%", job_dir.name)
        await safe_edit_high_priority(
            client, chat_id, cmd_msg_id,
            build_progress_text("download", 0, None, None),
            scheduler=scheduler,
        )

        # --- DOWNLOAD ---
        total = getattr(getattr(reply_msg, "file", None), "size", None)
        last_pct = -1

        def dl_progress(current: int, total_bytes: int):
            nonlocal last_pct
            t = total_bytes if total_bytes else total
            if t and t > 0:
                pct = int(min(99, max(0, (current / t) * 100)))
                if pct != last_pct:
                    last_pct = pct
                    state.pct = pct
                    state.note = None
                    low_update()
            else:
                state.pct = None
                state.note = "прогресс неизвестен"
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
        state.done_ts = now_local_str()
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
        state.done_ts = now_local_str()
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
                    nonlocal_state_pct = pct
                    # обновляем state из треда — thread-safe через scheduler
                    if nonlocal_state_pct != last_pct_local:
                        last_pct_local = nonlocal_state_pct
                        scheduler.request_threadsafe(
                            state.chat_id,
                            state.cmd_msg_id,
                            build_progress_text("transcribe", nonlocal_state_pct, None, None),
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
        state.done_ts = now_local_str()
        state.note = None
        low_update()

        # --- FINAL EDIT (high priority) ---
        final_msg = "🤖 Транскрипция:\n" + make_quote_block(text)

        if len(final_msg) <= TELEGRAM_MAX_MESSAGE_LEN:
            logger.info("job {}: sending final message (inline)", job_dir.name)
            await safe_edit_high_priority(client, chat_id, cmd_msg_id, final_msg, scheduler=scheduler)
        else:
            logger.info("job {}: sending final message as file (message too long)", job_dir.name)
            txt_path.write_text(text, encoding="utf-8")
            await safe_edit_high_priority(
                client, chat_id, cmd_msg_id,
                "🤖 Транскрипция прикреплена файлом",
                scheduler=scheduler,
                file=txt_path,
            )

    except Exception as e:
        logger.exception("job chat_id={} cmd_msg_id={} failed: {}", chat_id, cmd_msg_id, e)
        await safe_edit_high_priority(client, chat_id, cmd_msg_id, format_error(e), scheduler=scheduler)
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
    logger.info("waiting for outgoing /tr or /transcription commands")

    scheduler = LowPriorityEditScheduler(client, LOW_PRIORITY_EDIT_INTERVAL_SECONDS)
    logger.debug("low-priority edit interval: {}s", LOW_PRIORITY_EDIT_INTERVAL_SECONDS)
    model_cache = WhisperModelCache()
    asyncio.create_task(scheduler.run())

    @client.on(events.NewMessage(outgoing=True))
    async def handler(event: events.NewMessage.Event):
        logger.debug("outgoing message: chat_id={} msg_id={} text={!r}", event.chat_id, event.message.id, (event.raw_text or "")[:80])
        cmd = parse_command(event.raw_text)
        if not cmd:
            return

        chat_id = event.chat_id
        cmd_msg_id = event.message.id
        logger.info("command received: chat_id={} msg_id={} cmd={}", chat_id, cmd_msg_id, cmd)

        if not event.is_reply:
            await safe_edit_high_priority(
                client, chat_id, cmd_msg_id,
                "🤖 Транскрипция провалена из-за ошибки:\n```\nКоманда должна быть reply на сообщение с медиа\n```",
                scheduler=scheduler,
            )
            return

        reply_msg = await event.get_reply_message()
        if not reply_msg or not getattr(reply_msg, "media", None):
            await safe_edit_high_priority(
                client, chat_id, cmd_msg_id,
                "🤖 Транскрипция провалена из-за ошибки:\n```\nReply-сообщение не содержит медиа\n```",
                scheduler=scheduler,
            )
            return

        model_name = cmd.get("model") or DEFAULT_MODEL_NAME
        lang_force, lang_allowed = normalize_lang(cmd.get("lang"))
        logger.debug("starting transcription task: model={} lang_force={} lang_allowed={}", model_name, lang_force, lang_allowed)

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
            )
        )

    await client.run_until_disconnected()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("stopped")
