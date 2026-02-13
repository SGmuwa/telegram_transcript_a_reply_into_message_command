FROM python:3.14-slim

WORKDIR /app

# ffmpeg + ffprobe
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
  && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY telegram_transcript_a_reply_into_message_command.py /app/telegram_transcript_a_reply_into_message_command.py

ENV PYTHONUNBUFFERED=1

CMD ["python", "/app/telegram_transcript_a_reply_into_message_command.py"]
