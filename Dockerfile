FROM python:3.11-slim

# System deps: ffmpeg per Whisper
RUN apt-get update && \
    apt-get install -y --no-install-recommends ffmpeg && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copia solo requirements prima (layer cache)
COPY requirements.txt .

# 1) Aggiorna pip
# 2) Installa torch CPU-only (~250MB vs ~2GB GPU)
# 3) Installa il resto (openai-whisper riuserà torch già presente)
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir torch --index-url https://download.pytorch.org/whl/cpu && \
    pip install --no-cache-dir -r requirements.txt

# Copia il codice sorgente
COPY . .

EXPOSE 8000

CMD uvicorn main:app --host 0.0.0.0 --port $PORT --workers 1
