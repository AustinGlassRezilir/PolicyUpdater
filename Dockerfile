FROM python:3.11-slim
WORKDIR /app

# System deps (optional, uncomment if you later need lxml etc.)
# RUN apt-get update && apt-get install -y --no-install-recommends \
#     build-essential libxml2-dev libxslt1-dev && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
# If Main.py is your entry script, keep this; otherwise change to your runner
CMD ["python", "Main.py"]
