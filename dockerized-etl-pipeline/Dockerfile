FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY etl/ ./etl/
COPY data/ ./data/

RUN mkdir -p /app/output

ENV PYTHONUNBUFFERED=1

CMD ["python", "etl/pipeline.py"]
