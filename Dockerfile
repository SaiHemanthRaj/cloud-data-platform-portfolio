FROM python:3.12-slim
WORKDIR /app
COPY . .
CMD ["python", "-m", "pipeline.ingest", "--source", "data/sample/events.csv"]
