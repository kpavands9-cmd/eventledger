FROM python:3.12-slim

WORKDIR /app

# Copy shared module and gateway package
COPY shared.py .
COPY gateway/ ./gateway/

EXPOSE 8000

ENV PORT=8000
ENV DB_PATH=/data/gateway.db

RUN mkdir -p /data

CMD ["python3", "gateway/main.py"]
