FROM python:3.12-slim

WORKDIR /app

# Copy shared module and account service package
COPY shared.py .
COPY account_service/ ./account_service/

EXPOSE 8001

ENV PORT=8001
ENV DB_PATH=/data/account_service.db

RUN mkdir -p /data

CMD ["python3", "account_service/main.py"]
