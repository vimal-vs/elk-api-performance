FROM python:3.10-slim

RUN apt-get update && \
    apt-get install -y cron && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY script.py .
COPY config.py .
COPY requirements.txt .

RUN pip install -r requirements.txt

COPY crontab /etc/cron.d/monthly-cron
RUN chmod 0644 /etc/cron.d/monthly-cron && echo "" >> /etc/cron.d/monthly-cron
RUN crontab /etc/cron.d/monthly-cron

CMD ["cron", "-f"]