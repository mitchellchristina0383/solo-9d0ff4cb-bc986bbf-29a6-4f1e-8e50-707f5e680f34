FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DATA_DIR=/data \
    HOST=0.0.0.0 \
    PORT=8080

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY app /app/app
COPY run.py /app/run.py
COPY docker/entrypoint.d /entrypoint.d

RUN chmod +x /entrypoint.d/*.sh

EXPOSE 8080

ENTRYPOINT ["/entrypoint.d/10-partitions.sh"]
CMD ["python", "run.py"]
