FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

COPY monitor_opendata.py .
COPY flight_avro_producer.py .
COPY ./avro avro

RUN mkdir -p logs

CMD ["/bin/bash", "-c", "python monitor_opendata.py"]