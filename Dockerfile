FROM apache/airflow:2.8.1-python3.11
COPY requirements.txt .
COPY .env .
RUN pip install --no-cache-dir -r requirements.txt