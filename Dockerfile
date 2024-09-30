FROM apache/airflow:2.10.2
COPY requirements.txt ./tmp
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r ./tmp/requirements.txt
