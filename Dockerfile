FROM apache/airflow:2.7.0-python3.11

# Copy and install Python dependencies
COPY airflow/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

