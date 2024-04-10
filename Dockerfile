FROM prefecthq/prefect:2-python3.12
COPY requirements.txt .
RUN pip install -r requirements.txt --trusted-host pypi.python.org --no-cache-dir

COPY transactions.py .
COPY eod_historical.py .
COPY ./misc/CustomLogging.py .

CMD ["python", "./transactions.py","./eod_historical.py"]