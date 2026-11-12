FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1
WORKDIR /app

COPY transfermarkt-scraper/requirements.txt ./requirements.txt
RUN python -m pip install --no-cache-dir -r requirements.txt

COPY transfermarkt-scraper/ ./
COPY transfermarkt-deploy/run.py ./run.py
COPY transfermarkt-deploy/api_key.py ./api_key.py

EXPOSE 10000
CMD ["python", "run.py"]
