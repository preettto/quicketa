# One image for every QuickETA process: API, Kafka consumers and the batch pipeline.
FROM python:3.12-slim

# libgomp1 = OpenMP runtime required by LightGBM
RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /srv
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY tests ./tests
# the trained model is baked into the image so the CD pipeline ships a self-contained service
COPY models ./models
ENV PYTHONUNBUFFERED=1 PYTHONPATH=/srv

# default command is overridden per service in docker-compose.yml
CMD ["python", "-c", "print('QuickETA image - choose a command')"]
