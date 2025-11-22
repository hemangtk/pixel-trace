# Dockerfile (GPU runtime)
FROM nvidia/cuda:11.8.0-cudnn8-runtime-ubuntu22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    python3-dev \
    python3-pip \
    libgl1 \
    ffmpeg \
    libjpeg-dev \
    libheif-dev \
    libraw-dev \
    libturbojpeg0-dev \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# set python
RUN ln -sf /usr/bin/python3 /usr/bin/python
RUN pip3 install --upgrade pip setuptools wheel

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip3 install -r /app/requirements.txt

# copy the application code
COPY . /app

ENV MODEL_DIR=/app/models/antelope
ENV SERVICE_ACCOUNT_FILE=/app/serviceAcc.json

# entrypoint
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]
