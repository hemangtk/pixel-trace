#commiting
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

RUN ln -sf /usr/bin/python3 /usr/bin/python
RUN pip3 install --upgrade pip setuptools wheel

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip3 install --no-cache-dir -r requirements.txt

COPY . /app

# Make entrypoint.sh executable
RUN chmod +x /app/entrypoint.sh

# Fix for RunPod: Remove docker-init if it exists (conflicts with RunPod's init)
RUN rm -f /usr/sbin/docker-init /sbin/docker-init || true

# Serverless mode → worker starts with Python
CMD ["python3", "serverless_worker.py"]
