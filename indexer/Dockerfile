FROM nvidia/cuda:12.2.0-cudnn8-devel-ubuntu22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV CUDA_HOME=/usr/local/cuda
ENV LD_LIBRARY_PATH=/usr/local/cuda/lib64:$LD_LIBRARY_PATH
ENV PATH=/usr/local/cuda/bin:$PATH

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

# COPY requirements
COPY requirements.txt /app/requirements.txt

# IMPORTANT: install ONNXRuntime GPU FIRST so InsightFace doesn't install CPU version
RUN pip3 install --no-cache-dir onnxruntime-gpu==1.18.0

# install all other Python deps
RUN pip3 install --no-cache-dir -r /app/requirements.txt

# copy application code
COPY . /app

# Make entrypoint executable
RUN chmod +x /app/entrypoint.sh

# Fix RunPod: remove docker-init
RUN rm -f /usr/sbin/docker-init /sbin/docker-init || true

CMD ["python3", "serverless_worker.py"]
