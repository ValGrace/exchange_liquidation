FROM eclipse-temurin:17-jdk-noble

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    python3.12 python3.12-dev python3-pip curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN ln -sf /usr/bin/python3.12 /usr/bin/python

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt --break-system-packages
COPY . .
CMD ["python", "-u", "main.py"]