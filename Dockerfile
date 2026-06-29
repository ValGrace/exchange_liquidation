FROM python:alpine3.12

WORKDIR /app

COPY requirements.txt /app/

RUN pip install -r requirements.txt

COPY . .

CMD ["python", "-u", "main.py"]

