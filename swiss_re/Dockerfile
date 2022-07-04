# syntax=docker/dockerfile:1
FROM ubuntu
RUN apt-get update -y
RUN apt-get install -y python3 pip
WORKDIR /app
COPY . .
RUN python3 -m pip install -r requirements.txt
RUN mkdir -p /app/result/
ENTRYPOINT ["python3", "/app/main.py"]