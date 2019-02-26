FROM quay.io/keboola/docker-custom-python:latest
MAINTAINER Viktor Sohajek <sohajek.viktor@gmail.com>

CMD ['pip', 'install', 'azure-storage-blob']
COPY . /code/
WORKDIR /data/
CMD ["python", "-u", "/code/main.py"]