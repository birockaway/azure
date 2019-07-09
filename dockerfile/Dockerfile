FROM quay.io/keboola/docker-custom-python:latest
MAINTAINER Viktor Sohajek <sohajek.viktor@gmail.com>

RUN ["pip", "install", "azure-storage-blob"]
RUN git clone https://github.com/birockaway/azure.git ./ #v1.3.2
WORKDIR /data/
CMD ["python","/home/main.py"]