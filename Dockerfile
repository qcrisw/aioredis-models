FROM python:3.9-alpine

LABEL maintainer="QCRI Software Group <qcriswgroup@hbku.edu.qa>"

WORKDIR /home

# Needed by some of the dependencies.
RUN apk add gcc musl-dev make

RUN pip3 install --upgrade pip
COPY /requirements.txt /home
RUN pip3 install -r requirements.txt

COPY . .
