#!/bin/bash

set -e

image_name="aioredis-models:latest"
docker build -t $image_name .
docker run -it --rm -v $PWD/test-reports:/home/test-reports -v $PWD/docs:/home/docs $image_name $*
