#!/bin/bash

set -e

./run-docker.sh "sphinx-apidoc -f -o docs/source aioredis_models && (cd docs && make html)"
