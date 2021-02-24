# aioredis-models

A wrapper over [aioredis](https://github.com/aio-libs/aioredis) that models
[Redis](https://redis.io/) data as simple data structures.

![GitHub](https://img.shields.io/github/license/qcrisw/aioredis-models)
![PyPI](https://img.shields.io/pypi/v/aioredis_models)

![GitHub Workflow Status (event)](https://img.shields.io/github/workflow/status/qcrisw/aioredis-models/quality-check?event=push&label=quality-checks)
[![Documentation Status](https://readthedocs.org/projects/aioredis-models/badge/?version=latest)](https://aioredis-models.readthedocs.io/en/latest/?badge=latest)
[![codecov](https://codecov.io/gh/qcrisw/aioredis-models/branch/main/graph/badge.svg?token=5K5M77QXO5)](https://codecov.io/gh/qcrisw/aioredis-models)
![GitHub Workflow Status](https://img.shields.io/github/workflow/status/qcrisw/aioredis-models/publish-package?label=package-publish)

## Supported data structures

- [x] Keys
- [x] Strings
- [x] Lists
- [x] Hash maps
- [x] Sets
- [x] Double hash maps

## Requirements

- Python 3.6+
- [aioredis](https://github.com/aio-libs/aioredis) requirements

## Documentation

Detailed documentation is available at https://aioredis-models.readthedocs.io/.

## Usage

Construction of all data structures requires at least an `aioredis.Redis` instance
and a key. For example, to create a `RedisString`:

``` python
from aioredis-models import RedisString

redis_string = RedisString(redis, 'my-string')
```

Once a model has been constructed, various functions can be used to interact with it.

``` python
import aioredis
from aioredis-models import RedisString

async def do_work(redis_string: RedisString):
  stored_value = await redis_string.get()
  print(stored_value)
```

## Contributing

The library is currently in very early stages of development and there is a lot of room for growth.
As such, contributions are welcome. To contribute, create a pull request into the `main` branch.
Make sure the tests pass and there are no linting errors. Also, please update documentation, if
needed.

### Testing

The easiest way to run the tests is to execute the [`./test.sh`](./test.sh) script.
This requires Docker to be installed and running. The following can be used to run
without Docker:

``` bash
pip3 install -r requirements.txt
tox
```

### Linting

Similar to testing, linting rules can be run by executing the [`./lint.sh`](./lint.sh) script.
To run without Docker:

``` bash
pip3 install -r requirements.txt
pylint aioredis_models
```

### Documentation

Documentation can get regenerated using [`./generate-docs.sh`](./generate-docs.sh) script.
To run without Docker:

``` bash
pip3 install -r requirements.txt
sphinx-apidoc -f -o docs/source aioredis_models && (cd docs && make html)
```

## License

This library is offered under the MIT license.
