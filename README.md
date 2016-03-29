### Sentinel Metadata Generator

This small library helps with generating sentinel-s3 metadata and upload it to Amazon S3 and/or ElasticSearch

#### Installation

    $ pip install -r requirements.txt

#### Usage

```
    $ python main.py --help

      Usage: main.py [OPTIONS] <operations: choices: s3 | es>

      Options:
        --start TEXT           Start Date. Format: YYYY-MM-DD
        --end TEXT             End Date. Format: YYYY-MM-DD
        --concurrency INTEGER  Process concurrency. Default=20
        --es-host TEXT         Elasticsearch host address
        --es-port INTEGER      Elasticsearch port number
        -v, --verbose
        --help                 Show this message and exit.
```

Example:

    $ python main.py s3 es --start='2016-01-01' --verbose --concurrency=20


### To do:

- Add uploader
