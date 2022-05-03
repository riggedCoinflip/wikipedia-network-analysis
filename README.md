## Description
Networkanalysis of wikipedia

## Requirements

- Docker and python3 installed
- \>200 GB of free space

## Installation

1. `pip install -r requirements.txt`
2. Download from https://dumps.wikimedia.org/enwiki/<date> the DB dumps named 
`enwiki-<date>-page.sql.gz` and `enwiki-<date>-pagelinks.sql.gz`
3. Unzip them. Care: about 100 GB of data.
4. Run `python src/split-migrations.py`. Might take hour or longer - and needs 100 GB as well

## Running the app

```bash
# start docker
$ docker-compose up

```

## Test

```bash
```

## Do on new commit
- `pip freeze > requirements.txt`
- update changelog

## License

This project is [MIT licensed]( LICENSE ).
