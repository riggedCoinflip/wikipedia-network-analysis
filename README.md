## Description
Networkanalysis of wikipedia

## Requirements

- Docker and python3 installed
- About 250 GB of free space

## Installation

1. run `python src/install-requirements.py`to install requirements
2. Download from https://dumps.wikimedia.org/enwiki/<date> the DB dumps named 
`enwiki-<date>-page.sql.gz` and `enwiki-<date>-pagelinks.sql.gz`
3. Unzip them. Care: about 100 GB of data.
4. Run `python src/split-migrations.py`

## Running the app

```bash
# start docker
$ docker-compose up

# development
$ npm run start

# watch mode
$ npm run start:dev

# production mode
$ npm run start:prod
```

## Test

```bash
# unit tests
$ npm run test

# e2e tests
$ npm run test:e2e

# test coverage
$ npm run test:cov
```

## License

This project is [MIT licensed](LICENSE).
