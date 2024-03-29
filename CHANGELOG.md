# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## 16.07.2022
### Added
Support access to non-localhost environments
Create method that allows insering multiple CSV files

### Changed
Fix create Pagelinks MATCH to only match the right pages 

## 26.06.2022
### Added
- support for gds in docker for neo4j
- cypher scripts to generate graphs

## 26.05.2022
### Added
- full support to CREATE data from CSV with tests

### Changed
- migrations from raw are now put in docker file directly

## 24.05.2022
### Added
- APOC plugin to docker compose
- CSV to import page

### Changed
- on split_migration, replaces \' with \" and vice versa for csv parsing. This is done as neo4j does not allow the use of a different QUOTECHAR

## 09.05.2022
### Added
- neo4j scripts for page and pagelink

## 02.05.2022
### Added
- scripts to capture and install requirements
- docker compose of neo4j

## 01.05.2022
### Added
- script to split the giant SQL dumps into readable scv snippets

## 27.04.2022
Created Project
