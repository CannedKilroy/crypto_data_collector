# Cryptocurrency Futures Data Capture Tool

## Introduction
Async data pipeline to capture public cryptocurrency exchange data using ccxt.

- Producers get realtime data from exchange streams and push to a queue
- Consumers process the data
- Consumer and producer pipelines manage shutdowns and state tracking

Note: The consumers in this repo are only base classes and examples. You should create your own consumer implementations (write to db, send alerts, etc...)

## Example Usage:
For example usage read through `src/crypto_data_collector/__main__.py`

## Quick Start
  - `git clone https://github.com/CannedKilroy/crypto_data_collector.git`
  - `cd crypto_data_collector`
  - `poetry install`
  - `poetry run python -m crypto_data_collector`

This will use the example configuration in `config/config.yaml`
for the initial exchanges / symbols / streams to watch, create 2
example consumers, and run the pipeline.

## Configuration
An example valid configuration is provided in config/config.yaml
Configuration is decoupled from state management, this is simply
for convience / example usage.

CCXT naming conventions can be found [here](https://docs.ccxt.com/#/?id=contract-naming-conventions)

## Features

Data Producer Status:
  - STAGED: Producer is created but not yet running.
  - RUNNING: When producer is running without error, ie producer is added to the pipeline and implicitly started. 
  - BACKOFF: Producer in exponential backoff due to transient error, likely network error.
  - CANCELLED: Producer explicitly cancelled without error. 
  - ERRORED: Producer stopped due to uncaught error, too many tries on transient error, or error shutting down.

## TODO:
- Pipeline / Producer / Consumer monitoring