# Cryptocurrency Futures Data Capture Tool

## Introduction
Async websocket data pipeline written in python to capture public cryptocurrency exchange data. Producer configs (exchanges, symbols, streams etc) are stored in config file. User handles consuming the data. 

## Setup
  - `git clone https://github.com/CannedKilroy/crypto_data_collector.git`
  - `cd crypto-data-collector`
  - `poetry install`
  - `poetry run python -m crypto_data_collector`

## Configuration
An example valid configuration is provided in config/config.yaml
CCXT naming conventions can be found [here](https://docs.ccxt.com/#/?id=contract-naming-conventions)
## Currently:
- Decoupling config from state management, so config structure is not enforced, consistant naming 
## Exmaple
Example script in examples/
