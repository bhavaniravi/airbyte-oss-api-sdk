# Airbyte OSS Python SDK

An experimental repo with Python SDK for Airbyte Open-source.

> Not ready for production

## How to Run

1. Have an Airbyte OSS instance running - https://docs.airbyte.com/contributing-to-airbyte/developing-locally
2. Run a postgres Docker for testing. This will be our source/destination(until further integrations are tested) - `docker run -p 5432:5432 postgres:10.4`
3. The setup and teardown is not done yet. So note down workspace id from Airbyte UI and update the `tests/test_source.py` file.
4. Run `test_source_definition` test to get the `source_definition_id` for postgres
5. Set pythonpath `export PYTHONPATH=$PYTHONPATH:$(pwd)`
6. Run the tests - `pytest -s tests/test_source.py`
