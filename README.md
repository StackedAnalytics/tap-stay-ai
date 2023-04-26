# tap-stay-ai

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md#singer-specification).

This tap:

- Pulls raw data from [Stay AI Open API](https://docs.stay.ai/)
- Extracts the following resources:
  - Subscriptions
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

## Getting Started
1. Copy config file.
   ```shell
   $ cp sample_config.json config.json
   ```
2. Edit properties in config file.
3. Create python venv.
   ```shell
   $ python -m venv venv
   ```
4. Activate venv.
   ```shell
   $ source venv/bin/activate
   ```
5. Install dependencies.
   ```shell
   $ pip install -r requirements
   ```
6. Install tap locally.
   ```shell
   $ pip install .
   ```

## Using the tap
```shell
$ venv/bin/tap-stay-ai --config config.json
```
