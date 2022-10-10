# target-bigquery

`target-bigquery` is a Singer target for BigQuery.

Build with the [Meltano Target SDK](https://sdk.meltano.com).

## Capabilities

* `about`
* `stream-maps`
* `schema-flattening`

## Settings
<!--
This section can be created by copy-pasting the CLI output from:

```
target-bigquery --about --format=markdown
```
-->

| Setting             | Required | Default | Description |
|:--------------------|:--------:|:-------:|:------------|
| project_id          | True     | None    | Google project id |
| dataset             | True     | None    | Dataset to load data into |
| location            | False    | None    | Dataset location |
| table_prefix        | False    | None    | Optional prefix to add to table names |
| batch_size          | False    |   10000 | Maximum size of batches when records are streamed in. BATCH messages are not affected by this property. |
| add_record_metadata | False    |    True | Add Singer Data Capture (SDC) metadata to records |
| stream_maps         | False    | None    | Config object for stream maps capability. For more information check out [Stream Maps](https://sdk.meltano.com/en/latest/stream_maps.html). |
| stream_map_config   | False    | None    | User-defined config values to be used within map expressions. |
| flattening_enabled  | False    | None    | 'True' to enable schema flattening and automatically expand nested properties. |
| flattening_max_depth| False    | None    | The max depth to flatten schemas. |

A full list of supported settings and capabilities is available by running: `target-bigquery --about`


### Configure using environment variables

This Singer target will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Google authentication

Google authentication is done by providing a path to a service account JSON file in the environment variable
`GOOGLE_APPLICATION_CREDENTIALS`

## Usage

You can easily run `target-bigquery` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Target Directly

```bash
target-bigquery --version
target-bigquery --help
# Test using the "Carbon Intensity" sample:
tap-carbon-intensity | target-bigquery --config /path/to/target-bigquery-config.json
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `target_bigquery/tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `target-bigquery` CLI interface directly using `poetry run`:

```bash
poetry run target-bigquery --help
```

### Testing with [Meltano](https://meltano.com/)

_**Note:** This target will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd target-bigquery
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke target-bigquery --version
# OR run a test `elt` pipeline with the Carbon Intensity sample tap:
meltano elt tap-carbon-intensity target-bigquery
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the Meltano SDK to
develop your own Singer taps and targets.
