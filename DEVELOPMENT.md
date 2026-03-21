# Setup for development and testing

Some tests in this library require access to actual Databricks workspaces to verify its file system operations
in the real Databricks environment. You need to configure access to a Databricks workspace and create work
directories within it before running the tests.

## Work directories in Databricks workspace

You need to create work directories in your Databricks workspace and Unity Catalog to use for the tests and
set the **POSIX paths** (without the `dbfs:/` scheme) of the test directories in the following environment variables.

| Location             | Environment variable name               | Default                                                      |
|----------------------|-----------------------------------------|--------------------------------------------------------------|
| Unity Catalog Volume | `FSSPEC_DATABRICKS_VOLUME_TEST_ROOT`    | `/Volumes/fsspec_databricks_test/fsspec_dbx_test/test/local` |
| Workspace files      | `FSSPEC_DATABRICKS_WORKSPACE_TEST_ROOT` | `/fsspec-databricks-test/local`                              |

## Local development

Configure Databricks Unified authentication locally, and set environment variable
`FSSPEC_DATABRICKS_VOLUME_TEST_ROOT` and `FSSPEC_DATABRICKS_WORKSPACE_TEST_ROOT` to specify the
location of work directories to use.

You can set authentication parameters and the environment variables above in a `.env` file
in the project root directory. The test suite loads it automatically via `python-dotenv`.

Run the tests with:

```bash
uv run pytest
```

## GitHub Actions

You need a Databricks service principal that has read-write access to the work directories.

### Environments

The workflows
use [GitHub Actions environments](https://docs.github.com/en/actions/deployment/targeting-different-environments/using-environments-for-deployment)
to scope secrets and variables per Databricks deployment. You must create at least a `default` environment
(used for the default Databricks workspace (e.g. Databricks Free Edition)). To run tests against additional cloud
deployments, create `azure`, `aws`, and/or `gcp` environments as needed.

Each environment must have the following secrets and variables configured:

| Secret name                | Description                                                              |
|----------------------------|--------------------------------------------------------------------------|
| `DATABRICKS_HOST`          | The URL of the Databricks workspace                                      |
| `DATABRICKS_CLIENT_ID`     | The client ID of the Databricks service principal to use for testing     |
| `DATABRICKS_CLIENT_SECRET` | The client secret of the Databricks service principal to use for testing |

| Variable name                           | Description                                                                       |
|-----------------------------------------|-----------------------------------------------------------------------------------|
| `FSSPEC_DATABRICKS_VOLUME_TEST_ROOT`    | The POSIX path of the work directory in Unity Catalog Volume to use for testing.  |
| `FSSPEC_DATABRICKS_WORKSPACE_TEST_ROOT` | The POSIX path of the directory in Databricks Workspace files to use for testing. |

### Repository-level secrets and variables

| Secret name     | Description                                                          |
|-----------------|----------------------------------------------------------------------|
| `CODECOV_TOKEN` | The repository upload token for [Codecov](https://about.codecov.io/) |

| Variable name       | Description                                                                                                                                                                                                   |
|---------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `TEST_ENVIRONMENTS` | A string containing the cloud environments to test against on each push. If the value contains `azure`, `aws`, or `gcp`, the corresponding job will run. Example: `azure gcp` to run both Azure and GCP jobs. |
