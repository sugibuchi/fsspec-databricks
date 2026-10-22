# Authentication

`fsspec-databricks` uses Databricks Unified Authentication, through the Databricks Python SDK. Its parameters and
environment variables are in the
[Databricks Python SDK documentation](https://databricks-sdk-py.readthedocs.io/en/latest/authentication.html).

---

## Default

With Databricks Unified Authentication configured, e.g. a default profile, no parameter is needed:

```python
from fsspec_databricks import DatabricksFileSystem

fs = DatabricksFileSystem()
```

---

## Constructor parameters

`DatabricksFileSystem` takes the parameters of the SDK's `WorkspaceClient`:

```python
# Personal access token
fs = DatabricksFileSystem(host=host_url, token=access_token)

# Another profile
fs = DatabricksFileSystem(profile="production")
```

---

## Environment variables

```bash
export DATABRICKS_CONFIG_PROFILE=production
```

```python
fs = DatabricksFileSystem()  # uses the "production" profile
```

---

## fsspec configuration

[fsspec's configuration](https://filesystem-spec.readthedocs.io/en/latest/features.html#configuration) sets default
parameters for the `dbfs` protocol:

```python
import fsspec

fsspec.config.conf["dbfs"] = {"profile": "production"}

fs = DatabricksFileSystem()  # uses the "production" profile
```

Or with environment variables named `FSSPEC_DBFS_<PARAMETER>`, read when fsspec is imported:

```bash
export FSSPEC_DBFS_PROFILE=production
```

---

## `WorkspaceClient`

`DatabricksFileSystem` can use a `WorkspaceClient` of yours, with its authentication:

```python
from databricks.sdk import WorkspaceClient

client = WorkspaceClient(...)

fs = DatabricksFileSystem(client=client)
```

A file system created with a `WorkspaceClient` is generally not picklable, as `WorkspaceClient` objects are not. Use
another way to authenticate where file systems are pickled, e.g. with Dask.
