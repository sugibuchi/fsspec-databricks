# Authentication

`fsspec-databricks` uses Databricks Unified Authentication provided by Databricks Python SDK.

You can find information about supported authentication parameters and environment variables in
the [Databricks Python SDK documentation](https://databricks-sdk-py.readthedocs.io/en/latest/authentication.html).

---

## Default authentication

If Databricks Unified Authentication is configured, `fsspec-databricks` will pick up credentials from the default
profile. For more, see the above Databricks SDK docs.

```python
from fsspec_databricks import DatabricksFileSystem

fs = DatabricksFileSystem()

with fs.open("dbfs:/Volumes/...") as f:
    ...
```

---

## Via constructor parameters

You can programmatically configure authentication by passing parameters to `DatabricksFileSystem` constructor.

```python
# Authentication with PAT
fs = DatabricksFileSystem(host=host_url, token=access_token)

# Use different profile
fs = DatabricksFileSystem(profile="production")
```

---

## Via environment variables

Or, you can configure authentication via environment variables.

```bash
# Shell
export DATABRICKS_CONFIG_PROFILE=production
```

```python
# Then in Python
fs = DatabricksFileSystem()  # will use the "production" profile
```

---

## By `fsspec` configuration

You can use
the [fsspec's configuration model](https://filesystem-spec.readthedocs.io/en/latest/features.html#configuration) to
configure and persist authentication parameters.

---

## With `WorkspaceClient`

You can create `DatabricksFileSystem` by explicitly setting Databricks SDK's `WorkspaceClient` object.
The created `DatabricksFileSystem` instance will use the authentication configured in the provided `WorkspaceClient`
object.

```python
from databricks.sdk import WorkspaceClient

client = WorkspaceClient(...)
...

fs = DatabricksFileSystem(client=client)
```

Note: a `DatabricksFileSystem` created with a `WorkspaceClient` will generally not be serializable, because
`WorkspaceClient` instances are not serializable. Consider using other configuration methods if you need
serializable filesystem objects.
