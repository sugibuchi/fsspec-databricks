# Security policy

## Supported versions

Security fixes are made in the latest release only.

## Reporting a vulnerability

Report vulnerabilities privately, through
[GitHub's private vulnerability reporting](https://github.com/sugibuchi/fsspec-databricks/security/advisories/new).
Do not open a public issue.

Include:

- the version of `fsspec-databricks`, and of `databricks-sdk`;
- the steps to reproduce, or a proof of concept;
- the impact, e.g. leaked credentials or access to files beyond the user's permissions.

Never include real credentials, tokens or workspace URLs in a report.

## Scope

`fsspec-databricks` accesses Databricks with the credentials of its user, through the Databricks SDK. In scope are,
for example:

- credentials or tokens written to logs, exceptions or pickled file systems;
- access to files or paths beyond what the given path names;
- requests sent to hosts other than the workspace, its storage proxy, or the URLs Databricks returns.

Vulnerabilities of Databricks itself or of the Databricks SDK go to Databricks.
