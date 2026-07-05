# tang

The tangy package provides methods to read from a [pulp](https://pulpproject.org/) database.

## Installation
`go get github.com/content-services/tang`

## Usage
The tangy package is meant to be imported into an existing project that is using pulp. It can be used like this:
```go
// Pulp database configuration information
dbConfig := tangy.Database{
    Name:       "pulp",
    Host:       "localhost",
    Port:       5434,
    User:       "pulp",
    Password:   "password",
    CACertPath: "",
    PoolLimit:  20,
}

// Create new Tangy instance using database config
t, err := tangy.New(dbConfig, tangy.Logger{Enabled: false})
if err != nil {
    return err
}
defer t.Close()

// Use Tangy to list RPMs with pagination for one or more repository versions, with name filtering
versionHref := "/api/pulp/e1c6bee3/api/v3/repositories/rpm/rpm/018c1c95-4281-76eb-b277-842cbad524f4/versions/1/"
rows, err := t.r.tangy.RpmRepositoryVersionPackageList(context.Background(), []string{versionHref}, tangy.RpmListFilters{Name: "kernel"}, tangy.PageOptions{Offset: 100, Limit: 20})
if err != nil {
  return err
}

// Use Tangy to search for RPMs, by name, that are associated to a specific repository version, returning up to the first 100 results
versionHref := "/api/pulp/e1c6bee3/api/v3/repositories/rpm/rpm/018c1c95-4281-76eb-b277-842cbad524f4/versions/1/"
rows, err := t.RpmRepositoryVersionPackageSearch(context.Background(), []string{versionHref}, "bear", 100)
if err != nil {
  return err
}

// Use Tangy to search for RPM Package Groups, by name, that are associated to a specific repository version, returning up to the first 100 results
versionHref := "/api/pulp/e1c6bee3/api/v3/repositories/rpm/rpm/018c1c95-4281-76eb-b277-842cbad524f4/versions/1/"
rows, err := t.RpmRepositoryVersionPackageGroupSearch(context.Background(), []string{versionHref}, "mammals", 100)
if err != nil {
  return err
}

// Use Tangy to search for RPM Environments, by name, that are associated to a specific repository version, returning up to the first 100 results
versionHref := "/api/pulp/e1c6bee3/api/v3/repositories/rpm/rpm/018c1c95-4281-76eb-b277-842cbad524f4/versions/1/"
rows, err := t.RpmRepositoryVersionPackageGroupSearch(context.Background(), []string{versionHref}, "animals", 100)
if err != nil {
  return err
}

// Use Tangy to list Python packages from the latest version of a repository, grouped by name_normalized
repositoryHref := "/api/pulp/default/api/v3/repositories/python/python/018c1c95-4281-76eb-b277-842cbad524f4/"
packages, err := t.PythonPackageList(context.Background(), repositoryHref, tangy.PythonPackageListFilters{Search: "django"}, tangy.PageOptions{Offset: 0, Limit: 10})

// Use Tangy to list Maven packages from the latest version of a repository, grouped by group_id and artifact_id
repositoryHref := "/api/pulp/default/api/v3/repositories/maven/maven/018c1c95-4281-76eb-b277-842cbad524f4/"
response, err := t.MavenPackageList(context.Background(), repositoryHref, tangy.PageOptions{Offset: 0, Limit: 10})
if err != nil {
  return err
}

// Use Tangy to list distribution files for a specific Python package version (filter by name_normalized)
distributions, err := t.PythonDistributionList(context.Background(), repositoryHref, "shelf-reader", "0.1", tangy.PageOptions{Offset: 0, Limit: 10})
if err != nil {
  return err
}

// Use Tangy to get metadata for a specific Python package version (filter by name_normalized)
detail, err := t.PythonPackageGet(context.Background(), repositoryHref, "shelf-reader", "0.1")
if err != nil {
  return err
}

// Use Tangy to list all builds (artifacts) for a specific Maven package version
buildResponse, err := t.MavenBuildList(context.Background(), repositoryHref, "org.xutils", "xutils", "3.8.5", tangy.PageOptions{Offset: 0, Limit: 10})
if err != nil {
  return err
}
```
See example.go for a complete RPM example.

### Python packages

Python support queries the `python_pythonpackagecontent` table. Each row is one installable distribution file (wheel, sdist, etc.).

- **`PythonPackageList`** — lists packages in the latest repository version, grouped by `name_normalized`, with all versions and `latest_versions` (most recent `pulp_created` per version). Supports optional `Search` filter on `name` or `name_normalized`. Pagination is done in SQL.
- **`PythonDistributionList`** — lists distribution files for a given `name_normalized` and `version`. Pagination is done in SQL.
- **`PythonPackageGet`** — returns package metadata for a given `name_normalized` and `version`, plus all other versions available in the repository and all distribution files for that version. Metadata is taken from one representative distribution (sdist preferred). Returns `ErrPythonPackageNotFound` when the package version is not in the repository.

Repository href format:

```
/api/pulp/{domain}/api/v3/repositories/python/python/{uuid}/
```

`PythonDistributionList` filters by `name_normalized` (PEP 503), not the display `name`.

## Developing
To develop for tangy, there are a few more things to know.

### Create your configuration
`$ cp ./configs/config.yaml.example ./configs/config.yaml`

### Connecting to pulp

#### Connect to an existing pulp server
To connect to an existing pulp server, put the corresponding connection information in `configs/config.yaml`.

#### Create a new pulp server
To create a new pulp server, you can use the provided make commands. You will need to have podman & podman-compose (or docker) installed.
The default values provided in config.yaml.example will work with this server.

##### Start containers
`make compose-up`

#### Stop containers
`make compose-down`

#### Clean container volumes
`make compose-clean`

### Testing

#### Unit tests

Unit tests live under `pkg/tangy/` and do not require a running Pulp instance (they cover helpers, response assembly, and the `MockTangy` interface).

```bash
go test ./pkg/tangy/... -v
```

#### Integration tests

Integration tests live under `internal/test/integration/`. They need a running Pulp stack and a `configs/config.yaml` that points at it (see `configs/config.yaml.example`).

1. Start Pulp:

```bash
make compose-up
```

2. Run all integration tests:

```bash
make test-integration
```

Or run a specific suite:

```bash
CONFIG_PATH="$(pwd)/configs/" go test ./internal/test/integration/ -run TestPythonSuite -v
CONFIG_PATH="$(pwd)/configs/" go test ./internal/test/integration/ -run TestRpmSuite -v
```

The Python integration test syncs `shelf-reader` from PyPI into a random domain via the Pulp API, then asserts tangy can read it from the database. Test data is left in the database after a run; use `make compose-clean` to wipe volumes and start fresh.

### Mocking
Tangy also exports a mock interface you can regenerate using the [mockery](https://github.com/vektra/mockery) tool.
