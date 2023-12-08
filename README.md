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

// Use Tangy to search for RPMs, by name, that are associated to a specific repository version
versionHref := "/pulp/e1c6bee3/api/v3/repositories/rpm/rpm/018c1c95-4281-76eb-b277-842cbad524f4/versions/1/"
rows, err := t.RpmRepositoryVersionPackageSearch(context.Background(), []string{versionHref}, "ninja")
if err != nil {
return err
}
```
See example.go for a complete example.

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