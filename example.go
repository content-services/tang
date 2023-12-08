package main

import (
	"context"
	"fmt"

	"github.com/content-services/tang/internal/config"
	"github.com/content-services/tang/internal/zestwrapper"
	"github.com/content-services/tang/pkg/tangy"
)

func main() {
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
		fmt.Println(err)
		return
	}

	// Call helper function that creates and syncs a repository
	versionHref, err := CreateRepositoryVersion()
	if err != nil {
		fmt.Println(err)
		return
	}

	// Use Tangy to search for RPMs, by name, that are associated to a specific repository version
	rows, err := t.RpmRepositoryVersionPackageSearch(context.Background(), []string{versionHref}, "ninja")
	if err != nil {
		fmt.Println(err)
		return
	}

	for _, row := range rows {
		fmt.Printf("\nName: %v \nSummary: %v", row.Name, row.Summary)
	}
	fmt.Printf("\n")
}

func CreateRepositoryVersion() (versionHref string, err error) {
	// Create new Pulp API wrapper instance, so we can use it for testing
	rpmZest := zestwrapper.NewRpmZest(context.Background(), config.Server{
		Url:            "http://localhost:8087",
		Username:       "admin",
		Password:       "password",
		StorageType:    "local",
		DownloadPolicy: "on_demand",
	})

	domainName := "example_domain"

	// Create domain and repository, then sync repository, to create a new repository version with rpm packages
	_, err = rpmZest.LookupOrCreateDomain(domainName)
	if err != nil {
		return "", err
	}

	repoHref, remoteHref, err := rpmZest.CreateRepository(domainName, "rpm modular", "https://fixtures.pulpproject.org/rpm-modular/")
	if err != nil {
		return "", err
	}

	syncTask, err := rpmZest.SyncRpmRepository(repoHref, remoteHref)
	if err != nil {
		return "", err
	}

	_, err = rpmZest.PollTask(syncTask)
	if err != nil {
		return "", err
	}

	resp, err := rpmZest.GetRpmRepositoryByName(domainName, "rpm modular")
	if err != nil {
		return "", err
	}
	if resp.LatestVersionHref == nil {
		return "", fmt.Errorf("latest version href is nil")
	}

	return *resp.LatestVersionHref, nil
}
