package integration

import (
	"context"
	"math/rand"
	"testing"

	"github.com/content-services/tang/internal/config"
	"github.com/content-services/tang/internal/zestwrapper"
	"github.com/content-services/tang/pkg/tangy"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type RpmSuite struct {
	suite.Suite
	client            *zestwrapper.RpmZest
	tangy             tangy.Tangy
	domainName        string
	remoteHref        string
	repoHref          string
	firstVersionHref  string
	secondVersionHref string
}

const testRepoName = "zoo"
const testRepoURL = "https://rverdile.fedorapeople.org/dummy-repos/comps/repo1/"
const testRepoURLTwo = "https://rverdile.fedorapeople.org/dummy-repos/comps/repo2/"

func (r *RpmSuite) CreateTestRepository(t *testing.T) {
	domainName := RandStringBytes(10)
	r.domainName = domainName

	_, err := r.client.LookupOrCreateDomain(domainName)
	require.NoError(t, err)

	repoHref, remoteHref, err := r.client.CreateRepository(domainName, testRepoName, testRepoURL)
	require.NoError(t, err)

	r.repoHref = repoHref
	r.remoteHref = remoteHref

	syncTask, err := r.client.SyncRpmRepository(repoHref, remoteHref)
	require.NoError(t, err)

	_, err = r.client.PollTask(syncTask)
	require.NoError(t, err)
}

func (r *RpmSuite) UpdateTestRepository(t *testing.T, url string) {
	err := r.client.UpdateRemote(r.remoteHref, url)
	require.NoError(t, err)

	syncTask, err := r.client.SyncRpmRepository(r.repoHref, r.remoteHref)
	require.NoError(t, err)

	_, err = r.client.PollTask(syncTask)
	require.NoError(t, err)
}

func TestRpmSuite(t *testing.T) {
	s := config.Get().Server
	rpmZest := zestwrapper.NewRpmZest(context.Background(), s)

	dbConfig := config.Get().Database
	ta, err := tangy.New(tangy.Database{
		Name:     dbConfig.Name,
		Host:     dbConfig.Host,
		Port:     dbConfig.Port,
		User:     dbConfig.User,
		Password: dbConfig.Password,
	}, tangy.Logger{Enabled: true, Logger: &log.Logger, LogLevel: zerolog.LevelDebugValue})
	require.NoError(t, err)

	r := RpmSuite{}
	r.client = &rpmZest
	r.tangy = ta
	r.CreateTestRepository(t)

	// Get first version href
	resp, err := r.client.GetRpmRepositoryByName(r.domainName, testRepoName)
	require.NoError(t, err)
	require.NotNil(t, resp.LatestVersionHref)
	r.firstVersionHref = *resp.LatestVersionHref

	// Create second repository version
	r.UpdateTestRepository(t, testRepoURLTwo)
	resp, err = r.client.GetRpmRepositoryByName(r.domainName, testRepoName)
	require.NoError(t, err)
	require.NotNil(t, resp.LatestVersionHref)
	r.secondVersionHref = *resp.LatestVersionHref

	suite.Run(t, &r)
}

func (r *RpmSuite) TestRpmRepositoryVersionPackageSearch() {
	firstVersionHref := &r.firstVersionHref
	secondVersionHref := &r.secondVersionHref

	// Search first repository version
	search, err := r.tangy.RpmRepositoryVersionPackageSearch(context.Background(), []string{*firstVersionHref}, "peng", 100)
	assert.NoError(r.T(), err)
	assert.Equal(r.T(), search[0].Name, "penguin")
	search, err = r.tangy.RpmRepositoryVersionPackageSearch(context.Background(), []string{*firstVersionHref}, "bea", 100)
	assert.NoError(r.T(), err)
	assert.Empty(r.T(), search)

	// Search second repository version, should have new package
	search, err = r.tangy.RpmRepositoryVersionPackageSearch(context.Background(), []string{*secondVersionHref}, "peng", 100)
	assert.NoError(r.T(), err)
	assert.Equal(r.T(), search[0].Name, "penguin")
	search, err = r.tangy.RpmRepositoryVersionPackageSearch(context.Background(), []string{*secondVersionHref}, "sto", 100)
	assert.NoError(r.T(), err)
	assert.Empty(r.T(), search)
	search, err = r.tangy.RpmRepositoryVersionPackageSearch(context.Background(), []string{*secondVersionHref}, "bea", 100)
	assert.NoError(r.T(), err)
	assert.Equal(r.T(), search[0].Name, "bear")

	// Re-search the first version, should be the same
	search, err = r.tangy.RpmRepositoryVersionPackageSearch(context.Background(), []string{*firstVersionHref}, "peng", 100)
	assert.NoError(r.T(), err)
	assert.Equal(r.T(), search[0].Name, "penguin")
	search, err = r.tangy.RpmRepositoryVersionPackageSearch(context.Background(), []string{*firstVersionHref}, "bea", 100)
	assert.NoError(r.T(), err)
	assert.Empty(r.T(), search)

	// Search both versions
	search, err = r.tangy.RpmRepositoryVersionPackageSearch(context.Background(), []string{*firstVersionHref, *secondVersionHref}, "e", 100)
	assert.NoError(r.T(), err)
	assert.Len(r.T(), search, 3)
	assert.Equal(r.T(), "bear", search[0].Name)
	assert.Equal(r.T(), "cockateel", search[1].Name)
	assert.Equal(r.T(), "penguin", search[2].Name)

	// Test search limit
	search, err = r.tangy.RpmRepositoryVersionPackageSearch(context.Background(), []string{*secondVersionHref}, "a", 1)
	assert.NoError(r.T(), err)
	assert.Len(r.T(), search, 1)

	// Test search empty list
	search, err = r.tangy.RpmRepositoryVersionPackageSearch(context.Background(), []string{}, "a", 1)
	assert.NoError(r.T(), err)
	assert.Len(r.T(), search, 0)
}

func (r *RpmSuite) TestRpmRepositoryVersionPackageGroupSearch() {
	firstVersionHref := &r.firstVersionHref
	secondVersionHref := &r.secondVersionHref

	// Search first repository version
	search, err := r.tangy.RpmRepositoryVersionPackageGroupSearch(context.Background(), []string{*firstVersionHref}, "bir", 100)
	assert.NoError(r.T(), err)
	assert.Equal(r.T(), search[0].Name, "birds")
	assert.Equal(r.T(), search[0].ID, "birds")
	assert.Equal(r.T(), search[0].Description, "birds")
	assert.ElementsMatch(r.T(), search[0].Packages, []string{"cockateel", "penguin", "stork"})
	search, err = r.tangy.RpmRepositoryVersionPackageGroupSearch(context.Background(), []string{*firstVersionHref}, "mamm", 100)
	assert.NoError(r.T(), err)
	assert.Empty(r.T(), search)

	// Search second repository version, should have new package and removed package
	search, err = r.tangy.RpmRepositoryVersionPackageGroupSearch(context.Background(), []string{*secondVersionHref}, "bir", 100)
	assert.NoError(r.T(), err)
	assert.Equal(r.T(), search[0].Name, "birds")
	assert.ElementsMatch(r.T(), search[0].Packages, []string{"cockateel", "penguin"})
	search, err = r.tangy.RpmRepositoryVersionPackageGroupSearch(context.Background(), []string{*secondVersionHref}, "mamm", 100)
	assert.NoError(r.T(), err)
	assert.ElementsMatch(r.T(), search[0].Packages, []string{"bear", "cat"})

	// Re-search the first version, should be the same
	search, err = r.tangy.RpmRepositoryVersionPackageGroupSearch(context.Background(), []string{*firstVersionHref}, "bir", 100)
	assert.NoError(r.T(), err)
	assert.Equal(r.T(), search[0].Name, "birds")
	assert.ElementsMatch(r.T(), search[0].Packages, []string{"cockateel", "penguin", "stork"})
	search, err = r.tangy.RpmRepositoryVersionPackageGroupSearch(context.Background(), []string{*firstVersionHref}, "mamm", 100)
	assert.NoError(r.T(), err)
	assert.Empty(r.T(), search)

	// Search both versions
	search, err = r.tangy.RpmRepositoryVersionPackageGroupSearch(context.Background(), []string{*firstVersionHref, *secondVersionHref}, "s", 100)
	assert.NoError(r.T(), err)
	assert.ElementsMatch(r.T(), search[0].Packages, []string{"cockateel", "penguin", "stork"})
	assert.ElementsMatch(r.T(), search[1].Packages, []string{"bear", "cat"})

	// Test search limit
	search, err = r.tangy.RpmRepositoryVersionPackageGroupSearch(context.Background(), []string{*firstVersionHref, *secondVersionHref}, "s", 1)
	assert.NoError(r.T(), err)
	assert.Len(r.T(), search, 1)

	// Test search empty list
	search, err = r.tangy.RpmRepositoryVersionPackageGroupSearch(context.Background(), []string{}, "a", 1)
	assert.NoError(r.T(), err)
	assert.Len(r.T(), search, 0)
}

func (r *RpmSuite) TestRpmRepositoryVersionEnvironmentSearch() {
	firstVersionHref := &r.firstVersionHref
	secondVersionHref := &r.secondVersionHref

	// Search first repository version
	search, err := r.tangy.RpmRepositoryVersionEnvironmentSearch(context.Background(), []string{*firstVersionHref}, "avi", 100)
	assert.NoError(r.T(), err)
	assert.Equal(r.T(), "avians", search[0].Name)
	assert.Equal(r.T(), "avians", search[0].ID)
	assert.Equal(r.T(), "avians", search[0].Description)
	search, err = r.tangy.RpmRepositoryVersionEnvironmentSearch(context.Background(), []string{*firstVersionHref}, "ani", 100)
	assert.NoError(r.T(), err)
	assert.Empty(r.T(), search)

	// Search second repository version, should have new package and removed package
	search, err = r.tangy.RpmRepositoryVersionEnvironmentSearch(context.Background(), []string{*secondVersionHref}, "avi", 100)
	assert.NoError(r.T(), err)
	assert.Equal(r.T(), "avians", search[0].Name)
	search, err = r.tangy.RpmRepositoryVersionEnvironmentSearch(context.Background(), []string{*secondVersionHref}, "ani", 100)
	assert.NoError(r.T(), err)
	assert.Equal(r.T(), "animals", search[0].Name)

	// Re-search the first version, should be the same
	search, err = r.tangy.RpmRepositoryVersionEnvironmentSearch(context.Background(), []string{*firstVersionHref}, "avi", 100)
	assert.NoError(r.T(), err)
	assert.Equal(r.T(), search[0].Name, "avians")
	search, err = r.tangy.RpmRepositoryVersionEnvironmentSearch(context.Background(), []string{*firstVersionHref}, "ani", 100)
	assert.NoError(r.T(), err)
	assert.Empty(r.T(), search)

	// Search both versions
	search, err = r.tangy.RpmRepositoryVersionEnvironmentSearch(context.Background(), []string{*firstVersionHref, *secondVersionHref}, "a", 100)
	assert.NoError(r.T(), err)
	assert.Equal(r.T(), 2, len(search))
	assert.Equal(r.T(), "animals", search[0].Name)
	assert.Equal(r.T(), "avians", search[1].Name)

	// Test search limit
	search, err = r.tangy.RpmRepositoryVersionEnvironmentSearch(context.Background(), []string{*firstVersionHref, *secondVersionHref}, "s", 1)
	assert.NoError(r.T(), err)
	assert.Len(r.T(), search, 1)

	// Test search empty list
	search, err = r.tangy.RpmRepositoryVersionEnvironmentSearch(context.Background(), []string{}, "a", 1)
	assert.NoError(r.T(), err)
	assert.Len(r.T(), search, 0)
}

func RandStringBytes(n int) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}
