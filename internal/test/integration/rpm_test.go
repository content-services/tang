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
	client     *zestwrapper.RpmZest
	tangy      tangy.Tangy
	domainName string
	remoteHref string
	repoHref   string
}

const testRepoName = "rpm modular"
const testRepoURL = "https://jlsherrill.fedorapeople.org/fake-repos/revision/one/"
const testRepoURLTwo = "https://jlsherrill.fedorapeople.org/fake-repos/revision/two/"

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
	suite.Run(t, &r)
}

func (r *RpmSuite) TestRpmRepositoryVersionPackageSearch() {
	resp, err := r.client.GetRpmRepositoryByName(r.domainName, testRepoName)
	require.NoError(r.T(), err)
	firstVersionHref := resp.LatestVersionHref
	require.NotNil(r.T(), firstVersionHref)

	// Search first repository version
	search, err := r.tangy.RpmRepositoryVersionPackageSearch(context.Background(), []string{*firstVersionHref}, "bea", 100)
	assert.NoError(r.T(), err)
	assert.Equal(r.T(), search[0].Name, "bear")
	search, err = r.tangy.RpmRepositoryVersionPackageSearch(context.Background(), []string{*firstVersionHref}, "cam", 100)
	assert.NoError(r.T(), err)
	assert.Empty(r.T(), search)

	// Create second repository version
	r.UpdateTestRepository(r.T(), testRepoURLTwo)
	resp, err = r.client.GetRpmRepositoryByName(r.domainName, testRepoName)
	require.NoError(r.T(), err)
	secondVersionHref := resp.LatestVersionHref
	require.NotNil(r.T(), secondVersionHref)

	// Search second repository version, should have new package
	search, err = r.tangy.RpmRepositoryVersionPackageSearch(context.Background(), []string{*secondVersionHref}, "bea", 100)
	assert.NoError(r.T(), err)
	assert.Equal(r.T(), search[0].Name, "bear")
	search, err = r.tangy.RpmRepositoryVersionPackageSearch(context.Background(), []string{*secondVersionHref}, "cam", 100)
	assert.NoError(r.T(), err)
	assert.Equal(r.T(), search[0].Name, "camel")

	// Re-search the first version, should be the same
	search, err = r.tangy.RpmRepositoryVersionPackageSearch(context.Background(), []string{*firstVersionHref}, "bea", 100)
	assert.NoError(r.T(), err)
	assert.Equal(r.T(), search[0].Name, "bear")
	search, err = r.tangy.RpmRepositoryVersionPackageSearch(context.Background(), []string{*firstVersionHref}, "cam", 100)
	assert.NoError(r.T(), err)
	assert.Empty(r.T(), search)

	// Search both versions
	search, err = r.tangy.RpmRepositoryVersionPackageSearch(context.Background(), []string{*firstVersionHref, *secondVersionHref}, "a", 100)
	assert.NoError(r.T(), err)
	assert.Len(r.T(), search, 2)
	assert.Equal(r.T(), search[0].Name, "bear")
	assert.Equal(r.T(), search[1].Name, "camel")

	// Create third repository version to remove new package
	r.UpdateTestRepository(r.T(), testRepoURL)
	resp, err = r.client.GetRpmRepositoryByName(r.domainName, testRepoName)
	require.NoError(r.T(), err)
	thirdVersionHref := resp.LatestVersionHref
	require.NotNil(r.T(), thirdVersionHref)

	// Search third repository version, should not have new package
	search, err = r.tangy.RpmRepositoryVersionPackageSearch(context.Background(), []string{*thirdVersionHref}, "bea", 100)
	assert.NoError(r.T(), err)
	assert.Equal(r.T(), search[0].Name, "bear")
	search, err = r.tangy.RpmRepositoryVersionPackageSearch(context.Background(), []string{*thirdVersionHref}, "cam", 100)
	assert.NoError(r.T(), err)
	assert.Empty(r.T(), search)

	// Test search limit
	search, err = r.tangy.RpmRepositoryVersionPackageSearch(context.Background(), []string{*secondVersionHref}, "a", 1)
	assert.NoError(r.T(), err)
	assert.Len(r.T(), search, 1)

	// Test search empty list
	search, err = r.tangy.RpmRepositoryVersionPackageSearch(context.Background(), []string{}, "a", 1)
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
