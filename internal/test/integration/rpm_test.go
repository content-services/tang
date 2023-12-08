package integration

import (
	"context"
	"math/rand"
	"testing"

	"github.com/content-services/tang/internal/config"
	"github.com/content-services/tang/internal/zestwrapper"
	"github.com/content-services/tang/pkg/tangy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type RpmSuite struct {
	suite.Suite
	client     *zestwrapper.RpmZest
	tangy      tangy.Tangy
	domainName string
}

const testRepoName = "rpm modular"
const testRepoURL = "https://fixtures.pulpproject.org/rpm-modular/"

func (r *RpmSuite) CreateTestRepository(t *testing.T) {
	domainName := RandStringBytes(10)
	r.domainName = domainName

	_, err := r.client.LookupOrCreateDomain(domainName)
	require.NoError(t, err)

	repoHref, remoteHref, err := r.client.CreateRepository(domainName, testRepoName, testRepoURL)
	require.NoError(t, err)

	syncTask, err := r.client.SyncRpmRepository(repoHref, remoteHref)
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
	}, tangy.Logger{Enabled: false})
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
	versionHref := resp.LatestVersionHref
	require.NotNil(r.T(), versionHref)

	search, err := r.tangy.RpmRepositoryVersionPackageSearch(context.Background(), []string{*versionHref}, "ninja")
	assert.NoError(r.T(), err)
	assert.Equal(r.T(), search[0].Name, "ninja-build")
}

func RandStringBytes(n int) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}
