package integration

import (
	"context"
	"strings"
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

const (
	testNpmRepoName    = "is-odd-fixture"
	testNpmPackageName = "is-odd"
	testNpmVersion     = "3.0.1"
	testNpmRemoteURL   = "https://registry.npmjs.org/is-odd/3.0.1"
)

type NpmSuite struct {
	suite.Suite
	client         *zestwrapper.NpmZest
	tangy          tangy.Tangy
	domainName     string
	repositoryHref string
}

func (n *NpmSuite) createTestRepository(t *testing.T) {
	_, err := n.client.LookupOrCreateDomain(n.domainName)
	require.NoError(t, err)

	repoHref, remoteHref, err := n.client.CreateRepository(
		n.domainName,
		testNpmRepoName,
		testNpmRemoteURL,
	)
	require.NoError(t, err)

	syncTask, err := n.client.SyncNpmRepository(repoHref, remoteHref)
	require.NoError(t, err)

	_, err = n.client.PollTask(syncTask)
	require.NoError(t, err)

	n.repositoryHref = repoHref
}

func TestNpmSuite(t *testing.T) {
	s := config.Get().Server
	npmZest := zestwrapper.NewNpmZest(context.Background(), s)

	dbConfig := config.Get().Database
	ta, err := tangy.New(tangy.Database{
		Name:     dbConfig.Name,
		Host:     dbConfig.Host,
		Port:     dbConfig.Port,
		User:     dbConfig.User,
		Password: dbConfig.Password,
	}, tangy.Logger{Enabled: true, Logger: &log.Logger, LogLevel: zerolog.LevelDebugValue})
	require.NoError(t, err)
	t.Cleanup(ta.Close)

	n := NpmSuite{}
	n.client = &npmZest
	n.tangy = ta
	n.domainName = RandStringBytes(10)

	n.createTestRepository(t)

	suite.Run(t, &n)
}

func (n *NpmSuite) TestNpmPackageList() {
	response, err := n.tangy.NpmPackageList(context.Background(), n.repositoryHref, tangy.NpmPackageListFilters{Search: testNpmPackageName}, tangy.PageOptions{
		Offset: 0,
		Limit:  10,
	})
	require.NoError(n.T(), err)
	require.NotEmpty(n.T(), response.Results)
	assert.Equal(n.T(), 1, response.Total)

	pkg := response.Results[0]
	assert.Equal(n.T(), testNpmPackageName, pkg.Name)
	assert.Contains(n.T(), pkg.Versions, testNpmVersion)
	require.NotEmpty(n.T(), pkg.LatestVersions)

	foundVersion := false
	for _, latest := range pkg.LatestVersions {
		if latest.Version == testNpmVersion {
			foundVersion = true
			assert.NotEmpty(n.T(), latest.CreatedAt)
		}
	}
	assert.True(n.T(), foundVersion)
}

func (n *NpmSuite) TestNpmPackageListSearchFilter() {
	response, err := n.tangy.NpmPackageList(context.Background(), n.repositoryHref, tangy.NpmPackageListFilters{Search: "is-odd"}, tangy.PageOptions{
		Offset: 0,
		Limit:  10,
	})
	require.NoError(n.T(), err)
	require.NotEmpty(n.T(), response.Results)
	assert.Equal(n.T(), 1, response.Total)
	assert.Equal(n.T(), testNpmPackageName, response.Results[0].Name)

	response, err = n.tangy.NpmPackageList(context.Background(), n.repositoryHref, tangy.NpmPackageListFilters{Search: "nonexistent-package"}, tangy.PageOptions{
		Offset: 0,
		Limit:  10,
	})
	require.NoError(n.T(), err)
	assert.Empty(n.T(), response.Results)
	assert.Zero(n.T(), response.Total)
}

func (n *NpmSuite) TestNpmPackageListPagination() {
	response, err := n.tangy.NpmPackageList(context.Background(), n.repositoryHref, tangy.NpmPackageListFilters{Search: testNpmPackageName}, tangy.PageOptions{
		Offset: 0,
		Limit:  1,
	})
	require.NoError(n.T(), err)
	assert.Len(n.T(), response.Results, 1)
	assert.Equal(n.T(), 1, response.Total)
	assert.Equal(n.T(), 1, response.Limit)

	response, err = n.tangy.NpmPackageList(context.Background(), n.repositoryHref, tangy.NpmPackageListFilters{Search: testNpmPackageName}, tangy.PageOptions{
		Offset: 1,
		Limit:  1,
	})
	require.NoError(n.T(), err)
	assert.Empty(n.T(), response.Results)
	assert.Equal(n.T(), 1, response.Total)
}

func (n *NpmSuite) TestNpmPackageListEmptyHref() {
	response, err := n.tangy.NpmPackageList(context.Background(), "", tangy.NpmPackageListFilters{}, tangy.PageOptions{Limit: 10})
	require.NoError(n.T(), err)
	assert.Empty(n.T(), response.Results)
	assert.Zero(n.T(), response.Total)
}

func (n *NpmSuite) TestNpmPackageGet() {
	detail, err := n.tangy.NpmPackageGet(
		context.Background(),
		n.repositoryHref,
		testNpmPackageName,
		testNpmVersion,
	)
	require.NoError(n.T(), err)
	assert.Equal(n.T(), testNpmPackageName, detail.Name)
	assert.Equal(n.T(), testNpmVersion, detail.Version)
	assert.NotEmpty(n.T(), detail.CreatedAt)
	assert.Contains(n.T(), detail.Versions, testNpmVersion)
	require.NotEmpty(n.T(), detail.LatestVersions)
	assert.NotEmpty(n.T(), detail.Tarball.RelativePath)
	assert.True(n.T(), strings.HasSuffix(detail.Tarball.Filename, ".tgz"), detail.Tarball.Filename)
	assert.NotEmpty(n.T(), detail.Tarball.Sha256)
	assert.NotZero(n.T(), detail.Tarball.Size)
}

func (n *NpmSuite) TestNpmPackageVersionsGet() {
	details, err := n.tangy.NpmPackageVersionsGet(
		context.Background(),
		n.repositoryHref,
		testNpmPackageName,
	)
	require.NoError(n.T(), err)
	require.Len(n.T(), details, 1)

	detail := details[0]
	assert.Equal(n.T(), testNpmPackageName, detail.Name)
	assert.Equal(n.T(), testNpmVersion, detail.Version)
	assert.NotEmpty(n.T(), detail.CreatedAt)
	assert.Equal(n.T(), []string{testNpmVersion}, detail.Versions)
	assert.Len(n.T(), detail.LatestVersions, 1)
	assert.NotEmpty(n.T(), detail.Tarball.RelativePath)
	assert.NotEmpty(n.T(), detail.Tarball.Sha256)
	assert.NotZero(n.T(), detail.Tarball.Size)
}

func (n *NpmSuite) TestNpmPackageGetNotFound() {
	_, err := n.tangy.NpmPackageGet(
		context.Background(),
		n.repositoryHref,
		testNpmPackageName,
		"9.9.9",
	)
	require.Error(n.T(), err)
	assert.ErrorIs(n.T(), err, tangy.ErrNpmPackageNotFound)
}

func (n *NpmSuite) TestNpmPackageVersionsGetNotFound() {
	_, err := n.tangy.NpmPackageVersionsGet(
		context.Background(),
		n.repositoryHref,
		"nonexistent-package",
	)
	require.Error(n.T(), err)
	assert.ErrorIs(n.T(), err, tangy.ErrNpmPackageNotFound)
}

func (n *NpmSuite) TestNpmPackageGetEmptyHref() {
	detail, err := n.tangy.NpmPackageGet(context.Background(), "", testNpmPackageName, testNpmVersion)
	require.NoError(n.T(), err)
	assert.Empty(n.T(), detail.Name)
}

func (n *NpmSuite) TestNpmPackageVersionsGetEmptyHref() {
	details, err := n.tangy.NpmPackageVersionsGet(context.Background(), "", testNpmPackageName)
	require.NoError(n.T(), err)
	assert.Nil(n.T(), details)
}

func (n *NpmSuite) TestNpmBuildList() {
	response, err := n.tangy.NpmBuildList(
		context.Background(),
		n.repositoryHref,
		"",
		"",
		tangy.PageOptions{Offset: 0, Limit: 10},
	)
	require.NoError(n.T(), err)
	require.NotEmpty(n.T(), response.Results)
	assert.GreaterOrEqual(n.T(), response.Total, 1)
	assert.Equal(n.T(), 10, response.Limit)

	found := false
	for _, build := range response.Results {
		if build.Name == testNpmPackageName && build.Version == testNpmVersion {
			found = true
			assert.NotEmpty(n.T(), build.CreatedAt)
		}
	}
	assert.True(n.T(), found)

	filtered, err := n.tangy.NpmBuildList(
		context.Background(),
		n.repositoryHref,
		testNpmPackageName,
		testNpmVersion,
		tangy.PageOptions{Offset: 0, Limit: 10},
	)
	require.NoError(n.T(), err)
	require.Len(n.T(), filtered.Results, 1)
	assert.Equal(n.T(), 1, filtered.Total)
	assert.Equal(n.T(), testNpmPackageName, filtered.Results[0].Name)
	assert.Equal(n.T(), testNpmVersion, filtered.Results[0].Version)
}

func (n *NpmSuite) TestNpmBuildListPagination() {
	response, err := n.tangy.NpmBuildList(
		context.Background(),
		n.repositoryHref,
		testNpmPackageName,
		"",
		tangy.PageOptions{Offset: 0, Limit: 1},
	)
	require.NoError(n.T(), err)
	assert.Len(n.T(), response.Results, 1)
	assert.Equal(n.T(), 1, response.Total)

	response, err = n.tangy.NpmBuildList(
		context.Background(),
		n.repositoryHref,
		testNpmPackageName,
		"",
		tangy.PageOptions{Offset: 1, Limit: 1},
	)
	require.NoError(n.T(), err)
	assert.Empty(n.T(), response.Results)
	assert.Equal(n.T(), 1, response.Total)
}

func (n *NpmSuite) TestNpmBuildListEmptyHref() {
	response, err := n.tangy.NpmBuildList(context.Background(), "", testNpmPackageName, testNpmVersion, tangy.PageOptions{Limit: 10})
	require.NoError(n.T(), err)
	assert.Empty(n.T(), response.Results)
	assert.Zero(n.T(), response.Total)
}

const (
	testNpmScopedRepoName    = "types-is-odd-fixture"
	testNpmScopedPackageName = "@types/is-odd"
	testNpmScopedVersion     = "3.0.0"
	testNpmScopedRemoteURL   = "https://registry.npmjs.org/@types/is-odd/3.0.0"
)

type NpmScopedSuite struct {
	suite.Suite
	client         *zestwrapper.NpmZest
	tangy          tangy.Tangy
	domainName     string
	repositoryHref string
}

func (n *NpmScopedSuite) createTestRepository(t *testing.T) {
	_, err := n.client.LookupOrCreateDomain(n.domainName)
	require.NoError(t, err)

	repoHref, remoteHref, err := n.client.CreateRepository(
		n.domainName,
		testNpmScopedRepoName,
		testNpmScopedRemoteURL,
	)
	require.NoError(t, err)

	syncTask, err := n.client.SyncNpmRepository(repoHref, remoteHref)
	require.NoError(t, err)

	_, err = n.client.PollTask(syncTask)
	require.NoError(t, err)

	n.repositoryHref = repoHref
}

func TestNpmScopedSuite(t *testing.T) {
	s := config.Get().Server
	npmZest := zestwrapper.NewNpmZest(context.Background(), s)

	dbConfig := config.Get().Database
	ta, err := tangy.New(tangy.Database{
		Name:     dbConfig.Name,
		Host:     dbConfig.Host,
		Port:     dbConfig.Port,
		User:     dbConfig.User,
		Password: dbConfig.Password,
	}, tangy.Logger{Enabled: true, Logger: &log.Logger, LogLevel: zerolog.LevelDebugValue})
	require.NoError(t, err)
	t.Cleanup(ta.Close)

	n := NpmScopedSuite{}
	n.client = &npmZest
	n.tangy = ta
	n.domainName = RandStringBytes(10)

	n.createTestRepository(t)

	suite.Run(t, &n)
}

func (n *NpmScopedSuite) TestNpmScopedPackage() {
	listResponse, err := n.tangy.NpmPackageList(context.Background(), n.repositoryHref, tangy.NpmPackageListFilters{Search: "@types"}, tangy.PageOptions{
		Offset: 0,
		Limit:  10,
	})
	require.NoError(n.T(), err)
	require.NotEmpty(n.T(), listResponse.Results)
	assert.Equal(n.T(), 1, listResponse.Total)
	assert.Equal(n.T(), testNpmScopedPackageName, listResponse.Results[0].Name)
	assert.Contains(n.T(), listResponse.Results[0].Versions, testNpmScopedVersion)

	listResponse, err = n.tangy.NpmPackageList(context.Background(), n.repositoryHref, tangy.NpmPackageListFilters{Search: "is-odd"}, tangy.PageOptions{
		Offset: 0,
		Limit:  10,
	})
	require.NoError(n.T(), err)
	require.NotEmpty(n.T(), listResponse.Results)
	assert.Equal(n.T(), 1, listResponse.Total)
	assert.Equal(n.T(), testNpmScopedPackageName, listResponse.Results[0].Name)

	detail, err := n.tangy.NpmPackageGet(
		context.Background(),
		n.repositoryHref,
		testNpmScopedPackageName,
		testNpmScopedVersion,
	)
	require.NoError(n.T(), err)
	assert.Equal(n.T(), testNpmScopedPackageName, detail.Name)
	assert.Equal(n.T(), testNpmScopedVersion, detail.Version)
	assert.NotEmpty(n.T(), detail.CreatedAt)
	assert.NotEmpty(n.T(), detail.Tarball.RelativePath)
	assert.True(n.T(), strings.HasSuffix(detail.Tarball.Filename, ".tgz"), detail.Tarball.Filename)
	assert.NotEmpty(n.T(), detail.Tarball.Sha256)
	assert.NotZero(n.T(), detail.Tarball.Size)

	details, err := n.tangy.NpmPackageVersionsGet(
		context.Background(),
		n.repositoryHref,
		testNpmScopedPackageName,
	)
	require.NoError(n.T(), err)
	require.Len(n.T(), details, 1)
	assert.Equal(n.T(), testNpmScopedPackageName, details[0].Name)
	assert.Equal(n.T(), testNpmScopedVersion, details[0].Version)

	buildResponse, err := n.tangy.NpmBuildList(
		context.Background(),
		n.repositoryHref,
		testNpmScopedPackageName,
		testNpmScopedVersion,
		tangy.PageOptions{Offset: 0, Limit: 10},
	)
	require.NoError(n.T(), err)
	require.Len(n.T(), buildResponse.Results, 1)
	assert.Equal(n.T(), 1, buildResponse.Total)
	assert.Equal(n.T(), testNpmScopedPackageName, buildResponse.Results[0].Name)
	assert.Equal(n.T(), testNpmScopedVersion, buildResponse.Results[0].Version)
}
