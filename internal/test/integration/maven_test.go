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
	testMavenRepoName             = "maven-releases-fixture"
	testMavenDistributionBasePath = "maven-releases-fixture"
	testMavenFixtureUrl           = "https://content-services.github.io/fixtures/maven/maven-releases/"
	testMavenGroupID              = "com.example.fixture"
	testMavenArtifactID           = "raccoon"
	testMavenBaseVersion100       = "1.0.0"
	testMavenBaseVersion200       = "2.0.0"
)

var testMavenArtifactPaths = []string{
	"com/example/fixture/raccoon/1.0.0.rhlw-00001/com.example.fixture.raccoon.1.0.0.rhlw-00001.pom",
	"com/example/fixture/raccoon/1.0.0.rhlw-00001/com.example.fixture.raccoon.1.0.0.rhlw-00001.jar",
	"com/example/fixture/raccoon/1.0.0.rhlw-00002/com.example.fixture.raccoon.1.0.0.rhlw-00002.pom",
	"com/example/fixture/raccoon/1.0.0.rhlw-00002/com.example.fixture.raccoon.1.0.0.rhlw-00002.jar",
	"com/example/fixture/raccoon/2.0.0.rhlw-00001/com.example.fixture.raccoon.2.0.0.rhlw-00001.pom",
	"com/example/fixture/raccoon/2.0.0.rhlw-00001/com.example.fixture.raccoon.2.0.0.rhlw-00001.jar",
	"com/example/fixture/raccoon/2.0.0.rhlw-00002/com.example.fixture.raccoon.2.0.0.rhlw-00002.pom",
	"com/example/fixture/raccoon/2.0.0.rhlw-00002/com.example.fixture.raccoon.2.0.0.rhlw-00002.jar",
}

type MavenSuite struct {
	suite.Suite
	client            *zestwrapper.MavenZest
	tangy             tangy.Tangy
	domainName        string
	contentUrl        string
	contentPathPrefix string
	repositoryHref    string
}

func (m *MavenSuite) createTestRepository(t *testing.T) {
	_, err := m.client.LookupOrCreateDomain(m.domainName)
	require.NoError(t, err)

	repoHref, remoteHref, err := m.client.CreateRepository(
		m.domainName,
		testMavenRepoName,
		testMavenFixtureUrl,
		testMavenDistributionBasePath,
	)
	require.NoError(t, err)

	for _, artifactPath := range testMavenArtifactPaths {
		err = m.client.FetchArtifact(m.contentUrl, m.contentPathPrefix, m.domainName, testMavenDistributionBasePath, artifactPath)
		require.NoError(t, err)
	}

	addTask, err := m.client.AddCachedContent(repoHref, remoteHref)
	require.NoError(t, err)

	_, err = m.client.PollTask(addTask)
	require.NoError(t, err)

	m.repositoryHref = repoHref
}

func TestMavenSuite(t *testing.T) {
	s := config.Get().Server
	mavenZest := zestwrapper.NewMavenZest(context.Background(), s)

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

	m := MavenSuite{}
	m.client = &mavenZest
	m.tangy = ta
	m.contentUrl = s.ContentUrl
	m.contentPathPrefix = s.ContentPathPrefix
	m.domainName = RandStringBytes(10)

	m.createTestRepository(t)

	suite.Run(t, &m)
}

func (m *MavenSuite) TestMavenPackageList() {
	response, err := m.tangy.MavenPackageList(context.Background(), m.repositoryHref, tangy.MavenPackageListFilters{}, tangy.PageOptions{
		Offset: 0,
		Limit:  10,
	})
	require.NoError(m.T(), err)
	require.NotEmpty(m.T(), response.Results)
	assert.Equal(m.T(), 1, response.Total)

	pkg := response.Results[0]
	assert.Equal(m.T(), testMavenGroupID, pkg.GroupID)
	assert.Equal(m.T(), testMavenArtifactID, pkg.ArtifactID)
	assert.Len(m.T(), pkg.Versions, 2)
	assert.Contains(m.T(), pkg.Versions, testMavenBaseVersion100)
	assert.Contains(m.T(), pkg.Versions, testMavenBaseVersion200)
	require.Len(m.T(), pkg.LatestReleases, 2)

	foundVersions := make(map[string]string)
	for _, latest := range pkg.LatestReleases {
		foundVersions[latest.Version] = latest.Release
		assert.NotEmpty(m.T(), latest.CreatedAt)
	}
	assert.Equal(m.T(), "rhlw-00002", foundVersions[testMavenBaseVersion100])
	assert.Equal(m.T(), "rhlw-00002", foundVersions[testMavenBaseVersion200])
}

func (m *MavenSuite) TestMavenPackageListSearchFilter() {
	response, err := m.tangy.MavenPackageList(context.Background(), m.repositoryHref, tangy.MavenPackageListFilters{Search: "raccoon"}, tangy.PageOptions{
		Offset: 0,
		Limit:  10,
	})
	require.NoError(m.T(), err)
	require.NotEmpty(m.T(), response.Results)
	assert.Equal(m.T(), 1, response.Total)
	assert.Equal(m.T(), testMavenArtifactID, response.Results[0].ArtifactID)

	response, err = m.tangy.MavenPackageList(context.Background(), m.repositoryHref, tangy.MavenPackageListFilters{Search: "nonexistent-artifact"}, tangy.PageOptions{
		Offset: 0,
		Limit:  10,
	})
	require.NoError(m.T(), err)
	assert.Empty(m.T(), response.Results)
	assert.Zero(m.T(), response.Total)
}

func (m *MavenSuite) TestMavenPackageListPagination() {
	response, err := m.tangy.MavenPackageList(context.Background(), m.repositoryHref, tangy.MavenPackageListFilters{}, tangy.PageOptions{
		Offset: 0,
		Limit:  1,
	})
	require.NoError(m.T(), err)
	assert.Len(m.T(), response.Results, 1)
	assert.Equal(m.T(), 1, response.Total)
	assert.Equal(m.T(), 1, response.Limit)

	response, err = m.tangy.MavenPackageList(context.Background(), m.repositoryHref, tangy.MavenPackageListFilters{}, tangy.PageOptions{
		Offset: 1,
		Limit:  1,
	})
	require.NoError(m.T(), err)
	assert.Empty(m.T(), response.Results)
	assert.Equal(m.T(), 1, response.Total)
}

func (m *MavenSuite) TestMavenPackageListEmptyHref() {
	response, err := m.tangy.MavenPackageList(context.Background(), "", tangy.MavenPackageListFilters{}, tangy.PageOptions{Limit: 10})
	require.NoError(m.T(), err)
	assert.Empty(m.T(), response.Results)
	assert.Zero(m.T(), response.Total)
}

func (m *MavenSuite) TestMavenVersionsList() {
	response, err := m.tangy.MavenVersionsList(
		context.Background(),
		m.repositoryHref,
		testMavenGroupID,
		testMavenArtifactID,
		testMavenBaseVersion100,
		tangy.PageOptions{Offset: 0, Limit: 10},
	)
	require.NoError(m.T(), err)
	require.Len(m.T(), response.Results, 1)
	assert.Equal(m.T(), 1, response.Total)
	assert.Equal(m.T(), 10, response.Limit)

	version := response.Results[0]
	assert.Equal(m.T(), testMavenGroupID, version.GroupID)
	assert.Equal(m.T(), testMavenArtifactID, version.ArtifactID)
	assert.Equal(m.T(), testMavenBaseVersion100, version.Version)
	require.Len(m.T(), version.Builds, 2)

	foundReleases := make(map[string]bool)
	for _, build := range version.Builds {
		assert.Equal(m.T(), testMavenBaseVersion100, build.Version)
		assert.True(m.T(), strings.HasSuffix(build.Filename, ".pom"), build.Filename)
		assert.NotEmpty(m.T(), build.CreatedAt)
		foundReleases[build.Release] = true
	}
	assert.True(m.T(), foundReleases["rhlw-00001"])
	assert.True(m.T(), foundReleases["rhlw-00002"])
}

func (m *MavenSuite) TestMavenVersionsListPagination() {
	response, err := m.tangy.MavenVersionsList(
		context.Background(),
		m.repositoryHref,
		testMavenGroupID,
		testMavenArtifactID,
		"",
		tangy.PageOptions{Offset: 0, Limit: 1},
	)
	require.NoError(m.T(), err)
	assert.Len(m.T(), response.Results, 1)
	assert.Equal(m.T(), 2, response.Total)

	response, err = m.tangy.MavenVersionsList(
		context.Background(),
		m.repositoryHref,
		testMavenGroupID,
		testMavenArtifactID,
		"",
		tangy.PageOptions{Offset: 2, Limit: 10},
	)
	require.NoError(m.T(), err)
	assert.Empty(m.T(), response.Results)
	assert.Equal(m.T(), 2, response.Total)
}

func (m *MavenSuite) TestMavenVersionsListEmptyHref() {
	response, err := m.tangy.MavenVersionsList(context.Background(), "", testMavenGroupID, testMavenArtifactID, testMavenBaseVersion100, tangy.PageOptions{Limit: 10})
	require.NoError(m.T(), err)
	assert.Empty(m.T(), response.Results)
	assert.Zero(m.T(), response.Total)
}

func (m *MavenSuite) TestMavenVersionsListOptionalFilters() {
	response, err := m.tangy.MavenVersionsList(
		context.Background(),
		m.repositoryHref,
		"",
		"",
		"",
		tangy.PageOptions{Offset: 0, Limit: 10},
	)
	require.NoError(m.T(), err)
	require.Len(m.T(), response.Results, 2)
	assert.Equal(m.T(), 2, response.Total)

	type versionRelease struct{ version, release string }
	foundBuilds := make(map[versionRelease]bool)
	for _, result := range response.Results {
		assert.Equal(m.T(), testMavenGroupID, result.GroupID)
		assert.Equal(m.T(), testMavenArtifactID, result.ArtifactID)
		for _, build := range result.Builds {
			foundBuilds[versionRelease{build.Version, build.Release}] = true
		}
	}
	assert.True(m.T(), foundBuilds[versionRelease{testMavenBaseVersion100, "rhlw-00001"}])
	assert.True(m.T(), foundBuilds[versionRelease{testMavenBaseVersion100, "rhlw-00002"}])
	assert.True(m.T(), foundBuilds[versionRelease{testMavenBaseVersion200, "rhlw-00001"}])
	assert.True(m.T(), foundBuilds[versionRelease{testMavenBaseVersion200, "rhlw-00002"}])

	filtered, err := m.tangy.MavenVersionsList(
		context.Background(),
		m.repositoryHref,
		testMavenGroupID,
		"",
		"",
		tangy.PageOptions{Offset: 0, Limit: 10},
	)
	require.NoError(m.T(), err)
	require.Len(m.T(), filtered.Results, 2)
	assert.Equal(m.T(), 2, filtered.Total)

	filtered, err = m.tangy.MavenVersionsList(
		context.Background(),
		m.repositoryHref,
		"",
		testMavenArtifactID,
		"",
		tangy.PageOptions{Offset: 0, Limit: 10},
	)
	require.NoError(m.T(), err)
	require.Len(m.T(), filtered.Results, 2)
	assert.Equal(m.T(), 2, filtered.Total)
}

func (m *MavenSuite) TestMavenRepositoryMetrics() {
	metrics, err := m.tangy.MavenRepositoryMetrics(context.Background(), m.repositoryHref)
	require.NoError(m.T(), err)
	assert.Equal(m.T(), 1, metrics.PackageCount)
	assert.Equal(m.T(), 4, metrics.BuildCount)
	assert.Equal(m.T(), 2, metrics.VersionCount)
}

func (m *MavenSuite) TestMavenRepositoryMetricsEmptyHref() {
	metrics, err := m.tangy.MavenRepositoryMetrics(context.Background(), "")
	require.NoError(m.T(), err)
	assert.Zero(m.T(), metrics.PackageCount)
	assert.Zero(m.T(), metrics.BuildCount)
	assert.Zero(m.T(), metrics.VersionCount)
}
