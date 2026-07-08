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
	testMavenRepoName             = "junit-fixture"
	testMavenDistributionBasePath = "junit-fixture"
	testMavenRemoteURL            = "http://maven_fixture/"
	testMavenContentOrigin        = "http://localhost:8088"
	testMavenGroupID              = "junit"
	testMavenArtifactID           = "junit"
	testMavenVersion              = "4.13.2"
	testMavenPomPath              = "junit/junit/4.13.2/junit-4.13.2.pom"
)

type MavenSuite struct {
	suite.Suite
	client         *zestwrapper.MavenZest
	tangy          tangy.Tangy
	domainName     string
	repositoryHref string
}

func (m *MavenSuite) createTestRepository(t *testing.T) {
	_, err := m.client.LookupOrCreateDomain(m.domainName)
	require.NoError(t, err)

	repoHref, remoteHref, err := m.client.CreateRepository(
		m.domainName,
		testMavenRepoName,
		testMavenRemoteURL,
		testMavenDistributionBasePath,
	)
	require.NoError(t, err)

	err = m.client.FetchArtifact(testMavenContentOrigin, m.domainName, testMavenDistributionBasePath, testMavenPomPath)
	require.NoError(t, err)

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
	assert.Contains(m.T(), pkg.Versions, testMavenVersion)
	require.NotEmpty(m.T(), pkg.LatestReleases)

	foundVersion := false
	for _, latest := range pkg.LatestReleases {
		if latest.Version == testMavenVersion {
			foundVersion = true
			assert.NotEmpty(m.T(), latest.CreatedAt)
		}
	}
	assert.True(m.T(), foundVersion)
}

func (m *MavenSuite) TestMavenPackageListSearchFilter() {
	response, err := m.tangy.MavenPackageList(context.Background(), m.repositoryHref, tangy.MavenPackageListFilters{Search: "junit"}, tangy.PageOptions{
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

func (m *MavenSuite) TestMavenBuildList() {
	response, err := m.tangy.MavenBuildList(
		context.Background(),
		m.repositoryHref,
		testMavenGroupID,
		testMavenArtifactID,
		testMavenVersion,
		tangy.PageOptions{Offset: 0, Limit: 10},
	)
	require.NoError(m.T(), err)
	require.NotEmpty(m.T(), response.Results)
	assert.Equal(m.T(), 1, response.Total)
	assert.Equal(m.T(), 10, response.Limit)

	build := response.Results[0]
	assert.Equal(m.T(), testMavenGroupID, build.GroupID)
	assert.Equal(m.T(), testMavenArtifactID, build.ArtifactID)
	assert.Equal(m.T(), testMavenVersion, build.Version)
	assert.True(m.T(), strings.HasSuffix(build.Filename, ".pom"), build.Filename)
	assert.NotEmpty(m.T(), build.CreatedAt)
}

func (m *MavenSuite) TestMavenBuildListPagination() {
	response, err := m.tangy.MavenBuildList(
		context.Background(),
		m.repositoryHref,
		testMavenGroupID,
		testMavenArtifactID,
		testMavenVersion,
		tangy.PageOptions{Offset: 0, Limit: 1},
	)
	require.NoError(m.T(), err)
	assert.Len(m.T(), response.Results, 1)
	assert.Equal(m.T(), 1, response.Total)

	response, err = m.tangy.MavenBuildList(
		context.Background(),
		m.repositoryHref,
		testMavenGroupID,
		testMavenArtifactID,
		testMavenVersion,
		tangy.PageOptions{Offset: 1, Limit: 10},
	)
	require.NoError(m.T(), err)
	assert.Empty(m.T(), response.Results)
	assert.Equal(m.T(), 1, response.Total)
}

func (m *MavenSuite) TestMavenBuildListEmptyHref() {
	response, err := m.tangy.MavenBuildList(context.Background(), "", testMavenGroupID, testMavenArtifactID, testMavenVersion, tangy.PageOptions{Limit: 10})
	require.NoError(m.T(), err)
	assert.Empty(m.T(), response.Results)
	assert.Zero(m.T(), response.Total)
}

func (m *MavenSuite) TestMavenBuildListOptionalFilters() {
	response, err := m.tangy.MavenBuildList(
		context.Background(),
		m.repositoryHref,
		"",
		"",
		"",
		tangy.PageOptions{Offset: 0, Limit: 10},
	)
	require.NoError(m.T(), err)
	require.NotEmpty(m.T(), response.Results)
	assert.Equal(m.T(), 1, response.Total)

	build := response.Results[0]
	assert.Equal(m.T(), testMavenGroupID, build.GroupID)
	assert.Equal(m.T(), testMavenArtifactID, build.ArtifactID)
	assert.Equal(m.T(), testMavenVersion, build.Version)

	filtered, err := m.tangy.MavenBuildList(
		context.Background(),
		m.repositoryHref,
		testMavenGroupID,
		"",
		"",
		tangy.PageOptions{Offset: 0, Limit: 10},
	)
	require.NoError(m.T(), err)
	require.Len(m.T(), filtered.Results, 1)
	assert.Equal(m.T(), 1, filtered.Total)

	filtered, err = m.tangy.MavenBuildList(
		context.Background(),
		m.repositoryHref,
		"",
		testMavenArtifactID,
		"",
		tangy.PageOptions{Offset: 0, Limit: 10},
	)
	require.NoError(m.T(), err)
	require.Len(m.T(), filtered.Results, 1)
	assert.Equal(m.T(), 1, filtered.Total)
}

func (m *MavenSuite) TestMavenRepositoryMetrics() {
	metrics, err := m.tangy.MavenRepositoryMetrics(context.Background(), m.repositoryHref)
	require.NoError(m.T(), err)
	assert.Equal(m.T(), 1, metrics.PackageCount)
	assert.Equal(m.T(), 1, metrics.BuildCount)
}

func (m *MavenSuite) TestMavenRepositoryMetricsEmptyHref() {
	metrics, err := m.tangy.MavenRepositoryMetrics(context.Background(), "")
	require.NoError(m.T(), err)
	assert.Zero(m.T(), metrics.PackageCount)
	assert.Zero(m.T(), metrics.BuildCount)
}
