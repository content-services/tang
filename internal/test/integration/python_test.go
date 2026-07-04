package integration

import (
	"context"
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
	testPythonRepoName = "shelf-reader-fixture"
	testPythonRepoURL  = "https://pypi.org/"
	testPythonIncludes = "shelf-reader"
)

type PythonSuite struct {
	suite.Suite
	client         *zestwrapper.PythonZest
	tangy          tangy.Tangy
	domainName     string
	repositoryHref string
}

func (p *PythonSuite) createTestRepository(t *testing.T) {
	_, err := p.client.LookupOrCreateDomain(p.domainName)
	require.NoError(t, err)

	repoHref, remoteHref, err := p.client.CreateRepository(
		p.domainName,
		testPythonRepoName,
		testPythonRepoURL,
		[]string{testPythonIncludes},
	)
	require.NoError(t, err)

	syncTask, err := p.client.SyncPythonRepository(repoHref, remoteHref)
	require.NoError(t, err)

	_, err = p.client.PollTask(syncTask)
	require.NoError(t, err)

	p.repositoryHref = repoHref
}

func TestPythonSuite(t *testing.T) {
	s := config.Get().Server
	pythonZest := zestwrapper.NewPythonZest(context.Background(), s)

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

	p := PythonSuite{}
	p.client = &pythonZest
	p.tangy = ta
	p.domainName = RandStringBytes(10)

	p.createTestRepository(t)

	suite.Run(t, &p)
}

func (p *PythonSuite) TestPythonPackageList() {
	response, err := p.tangy.PythonPackageList(context.Background(), p.repositoryHref, tangy.PythonPackageListFilters{}, tangy.PageOptions{
		Offset: 0,
		Limit:  10,
	})
	require.NoError(p.T(), err)
	require.NotEmpty(p.T(), response.Results)
	assert.Equal(p.T(), 1, response.Total)

	pkg := response.Results[0]
	assert.Equal(p.T(), "shelf-reader", pkg.NameNormalized)
	assert.Contains(p.T(), pkg.Versions, "0.1")
	require.NotEmpty(p.T(), pkg.LatestVersions)

	foundVersion := false
	for _, latest := range pkg.LatestVersions {
		if latest.Version == "0.1" {
			foundVersion = true
			assert.NotEmpty(p.T(), latest.CreatedAt)
		}
	}
	assert.True(p.T(), foundVersion)
}

func (p *PythonSuite) TestPythonPackageListSearchFilter() {
	response, err := p.tangy.PythonPackageList(context.Background(), p.repositoryHref, tangy.PythonPackageListFilters{Search: "shelf"}, tangy.PageOptions{
		Offset: 0,
		Limit:  10,
	})
	require.NoError(p.T(), err)
	require.NotEmpty(p.T(), response.Results)
	assert.Equal(p.T(), 1, response.Total)
	assert.Equal(p.T(), "shelf-reader", response.Results[0].NameNormalized)

	response, err = p.tangy.PythonPackageList(context.Background(), p.repositoryHref, tangy.PythonPackageListFilters{Search: "nonexistent-package"}, tangy.PageOptions{
		Offset: 0,
		Limit:  10,
	})
	require.NoError(p.T(), err)
	assert.Empty(p.T(), response.Results)
	assert.Zero(p.T(), response.Total)
}

func (p *PythonSuite) TestPythonPackageListPagination() {
	response, err := p.tangy.PythonPackageList(context.Background(), p.repositoryHref, tangy.PythonPackageListFilters{}, tangy.PageOptions{
		Offset: 0,
		Limit:  1,
	})
	require.NoError(p.T(), err)
	assert.Len(p.T(), response.Results, 1)
	assert.Equal(p.T(), 1, response.Total)
	assert.Equal(p.T(), 1, response.Limit)

	response, err = p.tangy.PythonPackageList(context.Background(), p.repositoryHref, tangy.PythonPackageListFilters{}, tangy.PageOptions{
		Offset: 1,
		Limit:  1,
	})
	require.NoError(p.T(), err)
	assert.Empty(p.T(), response.Results)
	assert.Equal(p.T(), 1, response.Total)
}

func (p *PythonSuite) TestPythonPackageListEmptyHref() {
	response, err := p.tangy.PythonPackageList(context.Background(), "", tangy.PythonPackageListFilters{}, tangy.PageOptions{Limit: 10})
	require.NoError(p.T(), err)
	assert.Empty(p.T(), response.Results)
	assert.Zero(p.T(), response.Total)
}

func (p *PythonSuite) TestPythonDistributionList() {
	response, err := p.tangy.PythonDistributionList(
		context.Background(),
		p.repositoryHref,
		"shelf-reader",
		"0.1",
		tangy.PageOptions{Offset: 0, Limit: 10},
	)
	require.NoError(p.T(), err)
	require.NotEmpty(p.T(), response.Results)
	assert.GreaterOrEqual(p.T(), response.Total, 1)
	assert.Equal(p.T(), 10, response.Limit)

	for _, dist := range response.Results {
		assert.Equal(p.T(), "shelf-reader", dist.NameNormalized)
		assert.Equal(p.T(), "0.1", dist.Version)
		assert.NotEmpty(p.T(), dist.Filename)
		assert.NotEmpty(p.T(), dist.PackageType)
		assert.NotEmpty(p.T(), dist.Sha256)
		assert.NotZero(p.T(), dist.Size)
		assert.NotEmpty(p.T(), dist.CreatedAt)
	}

	packageTypes := make(map[string]struct{})
	for _, dist := range response.Results {
		packageTypes[dist.PackageType] = struct{}{}
	}
	assert.NotEmpty(p.T(), packageTypes)
}

func (p *PythonSuite) TestPythonDistributionListPagination() {
	response, err := p.tangy.PythonDistributionList(
		context.Background(),
		p.repositoryHref,
		"shelf-reader",
		"0.1",
		tangy.PageOptions{Offset: 0, Limit: 1},
	)
	require.NoError(p.T(), err)
	assert.Len(p.T(), response.Results, 1)
	assert.GreaterOrEqual(p.T(), response.Total, 1)

	response, err = p.tangy.PythonDistributionList(
		context.Background(),
		p.repositoryHref,
		"shelf-reader",
		"0.1",
		tangy.PageOptions{Offset: 100, Limit: 10},
	)
	require.NoError(p.T(), err)
	assert.Empty(p.T(), response.Results)
	assert.GreaterOrEqual(p.T(), response.Total, 1)
}
