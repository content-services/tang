package integration

import (
	"context"
	"fmt"
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

const (
	testPythonRepoName              = "shelf-reader-fixture"
	testPythonMultiVersionRepoName  = "idna-multi-version-fixture"
	testPythonMultiVersionPackage   = "idna"
	testPythonMultiVersionKeepCount = int64(2)
	testPythonRepoURL               = "https://pypi.org/"
	testPythonIncludes              = "shelf-reader"
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
		0,
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

func (p *PythonSuite) TestPythonPackageGet() {
	detail, err := p.tangy.PythonPackageGet(
		context.Background(),
		p.repositoryHref,
		"shelf-reader",
		"0.1",
	)
	require.NoError(p.T(), err)
	assert.Equal(p.T(), "shelf-reader", detail.NameNormalized)
	assert.Equal(p.T(), "0.1", detail.Version)
	assert.NotEmpty(p.T(), detail.Name)
	assert.Equal(p.T(), "Austin Macdonald", detail.Author)
	assert.Equal(p.T(), "asmacdo@gmail.com", detail.AuthorEmail)
	assert.NotEmpty(p.T(), detail.LastUpdated)
	assert.Contains(p.T(), detail.Versions, "0.1")
	require.NotEmpty(p.T(), detail.LatestVersions)

	foundVersion := false
	for _, latest := range detail.LatestVersions {
		if latest.Version == "0.1" {
			foundVersion = true
			assert.NotEmpty(p.T(), latest.CreatedAt)
		}
	}
	assert.True(p.T(), foundVersion)

	require.NotEmpty(p.T(), detail.Distributions)
	for _, dist := range detail.Distributions {
		assert.Equal(p.T(), "shelf-reader", dist.NameNormalized)
		assert.Equal(p.T(), "0.1", dist.Version)
		assert.NotEmpty(p.T(), dist.Filename)
		assert.NotEmpty(p.T(), dist.PackageType)
		assert.NotEmpty(p.T(), dist.Sha256)
		assert.NotZero(p.T(), dist.Size)
		assert.NotEmpty(p.T(), dist.CreatedAt)
	}
}

func (p *PythonSuite) TestPythonPackageVersionsGet() {
	repoHref, remoteHref, err := p.client.CreateRepository(
		p.domainName,
		testPythonMultiVersionRepoName,
		testPythonRepoURL,
		[]string{testPythonMultiVersionPackage},
		testPythonMultiVersionKeepCount,
	)
	require.NoError(p.T(), err)

	syncTask, err := p.client.SyncPythonRepository(repoHref, remoteHref)
	require.NoError(p.T(), err)

	_, err = p.client.PollTask(syncTask)
	require.NoError(p.T(), err)

	details, err := p.tangy.PythonPackageVersionsGet(
		context.Background(),
		repoHref,
		testPythonMultiVersionPackage,
	)
	require.NoError(p.T(), err)
	require.GreaterOrEqual(p.T(), len(details), 2)

	versions := make([]string, len(details))
	for i, detail := range details {
		assert.Equal(p.T(), testPythonMultiVersionPackage, detail.NameNormalized)
		assert.NotEmpty(p.T(), detail.Name)
		assert.NotEmpty(p.T(), detail.Version)
		assert.NotEmpty(p.T(), detail.LastUpdated)
		assert.Equal(p.T(), "Kim Davies", detail.Author)
		assert.Equal(p.T(), "Kim Davies <kim+pypi@gumleaf.org>", detail.AuthorEmail)
		require.NotEmpty(p.T(), detail.Distributions)

		for _, dist := range detail.Distributions {
			assert.Equal(p.T(), testPythonMultiVersionPackage, dist.NameNormalized)
			assert.Equal(p.T(), detail.Version, dist.Version)
		}

		versions[i] = detail.Version
	}

	assert.Len(p.T(), details[0].Versions, len(details))
	assert.Len(p.T(), details[0].LatestVersions, len(details))
	assert.Equal(p.T(), details[0].Versions, versions)
}

func (p *PythonSuite) TestPythonPackageVersionsGetNotFound() {
	_, err := p.tangy.PythonPackageVersionsGet(
		context.Background(),
		p.repositoryHref,
		"nonexistent-package",
	)
	require.Error(p.T(), err)
	assert.ErrorIs(p.T(), err, tangy.ErrPythonPackageNotFound)
}

func (p *PythonSuite) TestPythonPackageVersionsGetEmptyHref() {
	details, err := p.tangy.PythonPackageVersionsGet(context.Background(), "", "shelf-reader")
	require.NoError(p.T(), err)
	assert.Nil(p.T(), details)
}

func (p *PythonSuite) TestPythonPackageVersionsGetEmptyNameNormalized() {
	repoHref, remoteHref, err := p.client.CreateRepository(
		p.domainName,
		fmt.Sprintf("%s-%v", testPythonMultiVersionRepoName, rand.Int()),
		testPythonRepoURL,
		[]string{testPythonIncludes, testPythonMultiVersionPackage},
		0,
	)
	require.NoError(p.T(), err)

	syncTask, err := p.client.SyncPythonRepository(repoHref, remoteHref)
	require.NoError(p.T(), err)

	_, err = p.client.PollTask(syncTask)
	require.NoError(p.T(), err)

	// Call with empty nameNormalized - should return all packages in the repository
	details, err := p.tangy.PythonPackageVersionsGet(
		context.Background(),
		repoHref,
		"",
	)
	require.NoError(p.T(), err)
	require.NotEmpty(p.T(), details, "Should return packages when nameNormalized is empty")

	// Verify that we got actual package data back
	for _, detail := range details {
		assert.NotEmpty(p.T(), detail.NameNormalized)
		assert.NotEmpty(p.T(), detail.Name)
		assert.NotEmpty(p.T(), detail.Version)
	}

	singleDetails, err := p.tangy.PythonPackageVersionsGet(
		context.Background(),
		repoHref,
		testPythonMultiVersionPackage,
	)
	assert.NoError(p.T(), err)
	assert.Greater(p.T(), len(details), len(singleDetails))
}

func (p *PythonSuite) TestPythonPackageGetNotFound() {
	_, err := p.tangy.PythonPackageGet(
		context.Background(),
		p.repositoryHref,
		"shelf-reader",
		"9.9.9",
	)
	require.Error(p.T(), err)
	assert.ErrorIs(p.T(), err, tangy.ErrPythonPackageNotFound)
}

func (p *PythonSuite) TestPythonPackageGetEmptyHref() {
	detail, err := p.tangy.PythonPackageGet(context.Background(), "", "shelf-reader", "0.1")
	require.NoError(p.T(), err)
	assert.Empty(p.T(), detail.NameNormalized)
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
