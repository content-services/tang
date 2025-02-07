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
const testRepoURL = "https://content-services.github.io/fixtures/yum/comps-modules/v1/"
const testRepoURLTwo = "https://content-services.github.io/fixtures/yum/comps-modules/v2/"
const testRepoNameWithErrata = "multiple-errata"
const testRepoURLWithErrata = "https://stephenw.fedorapeople.org/fakerepos/multiple_errata/"

func (r *RpmSuite) CreateTestRepository(t *testing.T, repoName string, repoUrl string) {
	_, err := r.client.LookupOrCreateDomain(r.domainName)
	require.NoError(t, err)

	repoHref, remoteHref, err := r.client.CreateRepository(r.domainName, repoName, repoUrl)
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

	r.domainName = RandStringBytes(10)

	r.CreateTestRepository(t, testRepoName, testRepoURL)

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
	assert.ElementsMatch(r.T(), search[0].Packages, []string{"cockateel", "penguin", "stork", "duck"})
	search, err = r.tangy.RpmRepositoryVersionPackageGroupSearch(context.Background(), []string{*firstVersionHref}, "mamm", 100)
	assert.NoError(r.T(), err)
	assert.Empty(r.T(), search)

	// Search second repository version, should have new package and removed package
	search, err = r.tangy.RpmRepositoryVersionPackageGroupSearch(context.Background(), []string{*secondVersionHref}, "bir", 100)
	assert.NoError(r.T(), err)
	assert.Equal(r.T(), search[0].Name, "birds")
	assert.ElementsMatch(r.T(), search[0].Packages, []string{"cockateel", "penguin", "duck"})
	search, err = r.tangy.RpmRepositoryVersionPackageGroupSearch(context.Background(), []string{*secondVersionHref}, "mamm", 100)
	assert.NoError(r.T(), err)
	assert.ElementsMatch(r.T(), search[0].Packages, []string{"bear", "cat"})

	// Re-search the first version, should be the same
	search, err = r.tangy.RpmRepositoryVersionPackageGroupSearch(context.Background(), []string{*firstVersionHref}, "bir", 100)
	assert.NoError(r.T(), err)
	assert.Equal(r.T(), search[0].Name, "birds")
	assert.ElementsMatch(r.T(), search[0].Packages, []string{"cockateel", "penguin", "stork", "duck"})
	search, err = r.tangy.RpmRepositoryVersionPackageGroupSearch(context.Background(), []string{*firstVersionHref}, "mamm", 100)
	assert.NoError(r.T(), err)
	assert.Empty(r.T(), search)

	// Search both versions
	search, err = r.tangy.RpmRepositoryVersionPackageGroupSearch(context.Background(), []string{*firstVersionHref, *secondVersionHref}, "s", 100)
	assert.NoError(r.T(), err)
	assert.ElementsMatch(r.T(), search[0].Packages, []string{"cockateel", "penguin", "stork", "duck"})
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

func (r *RpmSuite) TestRpmRepositoryVersionErrataListFilter() {
	r.CreateTestRepository(r.T(), testRepoNameWithErrata, testRepoURLWithErrata)
	resp, err := r.client.GetRpmRepositoryByName(r.domainName, testRepoNameWithErrata)
	require.NoError(r.T(), err)
	require.NotNil(r.T(), resp.LatestVersionHref)
	firstVersionHref := resp.LatestVersionHref

	// no filter
	singleList, total, err := r.tangy.RpmRepositoryVersionErrataList(context.Background(), []string{*firstVersionHref}, tangy.ErrataListFilters{Search: ""}, tangy.PageOptions{})
	require.NoError(r.T(), err)
	assert.NotEmpty(r.T(), singleList)
	assert.Equal(r.T(), total, 6)

	// test limit
	singleList, total, err = r.tangy.RpmRepositoryVersionErrataList(context.Background(), []string{*firstVersionHref}, tangy.ErrataListFilters{}, tangy.PageOptions{Limit: 1})
	require.NoError(r.T(), err)
	assert.NotEmpty(r.T(), singleList)
	assert.Equal(r.T(), len(singleList), 1)
	assert.Equal(r.T(), total, 6)

	// test offset
	singleList, total, err = r.tangy.RpmRepositoryVersionErrataList(context.Background(), []string{*firstVersionHref}, tangy.ErrataListFilters{}, tangy.PageOptions{Offset: 3})
	require.NoError(r.T(), err)
	assert.NotEmpty(r.T(), singleList)
	assert.Equal(r.T(), len(singleList), 3)
	assert.Equal(r.T(), total, 6)

	// id filter partial
	singleList, total, err = r.tangy.RpmRepositoryVersionErrataList(context.Background(), []string{*firstVersionHref}, tangy.ErrataListFilters{Search: "0055"}, tangy.PageOptions{})
	require.NoError(r.T(), err)
	assert.NotEmpty(r.T(), singleList)
	assert.Equal(r.T(), total, 1)

	// type filter
	singleList, total, err = r.tangy.RpmRepositoryVersionErrataList(context.Background(), []string{*firstVersionHref}, tangy.ErrataListFilters{Type: []string{"security"}}, tangy.PageOptions{})
	require.NoError(r.T(), err)
	assert.NotEmpty(r.T(), singleList)
	assert.Equal(r.T(), total, 4)

	// multiple types filter
	singleList, total, err = r.tangy.RpmRepositoryVersionErrataList(context.Background(), []string{*firstVersionHref}, tangy.ErrataListFilters{Type: []string{"security", "enhancement"}}, tangy.PageOptions{})
	require.NoError(r.T(), err)
	assert.NotEmpty(r.T(), singleList)
	assert.Equal(r.T(), total, 5)

	// type filter partial (empty)
	emptyList, total, err := r.tangy.RpmRepositoryVersionErrataList(context.Background(), []string{*firstVersionHref}, tangy.ErrataListFilters{Type: []string{"secu"}}, tangy.PageOptions{})
	require.NoError(r.T(), err)
	assert.Empty(r.T(), emptyList)
	assert.Equal(r.T(), total, 0)

	// severity filter
	singleList, total, err = r.tangy.RpmRepositoryVersionErrataList(context.Background(), []string{*firstVersionHref}, tangy.ErrataListFilters{Severity: []string{"Low"}}, tangy.PageOptions{})
	require.NoError(r.T(), err)
	assert.NotEmpty(r.T(), singleList)
	assert.Equal(r.T(), total, 1)

	// multiple severities filter
	singleList, total, err = r.tangy.RpmRepositoryVersionErrataList(context.Background(), []string{*firstVersionHref}, tangy.ErrataListFilters{Severity: []string{"Low", "Unknown"}}, tangy.PageOptions{})
	require.NoError(r.T(), err)
	assert.NotEmpty(r.T(), singleList)
	assert.Equal(r.T(), total, 3)

	// severity filter partial (empty)
	emptyList, total, err = r.tangy.RpmRepositoryVersionErrataList(context.Background(), []string{*firstVersionHref}, tangy.ErrataListFilters{Severity: []string{"Lo"}}, tangy.PageOptions{})
	require.NoError(r.T(), err)
	assert.Empty(r.T(), emptyList)
	assert.Equal(r.T(), total, 0)
}

func (r *RpmSuite) TestRpmRepositoryVersionErrataListSort() {
	resp, err := r.client.GetRpmRepositoryByName(r.domainName, testRepoNameWithErrata)
	require.NoError(r.T(), err)
	require.NotNil(r.T(), resp.LatestVersionHref)
	firstVersionHref := resp.LatestVersionHref

	// no sort specified, defaults to issued_date desc
	errata, total, err := r.tangy.RpmRepositoryVersionErrataList(context.Background(), []string{*firstVersionHref}, tangy.ErrataListFilters{}, tangy.PageOptions{})
	require.NoError(r.T(), err)
	assert.NotEmpty(r.T(), errata)
	assert.Equal(r.T(), errata[0].IssuedDate, "2013-01-27 16:08:09")
	assert.Equal(r.T(), total, 6)

	// sorting by issued_date asc
	errata, total, err = r.tangy.RpmRepositoryVersionErrataList(context.Background(), []string{*firstVersionHref}, tangy.ErrataListFilters{}, tangy.PageOptions{SortBy: "issued_date:asc"})
	require.NoError(r.T(), err)
	assert.NotEmpty(r.T(), errata)
	assert.Equal(r.T(), errata[0].IssuedDate, "2009-05-20 00:00:00")
	assert.Equal(r.T(), total, 6)

	// sorting by issued_date desc
	errata, total, err = r.tangy.RpmRepositoryVersionErrataList(context.Background(), []string{*firstVersionHref}, tangy.ErrataListFilters{}, tangy.PageOptions{SortBy: "issued_date:desc"})
	require.NoError(r.T(), err)
	assert.NotEmpty(r.T(), errata)
	assert.Equal(r.T(), errata[0].IssuedDate, "2013-01-27 16:08:09")
	assert.Equal(r.T(), total, 6)

	// sorting by type asc
	errata, total, err = r.tangy.RpmRepositoryVersionErrataList(context.Background(), []string{*firstVersionHref}, tangy.ErrataListFilters{}, tangy.PageOptions{SortBy: "type:asc"})
	require.NoError(r.T(), err)
	assert.NotEmpty(r.T(), errata)
	assert.Equal(r.T(), errata[0].Type, "bugfix")
	assert.Equal(r.T(), total, 6)

	// sorting by type desc
	errata, total, err = r.tangy.RpmRepositoryVersionErrataList(context.Background(), []string{*firstVersionHref}, tangy.ErrataListFilters{}, tangy.PageOptions{SortBy: "type:desc"})
	require.NoError(r.T(), err)
	assert.NotEmpty(r.T(), errata)
	assert.Equal(r.T(), errata[0].Type, "security")
	assert.Equal(r.T(), total, 6)

	// sorting by severity asc
	errata, total, err = r.tangy.RpmRepositoryVersionErrataList(context.Background(), []string{*firstVersionHref}, tangy.ErrataListFilters{}, tangy.PageOptions{SortBy: "severity:asc"})
	require.NoError(r.T(), err)
	assert.NotEmpty(r.T(), errata)
	assert.Equal(r.T(), errata[0].Severity, "") // some errata in this repo have no severity listed, these show up first when sorting ascending
	assert.Equal(r.T(), total, 6)

	// sorting by severity desc
	errata, total, err = r.tangy.RpmRepositoryVersionErrataList(context.Background(), []string{*firstVersionHref}, tangy.ErrataListFilters{}, tangy.PageOptions{SortBy: "severity:desc"})
	require.NoError(r.T(), err)
	assert.NotEmpty(r.T(), errata)
	assert.Equal(r.T(), errata[0].Severity, "Moderate")
	assert.Equal(r.T(), total, 6)
}

func (r *RpmSuite) TestRpmRepositoryVersionModuleStreamsList() {
	firstVersionHref := &r.firstVersionHref
	secondVersionHref := &r.secondVersionHref

	// Expect populated
	singleList, err := r.tangy.RpmRepositoryVersionModuleStreamsList(context.Background(), []string{*firstVersionHref}, tangy.ModuleStreamListFilters{}, "anything!")

	assert.Equal(r.T(), singleList[0].Name, "duck")
	require.NoError(r.T(), err)
	assert.NotEmpty(r.T(), singleList)

	// Test search
	singleList, err = r.tangy.RpmRepositoryVersionModuleStreamsList(context.Background(), []string{*firstVersionHref}, tangy.ModuleStreamListFilters{Search: "Duck"}, "")
	require.NoError(r.T(), err)
	assert.NotEmpty(r.T(), singleList)

	// Test module name list filter
	singleList, err = r.tangy.RpmRepositoryVersionModuleStreamsList(context.Background(), []string{*firstVersionHref}, tangy.ModuleStreamListFilters{RpmNames: []string{"walrus", "kangaroo"}}, "anything DesC")
	require.NoError(r.T(), err)
	assert.Equal(r.T(), singleList[0].Name, "walrus")
	assert.NotEmpty(r.T(), singleList)

	// Test module name list filter
	singleList, err = r.tangy.RpmRepositoryVersionModuleStreamsList(context.Background(), []string{*firstVersionHref}, tangy.ModuleStreamListFilters{RpmNames: []string{"walrus", "kangaroo"}}, "name ASC")
	require.NoError(r.T(), err)
	assert.Equal(r.T(), singleList[0].Name, "kangaroo")
	assert.NotEmpty(r.T(), singleList)

	// Confirm no error on not found rpm name
	singleList, err = r.tangy.RpmRepositoryVersionModuleStreamsList(context.Background(), []string{*firstVersionHref}, tangy.ModuleStreamListFilters{RpmNames: []string{"banana"}}, "")
	require.NoError(r.T(), err)
	assert.Empty(r.T(), singleList)

	// Test no duplicated module names
	singleList, err = r.tangy.RpmRepositoryVersionModuleStreamsList(context.Background(), []string{*firstVersionHref, *secondVersionHref}, tangy.ModuleStreamListFilters{RpmNames: []string{"walrus", "kangaroo"}}, "name DESC")
	require.NoError(r.T(), err)
	assert.Equal(r.T(), singleList[0].Name, "walrus")
	assert.Len(r.T(), singleList, 3)
}

func (r *RpmSuite) TestRpmRepositoryVersionPackageListNameFilter() {
	resp, err := r.client.GetRpmRepositoryByName(r.domainName, testRepoName)
	require.NoError(r.T(), err)
	latestVersionHref := resp.LatestVersionHref
	require.NotNil(r.T(), latestVersionHref)

	// no filter
	singleList, total, err := r.tangy.RpmRepositoryVersionPackageList(context.Background(), []string{*latestVersionHref}, tangy.RpmListFilters{Name: ""}, tangy.PageOptions{})
	require.NoError(r.T(), err)
	assert.NotEmpty(r.T(), singleList)
	assert.Equal(r.T(), 9, total)

	// exact match
	singleList, total, err = r.tangy.RpmRepositoryVersionPackageList(context.Background(), []string{*latestVersionHref}, tangy.RpmListFilters{Name: "bear"}, tangy.PageOptions{})
	require.NoError(r.T(), err)
	assert.NotEmpty(r.T(), singleList)
	assert.Equal(r.T(), total, 1)

	// partial match
	singleList, total, err = r.tangy.RpmRepositoryVersionPackageList(context.Background(), []string{*latestVersionHref}, tangy.RpmListFilters{Name: "bea"}, tangy.PageOptions{})
	require.NoError(r.T(), err)
	assert.NotEmpty(r.T(), singleList)
	assert.Equal(r.T(), total, 1)

	// no match
	singleList, total, err = r.tangy.RpmRepositoryVersionPackageList(context.Background(), []string{*latestVersionHref}, tangy.RpmListFilters{Name: "bat"}, tangy.PageOptions{})
	require.NoError(r.T(), err)
	assert.Empty(r.T(), singleList)
	assert.Equal(r.T(), total, 0)
}

// RpmRepositoryVersionPackageList
func (r *RpmSuite) TestRpmRepositoryVersionPackageListNoDuplicates() {
	firstVersionHref := r.firstVersionHref
	secondVersionHref := r.secondVersionHref

	doubleList, total, err := r.tangy.RpmRepositoryVersionPackageList(context.Background(), []string{firstVersionHref, secondVersionHref}, tangy.RpmListFilters{}, tangy.PageOptions{})
	require.NoError(r.T(), err)
	assert.NotEmpty(r.T(), doubleList)
	assert.Equal(r.T(), 12, total)

	singleList, total, err := r.tangy.RpmRepositoryVersionPackageList(context.Background(), []string{firstVersionHref}, tangy.RpmListFilters{}, tangy.PageOptions{})
	require.NoError(r.T(), err)
	assert.NotEmpty(r.T(), singleList)
	assert.Equal(r.T(), 7, total)
}

func (r *RpmSuite) TestRpmRepositoryVersionPackageListOffsetLimit() {
	firstVersionHref := r.firstVersionHref
	secondVersionHref := r.secondVersionHref

	list, total, err := r.tangy.RpmRepositoryVersionPackageList(context.Background(), []string{firstVersionHref, secondVersionHref}, tangy.RpmListFilters{}, tangy.PageOptions{Offset: 1, Limit: 4})
	require.NoError(r.T(), err)
	assert.NotEmpty(r.T(), list)
	assert.Equal(r.T(), 4, len(list))
	assert.Equal(r.T(), 12, total)

	list, total, err = r.tangy.RpmRepositoryVersionPackageList(context.Background(), []string{firstVersionHref, secondVersionHref}, tangy.RpmListFilters{}, tangy.PageOptions{Offset: 4, Limit: 1})
	require.NoError(r.T(), err)
	assert.NotEmpty(r.T(), list)
	assert.Equal(r.T(), 1, len(list))
	assert.Equal(r.T(), 12, total)

	list, total, err = r.tangy.RpmRepositoryVersionPackageList(context.Background(), []string{firstVersionHref, secondVersionHref}, tangy.RpmListFilters{}, tangy.PageOptions{Offset: 100, Limit: 100})
	require.NoError(r.T(), err)
	assert.Empty(r.T(), list)
	assert.Equal(r.T(), 0, len(list))
	assert.Equal(r.T(), 12, total)
}

func RandStringBytes(n int) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}
