package tangy

import (
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestContentIdsInVersionNew(t *testing.T) {
	t.Parallel()

	args := pgx.NamedArgs{}
	query := contentIdsInVersionNew(testRepoVersionUUID, 3, &args)

	assert.Contains(t, query, "crv.repository_id = @repoName")
	assert.Contains(t, query, "crv.number = @versionNum")
	assert.Contains(t, query, "crv.content_ids IS NOT NULL")
	require.Len(t, args, 2)
	assert.Contains(t, namedArgValues(args), testRepoVersionUUID)
	assert.Contains(t, namedArgValues(args), 3)
}

func TestContentIdsInVersionOld(t *testing.T) {
	t.Parallel()

	args := pgx.NamedArgs{}
	query := contentIdsInVersionOld(testRepoVersionUUID, 3, &args)

	assert.Contains(t, query, "crv.repository_id = @repoName")
	assert.Contains(t, query, "crv.number <= @versionNum")
	assert.Contains(t, query, "crv2.number IS NOT NULL")
	require.Len(t, args, 2)
	assert.Contains(t, namedArgValues(args), testRepoVersionUUID)
	assert.Contains(t, namedArgValues(args), 3)
}

func TestContentIdsInVersionsNew(t *testing.T) {
	t.Parallel()

	args := pgx.NamedArgs{}
	secondUUID := "019f3808-fcc2-716e-a7d3-e5a7ef1522a0"
	repoVerMap := []ParsedRepoVersion{
		{RepositoryUUID: testRepoVersionUUID, Version: 1},
		{RepositoryUUID: secondUUID, Version: 2},
	}

	query := contentIdsInVersionsNew(repoVerMap, &args)

	assert.Contains(t, query, "INNER JOIN core_repositoryversion crv ON (rp.content_ptr_id = ANY(crv.content_ids))")
	assert.Contains(t, query, " OR ")
	require.Len(t, args, 4)
	values := namedArgValues(args)
	assert.Contains(t, values, testRepoVersionUUID)
	assert.Contains(t, values, secondUUID)
	assert.Contains(t, values, 1)
	assert.Contains(t, values, 2)
}

func TestContentIdsInVersionsOld(t *testing.T) {
	t.Parallel()

	args := pgx.NamedArgs{}
	repoVerMap := []ParsedRepoVersion{
		{RepositoryUUID: testRepoVersionUUID, Version: 1},
	}

	query := contentIdsInVersionsOld(repoVerMap, &args)

	assert.Contains(t, query, "INNER JOIN core_repositorycontent crc on rp.content_ptr_id = crc.content_id")
	assert.Contains(t, query, "LEFT OUTER JOIN core_repositoryversion crv2")
	require.Len(t, args, 2)
	assert.Contains(t, namedArgValues(args), testRepoVersionUUID)
	assert.Contains(t, namedArgValues(args), 1)
}

func namedArgValues(args pgx.NamedArgs) []any {
	values := make([]any, 0, len(args))
	for _, value := range args {
		values = append(values, value)
	}
	return values
}
