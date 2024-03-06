package tangy

import (
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"golang.org/x/exp/rand"
)

// contentIdsInVersion forms a single query to fetch a list of content ids in a repository version
//
//	It uses randomized query parameter names and modifies the passed in namedArgs to include the key/values for these named query parameters.
//	By using randomized query parameter names, this query can be included multiple times with different repository
//	versions as multiple subqueries.
func contentIdsInVersion(repoId string, versionNum int, namedArgs *pgx.NamedArgs) string {
	ran := rand.Int()
	repoIdName := fmt.Sprintf("%v%v", "repoName", ran)
	versionNumName := fmt.Sprintf("%v%v", "versionNum", ran)
	query := `
                SELECT crc.content_id
                FROM core_repositorycontent crc
                INNER JOIN core_repositoryversion crv ON (crc.version_added_id = crv.pulp_id)
                LEFT OUTER JOIN core_repositoryversion crv2 ON (crc.version_removed_id = crv2.pulp_id)
                WHERE crv.repository_id = @%v AND crv.number <= @%v AND NOT (crv2.number <= @%v AND crv2.number IS NOT NULL)
	`
	(*namedArgs)[repoIdName] = repoId
	(*namedArgs)[versionNumName] = versionNum
	return fmt.Sprintf(query, repoIdName, versionNumName, versionNumName)
}

// Creates a sub query (including parenthesis) to lookup the content IDs of a list of repository versions.
//
//	Takes in a pointer to Named args in order to add required named arguments for the query.  Multiple queries are created
//	 and UNION'd together
func contentIdsInVersions(repoVerMap []ParsedRepoVersion, namedArgs *pgx.NamedArgs) string {
	queries := []string{}
	for _, parsed := range repoVerMap {
		queries = append(queries, contentIdsInVersion(parsed.RepositoryUUID, parsed.Version, namedArgs))
	}
	return fmt.Sprintf("( %v ) ", strings.Join(queries, " UNION "))
}
