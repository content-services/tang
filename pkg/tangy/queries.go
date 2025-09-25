package tangy

import (
	"fmt"
	"math/rand/v2"
	"strings"

	"github.com/jackc/pgx/v5"
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
                (crv.repository_id = @%v AND crv.number = @%v AND crv.content_ids IS NOT NULL)
	`
	(*namedArgs)[repoIdName] = repoId
	(*namedArgs)[versionNumName] = versionNum
	return fmt.Sprintf(query, repoIdName, versionNumName)
}

// returns part of a query that joins a table to the needed tables to select content units in a given set of versions
//
//	 The return of this functions should be added to a query such as "select ** from TABLE rp" query,
//	 Where rp has a column 'content_ptr_id', such as rpm_updaterecord, rpm_package, etc.
//		Takes in a pointer to Named args in order to add required named arguments for the query.
func contentIdsInVersions(repoVerMap []ParsedRepoVersion, namedArgs *pgx.NamedArgs) string {
	mainQuery := ` 				
                INNER JOIN core_repositoryversion crv ON (rp.content_ptr_id = ANY(crv.content_ids))
                WHERE
                    `
	queries := []string{}
	for _, parsed := range repoVerMap {
		queries = append(queries, contentIdsInVersion(parsed.RepositoryUUID, parsed.Version, namedArgs))
	}
	return fmt.Sprintf("%v (%v)", mainQuery, strings.Join(queries, " OR "))
}
