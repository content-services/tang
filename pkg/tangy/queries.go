package tangy

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// contentIdsInVersionNew forms a single query to fetch a list of content ids in a repository version
// using the new content_ids array field (only works for versions created after August 1st, 2025)
//
//	It uses randomized query parameter names and modifies the passed in namedArgs to include the key/values for these named query parameters.
//	By using randomized query parameter names, this query can be included multiple times with different repository
//	versions as multiple subqueries.
func contentIdsInVersionNew(repoId string, versionNum int, namedArgs *pgx.NamedArgs) string {
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

// contentIdsInVersionOld forms a single query to fetch a list of content ids in a repository version
// using the old method with core_repositorycontent table (works for all versions)
//
//	TODO: DELETE THIS FUNCTION after August 1st, 2026 when all repository versions use content_ids
//	It uses randomized query parameter names and modifies the passed in namedArgs to include the key/values for these named query parameters.
//	By using randomized query parameter names, this query can be included multiple times with different repository
//	versions as multiple subqueries.
func contentIdsInVersionOld(repoId string, versionNum int, namedArgs *pgx.NamedArgs) string {
	ran := rand.Int()
	repoIdName := fmt.Sprintf("%v%v", "repoName", ran)
	versionNumName := fmt.Sprintf("%v%v", "versionNum", ran)
	query := `
                (crv.repository_id = @%v AND crv.number <= @%v AND NOT (crv2.number <= @%v AND crv2.number IS NOT NULL))
	`
	(*namedArgs)[repoIdName] = repoId
	(*namedArgs)[versionNumName] = versionNum
	return fmt.Sprintf(query, repoIdName, versionNumName, versionNumName)
}

// contentIdsInVersionsNew returns part of a query that joins a table to the needed tables to select content units
// in a given set of versions using the new content_ids array field
//
//	TODO: DELETE THIS FUNCTION after August 1st, 2026 when all repository versions use content_ids
//	 The return of this functions should be added to a query such as "select ** from TABLE rp" query,
//	 Where rp has a column 'content_ptr_id', such as rpm_updaterecord, rpm_package, etc.
//		Takes in a pointer to Named args in order to add required named arguments for the query.
func contentIdsInVersionsNew(repoVerMap []ParsedRepoVersion, namedArgs *pgx.NamedArgs) string {
	mainQuery := ` 				
                INNER JOIN core_repositoryversion crv ON (rp.content_ptr_id = ANY(crv.content_ids))
                WHERE
                    `
	queries := []string{}
	for _, parsed := range repoVerMap {
		queries = append(queries, contentIdsInVersionNew(parsed.RepositoryUUID, parsed.Version, namedArgs))
	}
	return fmt.Sprintf("%v (%v)", mainQuery, strings.Join(queries, " OR "))
}

// contentIdsInVersionsOld returns part of a query that joins a table to the needed tables to select content units
// in a given set of versions using the old method with core_repositorycontent table
//
//	TODO: DELETE THIS FUNCTION after August 1st, 2025 when all repository versions use content_ids
//	 The return of this functions should be added to a query such as "select ** from TABLE rp" query,
//	 Where rp has a column 'content_ptr_id', such as rpm_updaterecord, rpm_package, etc.
//		Takes in a pointer to Named args in order to add required named arguments for the query.
func contentIdsInVersionsOld(repoVerMap []ParsedRepoVersion, namedArgs *pgx.NamedArgs) string {
	mainQuery := ` 				
                INNER JOIN core_repositorycontent crc on rp.content_ptr_id = crc.content_id
                INNER JOIN core_repositoryversion crv ON (crc.version_added_id = crv.pulp_id)
                LEFT OUTER JOIN core_repositoryversion crv2 ON (crc.version_removed_id = crv2.pulp_id)
                WHERE
                    `
	queries := []string{}
	for _, parsed := range repoVerMap {
		queries = append(queries, contentIdsInVersionOld(parsed.RepositoryUUID, parsed.Version, namedArgs))
	}
	return fmt.Sprintf("%v (%v)", mainQuery, strings.Join(queries, " OR "))
}

// checkAllVersionsWithContentIds checks if all repository versions in the given map were created after the specified date
func checkAllVersionsWithContentIds(ctx context.Context, conn *pgxpool.Conn, repoVerMap []ParsedRepoVersion) (bool, error) {
	if len(repoVerMap) == 0 {
		return true, nil
	}

	// Build query to check content_ids dates of all repository versions
	queryParts := []string{}
	args := pgx.NamedArgs{}
	for i, parsed := range repoVerMap {
		repoIdParam := fmt.Sprintf("repoId%d", i)
		versionNumParam := fmt.Sprintf("versionNum%d", i)
		queryParts = append(queryParts, fmt.Sprintf("(crv.repository_id = @%s AND crv.number = @%s)", repoIdParam, versionNumParam))
		args[repoIdParam] = parsed.RepositoryUUID
		args[versionNumParam] = parsed.Version
	}

	query := fmt.Sprintf(`
		SELECT COUNT(*) 
		FROM core_repositoryversion crv 
		WHERE (%s) AND crv.content_ids is null
	`, strings.Join(queryParts, " OR "))

	var count int
	err := conn.QueryRow(ctx, query, args).Scan(&count)
	if err != nil {
		return false, err
	}

	// If count is 0, all versions are after the cutoff date
	return count == 0, nil
}

// returns part of a query that joins a table to the needed tables to select content units in a given set of versions
//
//	 The return of this functions should be added to a query such as "select ** from TABLE rp" query,
//	 Where rp has a column 'content_ptr_id', such as rpm_updaterecord, rpm_package, etc.
//		Takes in a pointer to Named args in order to add required named arguments for the query.
//		This function automatically chooses between the old and new query methods based on repository version creation dates.
func contentIdsInVersions(ctx context.Context, conn *pgxpool.Conn, repoVerMap []ParsedRepoVersion, namedArgs *pgx.NamedArgs) (string, error) {
	// Check if all versions are after the cutoff date, not needed after August 1st, 2026
	useNewMethod, err := checkAllVersionsWithContentIds(ctx, conn, repoVerMap)
	if err != nil {
		return "", fmt.Errorf("error checking repository version dates: %w", err)
	}

	if useNewMethod {
		return contentIdsInVersionsNew(repoVerMap, namedArgs), nil
	}
	return contentIdsInVersionsOld(repoVerMap, namedArgs), nil
}
