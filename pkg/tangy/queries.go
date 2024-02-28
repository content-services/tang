package tangy

import (
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"golang.org/x/exp/rand"
)

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

func contentIdsInVersions(repoVerMap map[string]int, namedArgs *pgx.NamedArgs) string {
	queries := []string{}
	for repo, ver := range repoVerMap {
		queries = append(queries, contentIdsInVersion(repo, ver, namedArgs))
	}
	return fmt.Sprintf("( %v ) ", strings.Join(queries, " UNION "))

}
