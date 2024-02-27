package tangy

import (
	"fmt"
	"strings"
)

func contentIdsInVersion(repoId string, versionNum int) string {
	query := `
                SELECT crc.content_id
                FROM core_repositorycontent crc
                INNER JOIN core_repositoryversion crv ON (crc.version_added_id = crv.pulp_id)
                LEFT OUTER JOIN core_repositoryversion crv2 ON (crc.version_removed_id = crv2.pulp_id)
                WHERE crv.repository_id = '%v' AND crv.number <= %v AND NOT (crv2.number <= %v AND crv2.number IS NOT NULL)
	`
	return fmt.Sprintf(query, repoId, versionNum, versionNum)
}

func contentIdsInVersions(repoVerMap map[string]int) string {
	queries := []string{}
	for repo, ver := range repoVerMap {
		queries = append(queries, contentIdsInVersion(repo, ver))
	}
	return fmt.Sprintf("( %v ) ", strings.Join(queries, " UNION "))

}
