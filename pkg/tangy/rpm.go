package tangy

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const DefaultLimit = 500

type RpmPackageSearch struct {
	Name    string
	Summary string
}

// RpmRepositoryVersionPackageSearch search for RPMs, by name, associated to repository hrefs, returning an amount up to limit
func (t *tangyImpl) RpmRepositoryVersionPackageSearch(ctx context.Context, hrefs []string, search string, limit int) ([]RpmPackageSearch, error) {
	conn, err := t.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Release()

	if limit == 0 {
		limit = DefaultLimit
	}

	repositoryIDs, versions, err := parseRepositoryVersionHrefs(hrefs)
	if err != nil {
		return nil, fmt.Errorf("error parsing repository version hrefs: %w", err)
	}

	var query string
	for i := 0; i < len(repositoryIDs); i++ {
		id := repositoryIDs[i]
		ver := versions[i]

		query += fmt.Sprintf(`
			SELECT DISTINCT ON (rp.name) rp.name, rp.summary
		    FROM rpm_package rp
		    WHERE rp.content_ptr_id IN (
    		    SELECT crc.content_id
    		    FROM core_repositorycontent crc
    		    INNER JOIN core_repositoryversion crv ON (crc.version_added_id = crv.pulp_id)
    		    LEFT OUTER JOIN core_repositoryversion crv2 ON (crc.version_removed_id = crv2.pulp_id)
    		    WHERE crv.repository_id = '%v' AND crv.number <= %v AND NOT (crv2.number <= %v AND crv2.number IS NOT NULL)
				AND rp.name ILIKE CONCAT( '%%', '%v'::text, '%%') ORDER BY rp.name ASC LIMIT %v
            )
		`, id, ver, ver, search, limit)

		if i == len(repositoryIDs)-1 {
			query += ";"
			break
		}

		query += "UNION"
	}
	rows, err := conn.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}
	rpms, err := pgx.CollectRows(rows, pgx.RowToStructByName[RpmPackageSearch])
	if err != nil {
		return nil, err
	}
	return rpms, nil
}

func parseRepositoryVersionHrefs(hrefs []string) (repositoryIDs []string, versions []int, err error) {
	// /pulp/e1c6bee3/api/v3/repositories/rpm/rpm/018c1c95-4281-76eb-b277-842cbad524f4/versions/1/
	for _, href := range hrefs {
		splitHref := strings.Split(href, "/")
		id := splitHref[8]
		num := splitHref[10]

		_, err = uuid.Parse(id)
		if err != nil {
			return nil, nil, fmt.Errorf("%v is not a valid uuid", id)
		}

		ver, err := strconv.Atoi(num)
		if err != nil {
			return nil, nil, fmt.Errorf("%v is not a valid integer", num)
		}

		repositoryIDs = append(repositoryIDs, id)
		versions = append(versions, ver)
	}
	return
}
