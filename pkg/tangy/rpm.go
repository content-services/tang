package tangy

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5"
)

type RpmPackageSearch struct {
	Name    string
	Summary string
}

func (t *tangyImpl) RpmRepositoryVersionPackageSearch(ctx context.Context, hrefs []string, search string) ([]RpmPackageSearch, error) {
	conn, err := t.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Release()

	query := `
		SELECT DISTINCT ON (rp.name) rp.name, rp.summary
		FROM core_repositoryversion crv
		INNER JOIN core_repository cr on crv.repository_id = cr.pulp_id
		INNER JOIN core_repositorycontent crc on cr.pulp_id = crc.repository_id
		INNER JOIN core_content cc on crc.content_id = cc.pulp_id
		INNER JOIN rpm_package rp on cc.pulp_id = rp.content_ptr_id
		WHERE CONCAT(crv.repository_id, '/', crv.number) = ANY ($1)
		AND rp.name ILIKE CONCAT( '%', $2::text, '%')
		ORDER BY rp.name ASC
	`
	concatenatedIdAndVersion := parseRepositoryVersionHrefs(hrefs)
	rows, err := conn.Query(context.Background(), query, concatenatedIdAndVersion, search)
	if err != nil {
		return nil, err
	}
	rpms, err := pgx.CollectRows(rows, pgx.RowToStructByName[RpmPackageSearch])
	if err != nil {
		return nil, err
	}
	return rpms, nil
}

func parseRepositoryVersionHrefs(hrefs []string) (concatenatedIdAndVersion []string) {
	// /pulp/e1c6bee3/api/v3/repositories/rpm/rpm/018c1c95-4281-76eb-b277-842cbad524f4/versions/1/
	for _, href := range hrefs {
		splitHref := strings.Split(href, "/")
		concatenatedIdAndVersion = append(concatenatedIdAndVersion, splitHref[8]+"/"+splitHref[10])
	}
	return
}
