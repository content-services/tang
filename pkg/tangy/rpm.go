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

type rpmPackageGroupSearchQueryReturn struct {
	ID          string
	Name        string
	Description string
	Packages    []map[string]any
}

type RpmPackageGroupSearch struct {
	ID          string
	Name        string
	Description string
	Packages    []string
}

type RpmEnvironmentSearch struct {
	ID          string
	Name        string
	Description string
}

// RpmRepositoryVersionPackageSearch search for RPMs, by name, associated to repository hrefs, returning an amount up to limit
func (t *tangyImpl) RpmRepositoryVersionPackageSearch(ctx context.Context, hrefs []string, search string, limit int) ([]RpmPackageSearch, error) {
	if len(hrefs) == 0 {
		return []RpmPackageSearch{}, nil
	}

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

	query := `SELECT DISTINCT ON (rp.name) rp.name, rp.summary
              FROM rpm_package rp WHERE rp.content_ptr_id IN (`

	query = buildSearchQuery(query, search, limit, repositoryIDs, versions)

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

// RpmRepositoryVersionPackageGroupSearch search for RPM Package Groups, by name, associated to repository hrefs, returning an amount up to limit
func (t *tangyImpl) RpmRepositoryVersionPackageGroupSearch(ctx context.Context, hrefs []string, search string, limit int) ([]RpmPackageGroupSearch, error) {
	if len(hrefs) == 0 {
		return []RpmPackageGroupSearch{}, nil
	}

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

	query := `SELECT DISTINCT ON (rp.name, rp.id, rp.packages) rp.name, rp.id, rp.description, rp.packages
              FROM rpm_packagegroup rp WHERE rp.content_ptr_id IN (
			`
	query = buildSearchQuery(query, search, limit, repositoryIDs, versions)

	rows, err := conn.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}
	rpms, err := pgx.CollectRows(rows, pgx.RowToStructByName[rpmPackageGroupSearchQueryReturn])
	if err != nil {
		return nil, err
	}

	var pkgGroupMap = make(map[string]RpmPackageGroupSearch, 0)
	for _, rpm := range rpms {
		nameId := rpm.Name + rpm.ID
		pkgGroup, groupExists := pkgGroupMap[nameId]
		if groupExists {
			newList, err := parsePackages(rpm.Packages)
			if err != nil {
				return nil, err
			}
			pkgGroup.Packages = unionSlices[string](newList, pkgGroupMap[nameId].Packages)
		} else {
			pkgGroup.ID = rpm.ID
			pkgGroup.Name = rpm.Name
			pkgGroup.Description = rpm.Description
			pkgGroup.Packages, err = parsePackages(rpm.Packages)
			if err != nil {
				return nil, err
			}
		}
		pkgGroupMap[nameId] = RpmPackageGroupSearch{
			ID:          pkgGroup.ID,
			Name:        pkgGroup.Name,
			Description: pkgGroup.Description,
			Packages:    pkgGroup.Packages,
		}
	}

	var searchResult []RpmPackageGroupSearch
	for _, pkgGroup := range pkgGroupMap {
		searchResult = append(searchResult, pkgGroup)
	}

	return searchResult, nil
}

// RpmRepositoryVersionEnvironmentSearch search for RPM Environments, by name, associated to repository hrefs, returning an amount up to limit
func (t *tangyImpl) RpmRepositoryVersionEnvironmentSearch(ctx context.Context, hrefs []string, search string, limit int) ([]RpmEnvironmentSearch, error) {
	if len(hrefs) == 0 {
		return []RpmEnvironmentSearch{}, nil
	}

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

	query := `SELECT DISTINCT ON (rp.name, rp.id) rp.name, rp.id, rp.description
              FROM rpm_packageenvironment rp WHERE rp.content_ptr_id IN (
			`
	query = buildSearchQuery(query, search, limit, repositoryIDs, versions)

	rows, err := conn.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}
	rpms, err := pgx.CollectRows(rows, pgx.RowToStructByName[RpmEnvironmentSearch])
	if err != nil {
		return nil, err
	}

	return rpms, nil
}

// buildSearchQuery builds search query for rpm package, package group, and environment search by name
func buildSearchQuery(queryFragment string, search string, limit int, repositoryIDs []string, versions []int) string {
	query := queryFragment
	for i := 0; i < len(repositoryIDs); i++ {
		id := repositoryIDs[i]
		ver := versions[i]

		query += fmt.Sprintf(`
			(
    		    SELECT crc.content_id
    		    FROM core_repositorycontent crc
    		    INNER JOIN core_repositoryversion crv ON (crc.version_added_id = crv.pulp_id)
    		    LEFT OUTER JOIN core_repositoryversion crv2 ON (crc.version_removed_id = crv2.pulp_id)
    		    WHERE crv.repository_id = '%v' AND crv.number <= %v AND NOT (crv2.number <= %v AND crv2.number IS NOT NULL)
				AND rp.name ILIKE CONCAT( '%%', '%v'::text, '%%')
            )
		`, id, ver, ver, search)

		if i == len(repositoryIDs)-1 {
			query += fmt.Sprintf(") ORDER BY rp.name ASC LIMIT %v;", limit)
			break
		}

		query += "UNION"
	}
	return query
}

func parseRepositoryVersionHrefs(hrefs []string) (repositoryIDs []string, versions []int, err error) {
	// /pulp/e1c6bee3/api/v3/repositories/rpm/rpm/018c1c95-4281-76eb-b277-842cbad524f4/versions/1/
	for _, href := range hrefs {
		splitHref := strings.Split(href, "/")
		if len(splitHref) < 10 {
			return nil, nil, fmt.Errorf("%v is not a valid href", splitHref)
		}
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

func parsePackages(pulpPackageList []map[string]any) ([]string, error) {
	var packageList []string
	for _, pkg := range pulpPackageList {
		pkgName, ok := pkg["name"].(string)
		if !ok {
			return nil, fmt.Errorf("name invalid for package in package list")
		}
		packageList = append(packageList, pkgName)
	}
	return packageList, nil
}

func unionSlices[T comparable](a []T, b []T) []T {
	var mapSet = make(map[T]bool, 0)
	for _, i := range a {
		mapSet[i] = true
	}
	for _, i := range b {
		if _, ok := mapSet[i]; !ok {
			a = append(a, i)
		}
	}
	return a
}
