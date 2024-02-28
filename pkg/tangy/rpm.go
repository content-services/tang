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

type RpmListItem struct {
	Id      string
	Name    string // The rpm package name
	Arch    string // The Architecture of the rpm
	Version string // The version of the  rpm
	Release string // The release of the rpm
	Epoch   string // The epoch of the rpm
	Summary string // The summary of the rpm
}

type PageOptions struct {
	Offset int
	Limit  int
}

type RpmListFilters struct {
	Name string
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

	repoVerMap, err := parseRepositoryVersionHrefsMap(hrefs)
	if err != nil {
		return []RpmPackageSearch{}, fmt.Errorf("error parsing repository version hrefs: %w", err)
	}

	args := pgx.NamedArgs{"nameFilter": "%" + search + "%", "limit": limit}
	innerUnion := contentIdsInVersions(repoVerMap, &args)

	query := `SELECT DISTINCT ON (rp.name) rp.name, rp.summary
              FROM rpm_package rp WHERE rp.content_ptr_id IN `

	rows, err := conn.Query(context.Background(), query+innerUnion+" AND rp.name ILIKE CONCAT( '%', @nameFilter::text, '%') ORDER BY rp.name  LIMIT @limit", args)
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

	repoVerMap, err := parseRepositoryVersionHrefsMap(hrefs)
	if err != nil {
		return []RpmPackageGroupSearch{}, fmt.Errorf("error parsing repository version hrefs: %w", err)
	}

	args := pgx.NamedArgs{"nameFilter": "%" + search + "%"}
	innerUnion := contentIdsInVersions(repoVerMap, &args)

	query := `SELECT DISTINCT ON (rp.name, rp.id, rp.packages) rp.name, rp.id, rp.description, rp.packages
              FROM rpm_packagegroup rp WHERE rp.content_ptr_id IN 
			`

	rows, err := conn.Query(ctx, query+innerUnion+"AND rp.name ILIKE CONCAT( '%', @nameFilter::text, '%') ORDER BY rp.name", args)
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
	for _, rpm := range rpms {
		nameId := rpm.Name + rpm.ID
		val, ok := pkgGroupMap[nameId]
		if ok {
			searchResult = append(searchResult, val)
		}
		delete(pkgGroupMap, nameId) // delete it so we don't add it again
	}

	if len(searchResult) <= limit {
		return searchResult, nil
	} else {
		return searchResult[0:limit], nil
	}
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

	repoVerMap, err := parseRepositoryVersionHrefsMap(hrefs)
	if err != nil {
		return []RpmEnvironmentSearch{}, fmt.Errorf("error parsing repository version hrefs: %w", err)
	}

	args := pgx.NamedArgs{"nameFilter": "%" + search + "%", "limit": limit}
	innerUnion := contentIdsInVersions(repoVerMap, &args)

	query := `SELECT DISTINCT ON (rp.name, rp.id) rp.name, rp.id, rp.description
              FROM rpm_packageenvironment rp WHERE rp.content_ptr_id IN 
			`

	rows, err := conn.Query(ctx, query+innerUnion+" AND rp.name ILIKE CONCAT( '%', @nameFilter::text, '%') ORDER BY rp.name  LIMIT @limit", args)
	if err != nil {
		return nil, err
	}
	rpms, err := pgx.CollectRows(rows, pgx.RowToStructByName[RpmEnvironmentSearch])
	if err != nil {
		return nil, err
	}

	return rpms, nil
}

// RpmRepositoryVersionPackageSearch search for RPMs, by name, associated to repository hrefs, returning an amount up to limit
func (t *tangyImpl) RpmRepositoryVersionPackageList(ctx context.Context, hrefs []string, filterOpts RpmListFilters, pageOpts PageOptions) ([]RpmListItem, int, error) {
	if len(hrefs) == 0 {
		return []RpmListItem{}, 0, nil
	}

	conn, err := t.pool.Acquire(ctx)
	if err != nil {
		return nil, 0, err
	}
	defer conn.Release()

	if pageOpts.Limit == 0 {
		pageOpts.Limit = DefaultLimit
	}

	repoVerMap, err := parseRepositoryVersionHrefsMap(hrefs)
	if err != nil {
		return nil, 0, fmt.Errorf("error parsing repository version hrefs: %w", err)
	}

	countQueryOpen := "select count(*) as total FROM rpm_package rp WHERE rp.content_ptr_id IN "
	args := pgx.NamedArgs{"nameFilter": "%" + filterOpts.Name + "%"}
	innerUnion := contentIdsInVersions(repoVerMap, &args)

	var countTotal int
	err = conn.QueryRow(ctx, countQueryOpen+innerUnion+" AND rp.name ILIKE CONCAT( '%', @nameFilter::text, '%')", args).Scan(&countTotal)
	if err != nil {
		return nil, 0, err
	}

	queryOpen := `SELECT rp.content_ptr_id as id, rp.name, rp.version, rp.arch, rp.release, rp.epoch, rp.summary
              FROM rpm_package rp WHERE rp.content_ptr_id IN `

	args["limit"] = pageOpts.Limit
	args["offset"] = pageOpts.Offset
	rows, err := conn.Query(ctx, queryOpen+innerUnion+
		" AND rp.name ILIKE CONCAT( '%', @nameFilter::text, '%') ORDER BY rp.name ASC, rp.version ASC, rp.release ASC, rp.arch ASC LIMIT @limit OFFSET @offset",
		args)
	if err != nil {
		return nil, 0, err
	}
	rpms, err := pgx.CollectRows(rows, pgx.RowToStructByName[RpmListItem])
	if err != nil {
		return nil, 0, err
	}
	return rpms, countTotal, nil
}

type ParsedRepoVersion struct {
	RepositoryUUID string
	Version        int
}

func parseRepositoryVersionHrefsMap(hrefs []string) (mapping []ParsedRepoVersion, err error) {
	mapping = []ParsedRepoVersion{}
	// /api/pulp/e1c6bee3/api/v3/repositories/rpm/rpm/018c1c95-4281-76eb-b277-842cbad524f4/versions/1/
	for _, href := range hrefs {
		splitHref := strings.Split(href, "/")
		if len(splitHref) < 10 {
			return mapping, fmt.Errorf("%v is not a valid href", splitHref)
		}
		id := splitHref[9]
		num := splitHref[11]

		_, err = uuid.Parse(id)
		if err != nil {
			return mapping, fmt.Errorf("%v is not a valid uuid", id)
		}

		ver, err := strconv.Atoi(num)
		if err != nil {
			return mapping, fmt.Errorf("%v is not a valid integer", num)
		}

		mapping = append(mapping, ParsedRepoVersion{
			RepositoryUUID: id,
			Version:        ver,
		})
	}
	return mapping, nil
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
