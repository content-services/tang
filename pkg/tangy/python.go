package tangy

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PythonVersionInfo struct {
	Version   string `json:"version"`
	CreatedAt string `json:"created_at"`
}

type PythonPackageListItem struct {
	Name           string              `json:"name"`
	NameNormalized string              `json:"name_normalized"`
	Versions       []string            `json:"versions"`
	LatestVersions []PythonVersionInfo `json:"latest_versions"`
}

type PythonPackageListResponse struct {
	Results []PythonPackageListItem `json:"results"`
	Total   int                     `json:"total"`
	Limit   int                     `json:"limit"`
	Offset  int                     `json:"offset"`
}

type PythonPackageListFilters struct {
	Search string
}

type PythonDistributionListItem struct {
	Name           string `json:"name"`
	NameNormalized string `json:"name_normalized"`
	Version        string `json:"version"`
	Filename       string `json:"filename"`
	PackageType    string `json:"packagetype"`
	PythonVersion  string `json:"python_version"`
	Sha256         string `json:"sha256"`
	Size           int64  `json:"size"`
	CreatedAt      string `json:"created_at"`
}

type PythonDistributionListResponse struct {
	Results []PythonDistributionListItem `json:"results"`
	Total   int                          `json:"total"`
	Limit   int                          `json:"limit"`
	Offset  int                          `json:"offset"`
}

type pythonPackageVersionRow struct {
	NameNormalized string
	Name           string
	Version        string
	CreatedAt      time.Time
}

type pythonDistributionRow struct {
	Name           string
	NameNormalized string
	Version        string
	Filename       string
	PackageType    string
	PythonVersion  string
	Sha256         string
	Size           int64
	CreatedAt      time.Time
}

// PythonPackageList lists Python packages from the latest version of a repository,
// grouped by name_normalized with SQL-level pagination.
func (t *tangyImpl) PythonPackageList(ctx context.Context, repositoryHref string, filterOpts PythonPackageListFilters, pageOpts PageOptions) (PythonPackageListResponse, error) {
	if repositoryHref == "" {
		return PythonPackageListResponse{}, nil
	}

	conn, err := t.pool.Acquire(ctx)
	if err != nil {
		return PythonPackageListResponse{}, err
	}
	defer conn.Release()

	if pageOpts.Limit == 0 {
		pageOpts.Limit = DefaultLimit
	}

	repoUUID, err := parsePythonRepositoryHref(repositoryHref)
	if err != nil {
		return PythonPackageListResponse{}, fmt.Errorf("error parsing repository href: %w", err)
	}

	latestVersion, err := getLatestPythonRepositoryVersion(ctx, conn, repoUUID)
	if err != nil {
		return PythonPackageListResponse{}, fmt.Errorf("error getting latest repository version: %w", err)
	}

	repoVerMap := []ParsedRepoVersion{{
		RepositoryUUID: repoUUID,
		Version:        latestVersion,
	}}

	args := pgx.NamedArgs{
		"limit":  pageOpts.Limit,
		"offset": pageOpts.Offset,
	}
	searchFilter := ""
	if filterOpts.Search != "" {
		args["searchFilter"] = filterOpts.Search
		searchFilter = ` AND (rp.name ILIKE CONCAT(@searchFilter::text, '%')
			OR rp.name_normalized ILIKE CONCAT(@searchFilter::text, '%'))`
	}
	innerUnion, err := contentIdsInVersions(ctx, conn, repoVerMap, &args)
	if err != nil {
		return PythonPackageListResponse{}, err
	}

	countQuery := `
		SELECT COUNT(DISTINCT rp.name_normalized)
		FROM python_pythonpackagecontent rp
	` + innerUnion + searchFilter

	var countTotal int
	err = conn.QueryRow(ctx, countQuery, args).Scan(&countTotal)
	if err != nil {
		return PythonPackageListResponse{}, err
	}

	query := `
		WITH filtered AS (
			SELECT rp.name_normalized, rp.name, rp.version, cc.pulp_created
			FROM python_pythonpackagecontent rp
			INNER JOIN core_content cc ON rp.content_ptr_id = cc.pulp_id
	` + innerUnion + searchFilter + `
		),
		package_versions AS (
			SELECT name_normalized, MIN(name) AS name, version, MAX(pulp_created) AS created_at
			FROM filtered
			GROUP BY name_normalized, version
		),
		paginated_packages AS (
			SELECT name_normalized, MIN(name) AS name
			FROM package_versions
			GROUP BY name_normalized
			ORDER BY name_normalized
			LIMIT @limit OFFSET @offset
		)
		SELECT pv.name_normalized, pv.name, pv.version, pv.created_at
		FROM package_versions pv
		INNER JOIN paginated_packages pp ON pv.name_normalized = pp.name_normalized
		ORDER BY pv.name_normalized, pv.version`

	rows, err := conn.Query(ctx, query, args)
	if err != nil {
		return PythonPackageListResponse{}, err
	}

	versionRows, err := pgx.CollectRows(rows, pgx.RowToStructByName[pythonPackageVersionRow])
	if err != nil {
		return PythonPackageListResponse{}, err
	}

	return PythonPackageListResponse{
		Results: assemblePythonPackageListFromRows(versionRows),
		Total:   countTotal,
		Limit:   pageOpts.Limit,
		Offset:  pageOpts.Offset,
	}, nil
}

// PythonDistributionList lists all distribution files for a specific package name and version
// from the latest version of a repository. The name filter uses name_normalized (PEP 503).
func (t *tangyImpl) PythonDistributionList(ctx context.Context, repositoryHref, nameNormalized, version string, pageOpts PageOptions) (PythonDistributionListResponse, error) {
	if repositoryHref == "" {
		return PythonDistributionListResponse{}, nil
	}

	conn, err := t.pool.Acquire(ctx)
	if err != nil {
		return PythonDistributionListResponse{}, err
	}
	defer conn.Release()

	if pageOpts.Limit == 0 {
		pageOpts.Limit = DefaultLimit
	}

	repoUUID, err := parsePythonRepositoryHref(repositoryHref)
	if err != nil {
		return PythonDistributionListResponse{}, fmt.Errorf("error parsing repository href: %w", err)
	}

	latestVersion, err := getLatestPythonRepositoryVersion(ctx, conn, repoUUID)
	if err != nil {
		return PythonDistributionListResponse{}, fmt.Errorf("error getting latest repository version: %w", err)
	}

	repoVerMap := []ParsedRepoVersion{{
		RepositoryUUID: repoUUID,
		Version:        latestVersion,
	}}

	args := pgx.NamedArgs{
		"name_normalized": nameNormalized,
		"version":         version,
		"limit":           pageOpts.Limit,
		"offset":          pageOpts.Offset,
	}
	innerUnion, err := contentIdsInVersions(ctx, conn, repoVerMap, &args)
	if err != nil {
		return PythonDistributionListResponse{}, err
	}

	countQuery := `
		SELECT COUNT(*)
		FROM python_pythonpackagecontent rp
	` + innerUnion + `
		AND rp.name_normalized = @name_normalized
		AND rp.version = @version`

	var countTotal int
	err = conn.QueryRow(ctx, countQuery, args).Scan(&countTotal)
	if err != nil {
		return PythonDistributionListResponse{}, err
	}

	query := `
		SELECT rp.name, rp.name_normalized, rp.version, rp.filename, rp.packagetype,
		       rp.python_version, rp.sha256, rp.size, cc.pulp_created AS created_at
		FROM python_pythonpackagecontent rp
		INNER JOIN core_content cc ON rp.content_ptr_id = cc.pulp_id
	` + innerUnion + `
		AND rp.name_normalized = @name_normalized
		AND rp.version = @version
		ORDER BY cc.pulp_created DESC
		LIMIT @limit OFFSET @offset`

	rows, err := conn.Query(ctx, query, args)
	if err != nil {
		return PythonDistributionListResponse{}, err
	}

	distributions, err := pgx.CollectRows(rows, pgx.RowToStructByName[pythonDistributionRow])
	if err != nil {
		return PythonDistributionListResponse{}, err
	}

	results := make([]PythonDistributionListItem, len(distributions))
	for i, dist := range distributions {
		results[i] = PythonDistributionListItem{
			Name:           dist.Name,
			NameNormalized: dist.NameNormalized,
			Version:        dist.Version,
			Filename:       dist.Filename,
			PackageType:    dist.PackageType,
			PythonVersion:  dist.PythonVersion,
			Sha256:         dist.Sha256,
			Size:           dist.Size,
			CreatedAt:      dist.CreatedAt.Format(time.RFC3339),
		}
	}

	return PythonDistributionListResponse{
		Results: results,
		Total:   countTotal,
		Limit:   pageOpts.Limit,
		Offset:  pageOpts.Offset,
	}, nil
}

func assemblePythonPackageListFromRows(rows []pythonPackageVersionRow) []PythonPackageListItem {
	if len(rows) == 0 {
		return nil
	}

	results := make([]PythonPackageListItem, 0)
	var current PythonPackageListItem

	for i, row := range rows {
		if i == 0 || row.NameNormalized != current.NameNormalized {
			if i > 0 {
				results = append(results, current)
			}
			current = PythonPackageListItem{
				Name:           row.Name,
				NameNormalized: row.NameNormalized,
				Versions:       []string{row.Version},
				LatestVersions: []PythonVersionInfo{{
					Version:   row.Version,
					CreatedAt: row.CreatedAt.Format(time.RFC3339),
				}},
			}
			continue
		}

		current.Versions = append(current.Versions, row.Version)
		current.LatestVersions = append(current.LatestVersions, PythonVersionInfo{
			Version:   row.Version,
			CreatedAt: row.CreatedAt.Format(time.RFC3339),
		})
	}

	return append(results, current)
}

// parsePythonRepositoryHref extracts the repository UUID from a Python repository href.
// Example: /api/pulp/default/api/v3/repositories/python/python/018c1c95-4281-76eb-b277-842cbad524f4/
func parsePythonRepositoryHref(href string) (string, error) {
	parts := strings.Split(href, "/")
	var nonEmptyParts []string
	for _, part := range parts {
		if part != "" {
			nonEmptyParts = append(nonEmptyParts, part)
		}
	}

	if len(nonEmptyParts) < 8 {
		return "", fmt.Errorf("invalid repository href format: %s", href)
	}

	return nonEmptyParts[len(nonEmptyParts)-1], nil
}

func getLatestPythonRepositoryVersion(ctx context.Context, conn *pgxpool.Conn, repoUUID string) (int, error) {
	query := `
		SELECT MAX(number)
		FROM core_repositoryversion
		WHERE repository_id = $1 AND complete = true
	`

	var latestVersion int
	err := conn.QueryRow(ctx, query, repoUUID).Scan(&latestVersion)
	if err != nil {
		return 0, fmt.Errorf("failed to get latest version for repository %s: %w", repoUUID, err)
	}

	return latestVersion, nil
}
