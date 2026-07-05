package tangy

import (
	"context"
	"encoding/json"
	"errors"
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

// PythonPackageDetail holds metadata for a specific package name and version in a repository.
// Metadata is taken from one representative distribution (sdist preferred, then most recently synced).
type PythonPackageDetail struct {
	Name                   string              `json:"name"`
	NameNormalized         string              `json:"name_normalized"`
	Version                string              `json:"version"`
	Summary                string              `json:"summary"`
	Description            string              `json:"description"`
	DescriptionContentType string              `json:"description_content_type,omitempty"`
	Author                 string              `json:"author"`
	AuthorEmail            string              `json:"author_email,omitempty"`
	Maintainer             string              `json:"maintainer,omitempty"`
	MaintainerEmail        string              `json:"maintainer_email,omitempty"`
	License                string              `json:"license"`
	LicenseExpression      string              `json:"license_expression,omitempty"`
	HomePage               string              `json:"home_page,omitempty"`
	ProjectURL             string              `json:"project_url"`
	ProjectURLs            map[string]string   `json:"project_urls,omitempty"`
	Keywords               string              `json:"keywords,omitempty"`
	RequiresPython         string              `json:"requires_python,omitempty"`
	Classifiers            []string            `json:"classifiers,omitempty"`
	RequiresDist           []string            `json:"requires_dist,omitempty"`
	LastUpdated            string                      `json:"last_updated"`
	Versions               []string                    `json:"versions"`
	LatestVersions         []PythonVersionInfo         `json:"latest_versions"`
	Distributions          []PythonDistributionListItem `json:"distributions"`
}

var ErrPythonPackageNotFound = errors.New("python package not found")

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

type pythonPackageDetailRow struct {
	Name                   string
	NameNormalized         string
	Version                string
	Summary                string
	Description            string
	DescriptionContentType string
	Author                 string
	AuthorEmail            string
	Maintainer             string
	MaintainerEmail        string
	License                string
	LicenseExpression      string
	HomePage               string
	ProjectURL             string
	ProjectURLs            []byte
	Keywords               string
	RequiresPython         string
	Classifiers            []byte
	RequiresDist           []byte
	LastUpdated            time.Time
	Versions               []string
	LatestVersionsJSON     []byte
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

	distributions, err := fetchPythonDistributionRows(ctx, conn, innerUnion, args, pageOpts.Limit, pageOpts.Offset)
	if err != nil {
		return PythonDistributionListResponse{}, err
	}

	return PythonDistributionListResponse{
		Results: pythonDistributionRowsToItems(distributions),
		Total:   countTotal,
		Limit:   pageOpts.Limit,
		Offset:  pageOpts.Offset,
	}, nil
}

// PythonPackageGet returns metadata for a specific package name_normalized and version
// from the latest version of a repository, plus all other versions available in that repository.
func (t *tangyImpl) PythonPackageGet(ctx context.Context, repositoryHref, nameNormalized, version string) (PythonPackageDetail, error) {
	if repositoryHref == "" {
		return PythonPackageDetail{}, nil
	}

	conn, err := t.pool.Acquire(ctx)
	if err != nil {
		return PythonPackageDetail{}, err
	}
	defer conn.Release()

	repoUUID, err := parsePythonRepositoryHref(repositoryHref)
	if err != nil {
		return PythonPackageDetail{}, fmt.Errorf("error parsing repository href: %w", err)
	}

	latestVersion, err := getLatestPythonRepositoryVersion(ctx, conn, repoUUID)
	if err != nil {
		return PythonPackageDetail{}, fmt.Errorf("error getting latest repository version: %w", err)
	}

	repoVerMap := []ParsedRepoVersion{{
		RepositoryUUID: repoUUID,
		Version:        latestVersion,
	}}

	args := pgx.NamedArgs{
		"name_normalized": nameNormalized,
		"version":         version,
	}
	innerUnion, err := contentIdsInVersions(ctx, conn, repoVerMap, &args)
	if err != nil {
		return PythonPackageDetail{}, err
	}

	query := `
		WITH filtered AS (
			SELECT rp.name, rp.name_normalized, rp.version, rp.summary, rp.description,
			       rp.description_content_type, rp.author, rp.author_email,
			       rp.maintainer, rp.maintainer_email, rp.license, rp.license_expression,
			       rp.home_page, rp.project_url, rp.project_urls, rp.keywords,
			       rp.requires_python, rp.classifiers, rp.requires_dist,
			       rp.packagetype, cc.pulp_created
			FROM python_pythonpackagecontent rp
			INNER JOIN core_content cc ON rp.content_ptr_id = cc.pulp_id
	` + innerUnion + `
			AND rp.name_normalized = @name_normalized
		),
		version_agg AS (
			SELECT
				ARRAY_AGG(version ORDER BY version) AS versions,
				COALESCE(
					JSON_AGG(
						JSON_BUILD_OBJECT('version', version, 'created_at', last_updated)
						ORDER BY version
					),
					'[]'::json
				) AS latest_versions_json
			FROM (
				SELECT version, MAX(pulp_created) AS last_updated
				FROM filtered
				GROUP BY version
			) v
		),
		detail AS (
			SELECT f.*,
			       MAX(f.pulp_created) OVER () AS last_updated,
			       ROW_NUMBER() OVER (
			           ORDER BY CASE WHEN f.packagetype = 'sdist' THEN 0 ELSE 1 END,
			                    f.pulp_created DESC
			       ) AS rn
			FROM filtered f
			WHERE f.version = @version
		)
		SELECT d.name, d.name_normalized, d.version, d.summary, d.description,
		       d.description_content_type, d.author, d.author_email,
		       d.maintainer, d.maintainer_email, d.license, d.license_expression,
		       d.home_page, d.project_url, d.project_urls, d.keywords,
		       d.requires_python, d.classifiers, d.requires_dist,
		       d.last_updated, va.versions, va.latest_versions_json
		FROM detail d
		CROSS JOIN version_agg va
		WHERE d.rn = 1`

	rows, err := conn.Query(ctx, query, args)
	if err != nil {
		return PythonPackageDetail{}, err
	}

	row, err := pgx.CollectOneRow(rows, pgx.RowToStructByName[pythonPackageDetailRow])
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return PythonPackageDetail{}, fmt.Errorf("%w: %s@%s", ErrPythonPackageNotFound, nameNormalized, version)
		}
		return PythonPackageDetail{}, err
	}

	latestVersions, err := parsePythonLatestVersionsJSON(row.LatestVersionsJSON)
	if err != nil {
		return PythonPackageDetail{}, err
	}

	distributions, err := fetchPythonDistributionRows(ctx, conn, innerUnion, args, 0, 0)
	if err != nil {
		return PythonPackageDetail{}, err
	}

	return PythonPackageDetail{
		Name:                   row.Name,
		NameNormalized:         row.NameNormalized,
		Version:                row.Version,
		Summary:                row.Summary,
		Description:            row.Description,
		DescriptionContentType: row.DescriptionContentType,
		Author:                 row.Author,
		AuthorEmail:            row.AuthorEmail,
		Maintainer:             row.Maintainer,
		MaintainerEmail:        row.MaintainerEmail,
		License:                row.License,
		LicenseExpression:      row.LicenseExpression,
		HomePage:               row.HomePage,
		ProjectURL:             row.ProjectURL,
		ProjectURLs:            parsePythonJSONStringMap(row.ProjectURLs),
		Keywords:               row.Keywords,
		RequiresPython:         row.RequiresPython,
		Classifiers:            parsePythonJSONStringSlice(row.Classifiers),
		RequiresDist:           parsePythonJSONStringSlice(row.RequiresDist),
		LastUpdated:            row.LastUpdated.Format(time.RFC3339),
		Versions:               row.Versions,
		LatestVersions:         latestVersions,
		Distributions:          pythonDistributionRowsToItems(distributions),
	}, nil
}

func fetchPythonDistributionRows(ctx context.Context, conn *pgxpool.Conn, innerUnion string, args pgx.NamedArgs, limit, offset int) ([]pythonDistributionRow, error) {
	query := `
		SELECT rp.name, rp.name_normalized, rp.version, rp.filename, rp.packagetype,
		       rp.python_version, rp.sha256, rp.size, cc.pulp_created AS created_at
		FROM python_pythonpackagecontent rp
		INNER JOIN core_content cc ON rp.content_ptr_id = cc.pulp_id
	` + innerUnion + `
		AND rp.name_normalized = @name_normalized
		AND rp.version = @version
		ORDER BY cc.pulp_created DESC`

	if limit > 0 {
		args["limit"] = limit
		args["offset"] = offset
		query += `
		LIMIT @limit OFFSET @offset`
	}

	rows, err := conn.Query(ctx, query, args)
	if err != nil {
		return nil, err
	}

	return pgx.CollectRows(rows, pgx.RowToStructByName[pythonDistributionRow])
}

func pythonDistributionRowsToItems(distributions []pythonDistributionRow) []PythonDistributionListItem {
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
	return results
}

func parsePythonLatestVersionsJSON(data []byte) ([]PythonVersionInfo, error) {
	if len(data) == 0 {
		return nil, nil
	}

	var raw []struct {
		Version   string    `json:"version"`
		CreatedAt time.Time `json:"created_at"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("failed to parse latest_versions: %w", err)
	}

	latestVersions := make([]PythonVersionInfo, len(raw))
	for i, item := range raw {
		latestVersions[i] = PythonVersionInfo{
			Version:   item.Version,
			CreatedAt: item.CreatedAt.Format(time.RFC3339),
		}
	}
	return latestVersions, nil
}

func parsePythonJSONStringSlice(data []byte) []string {
	if len(data) == 0 || string(data) == "null" {
		return nil
	}

	var values []string
	if err := json.Unmarshal(data, &values); err != nil {
		return nil
	}
	return values
}

func parsePythonJSONStringMap(data []byte) map[string]string {
	if len(data) == 0 || string(data) == "null" {
		return nil
	}

	var values map[string]string
	if err := json.Unmarshal(data, &values); err != nil {
		return nil
	}
	return values
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
