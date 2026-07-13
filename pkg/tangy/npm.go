package tangy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type NpmVersionInfo struct {
	Version   string `json:"version"`
	CreatedAt string `json:"created_at"`
}

type NpmPackageListItem struct {
	Name           string           `json:"name"`
	Versions       []string         `json:"versions"`
	LatestVersions []NpmVersionInfo `json:"latest_versions"`
}

type NpmPackageListResponse struct {
	Results []NpmPackageListItem `json:"results"`
	Total   int                  `json:"total"`
	Limit   int                  `json:"limit"`
	Offset  int                  `json:"offset"`
}

type NpmPackageListFilters struct {
	Search string
}

type NpmBuildListItem struct {
	Name      string `json:"name"`
	Version   string `json:"version"`
	CreatedAt string `json:"created_at"`
}

type NpmBuildListResponse struct {
	Results []NpmBuildListItem `json:"results"`
	Total   int                `json:"total"`
	Limit   int                `json:"limit"`
	Offset  int                `json:"offset"`
}

type NpmTarballInfo struct {
	RelativePath string `json:"relative_path"`
	Filename     string `json:"filename"`
	Sha256       string `json:"sha256"`
	Size         int64  `json:"size"`
}

type NpmPackageDetail struct {
	Name           string           `json:"name"`
	Version        string           `json:"version"`
	CreatedAt      string           `json:"created_at"`
	Tarball        NpmTarballInfo   `json:"tarball"`
	Versions       []string         `json:"versions"`
	LatestVersions []NpmVersionInfo `json:"latest_versions"`
}

var ErrNpmPackageNotFound = errors.New("npm package not found")

type npmPackageVersionRow struct {
	Name      string
	Version   string
	CreatedAt time.Time
}

type npmPackageDetailRow struct {
	Name               string
	Version            string
	CreatedAt          time.Time
	RelativePath       *string
	Sha256             *string
	Size               *int64
	Versions           []string
	LatestVersionsJSON []byte
}

// NpmPackageList lists npm packages from the latest version of a repository,
// grouped by name with SQL-level pagination.
func (t *tangyImpl) NpmPackageList(ctx context.Context, repositoryHref string, filterOpts NpmPackageListFilters, pageOpts PageOptions) (NpmPackageListResponse, error) {
	if repositoryHref == "" {
		return NpmPackageListResponse{}, nil
	}

	conn, err := t.pool.Acquire(ctx)
	if err != nil {
		return NpmPackageListResponse{}, err
	}
	defer conn.Release()

	if pageOpts.Limit == 0 {
		pageOpts.Limit = DefaultLimit
	}

	repoUUID, err := parseNpmRepositoryHref(repositoryHref)
	if err != nil {
		return NpmPackageListResponse{}, fmt.Errorf("error parsing repository href: %w", err)
	}

	latestVersion, err := getLatestRepositoryVersion(ctx, conn, repoUUID)
	if err != nil {
		return NpmPackageListResponse{}, fmt.Errorf("error getting latest repository version: %w", err)
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
		searchFilter = ` AND rp.name ILIKE CONCAT(@searchFilter::text, '%')`
	}
	innerUnion, err := contentIdsInVersions(ctx, conn, repoVerMap, &args)
	if err != nil {
		return NpmPackageListResponse{}, err
	}

	countQuery := `
		SELECT COUNT(DISTINCT rp.name)
		FROM npm_package rp
	` + innerUnion + searchFilter

	var countTotal int
	err = conn.QueryRow(ctx, countQuery, args).Scan(&countTotal)
	if err != nil {
		return NpmPackageListResponse{}, err
	}

	query := `
		WITH filtered AS (
			SELECT rp.name, rp.version, cc.pulp_created
			FROM npm_package rp
			INNER JOIN core_content cc ON rp.content_ptr_id = cc.pulp_id
	` + innerUnion + searchFilter + `
		),
		package_versions AS (
			SELECT name, version, MAX(pulp_created) AS created_at
			FROM filtered
			GROUP BY name, version
		),
		paginated_packages AS (
			SELECT name
			FROM package_versions
			GROUP BY name
			ORDER BY name
			LIMIT @limit OFFSET @offset
		)
		SELECT pv.name, pv.version, pv.created_at
		FROM package_versions pv
		INNER JOIN paginated_packages pp ON pv.name = pp.name
		ORDER BY pv.name, pv.version`

	rows, err := conn.Query(ctx, query, args)
	if err != nil {
		return NpmPackageListResponse{}, err
	}

	versionRows, err := pgx.CollectRows(rows, pgx.RowToStructByName[npmPackageVersionRow])
	if err != nil {
		return NpmPackageListResponse{}, err
	}

	return NpmPackageListResponse{
		Results: assembleNpmPackageListFromRows(versionRows),
		Total:   countTotal,
		Limit:   pageOpts.Limit,
		Offset:  pageOpts.Offset,
	}, nil
}

// NpmPackageGet returns tarball info and timestamps for a specific package name and version
// from the latest version of a repository, plus all other versions available in that repository.
func (t *tangyImpl) NpmPackageGet(ctx context.Context, repositoryHref, name, version string) (NpmPackageDetail, error) {
	if repositoryHref == "" {
		return NpmPackageDetail{}, nil
	}

	conn, innerUnion, args, err := t.prepareNpmPackageQuery(ctx, repositoryHref, name)
	if err != nil {
		return NpmPackageDetail{}, err
	}
	defer conn.Release()

	detailRows, err := fetchNpmPackageDetailRows(ctx, conn, innerUnion, args, version)
	if err != nil {
		return NpmPackageDetail{}, err
	}
	if len(detailRows) == 0 {
		return NpmPackageDetail{}, fmt.Errorf("%w: %s@%s", ErrNpmPackageNotFound, name, version)
	}

	row := detailRows[0]
	latestVersions, err := parseNpmLatestVersionsJSON(row.LatestVersionsJSON)
	if err != nil {
		return NpmPackageDetail{}, err
	}

	return npmPackageDetailFromRow(row, latestVersions), nil
}

// NpmPackageVersionsGet returns tarball info for every version of a package name
// from the latest version of a repository.
func (t *tangyImpl) NpmPackageVersionsGet(ctx context.Context, repositoryHref, name string) ([]NpmPackageDetail, error) {
	if repositoryHref == "" {
		return nil, nil
	}

	conn, innerUnion, args, err := t.prepareNpmPackageQuery(ctx, repositoryHref, name)
	if err != nil {
		return nil, err
	}
	defer conn.Release()

	detailRows, err := fetchNpmPackageDetailRows(ctx, conn, innerUnion, args, "")
	if err != nil {
		return nil, err
	}
	if len(detailRows) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrNpmPackageNotFound, name)
	}

	latestVersions, err := parseNpmLatestVersionsJSON(detailRows[0].LatestVersionsJSON)
	if err != nil {
		return nil, err
	}

	results := make([]NpmPackageDetail, len(detailRows))
	for i, row := range detailRows {
		results[i] = npmPackageDetailFromRow(row, latestVersions)
	}

	return results, nil
}

// NpmBuildList lists all npm package builds (name + version pairs), optionally filtered by name
// and version, from the latest version of a repository.
func (t *tangyImpl) NpmBuildList(ctx context.Context, repositoryHref, name, version string, pageOpts PageOptions) (NpmBuildListResponse, error) {
	if repositoryHref == "" {
		return NpmBuildListResponse{}, nil
	}

	conn, err := t.pool.Acquire(ctx)
	if err != nil {
		return NpmBuildListResponse{}, err
	}
	defer conn.Release()

	if pageOpts.Limit == 0 {
		pageOpts.Limit = DefaultLimit
	}

	repoUUID, err := parseNpmRepositoryHref(repositoryHref)
	if err != nil {
		return NpmBuildListResponse{}, fmt.Errorf("error parsing repository href: %w", err)
	}

	latestVersion, err := getLatestRepositoryVersion(ctx, conn, repoUUID)
	if err != nil {
		return NpmBuildListResponse{}, fmt.Errorf("error getting latest repository version: %w", err)
	}

	repoVerMap := []ParsedRepoVersion{{
		RepositoryUUID: repoUUID,
		Version:        latestVersion,
	}}

	args := pgx.NamedArgs{}

	var whereClause string
	if name != "" {
		args["name"] = name
		whereClause += "\n\t\tAND rp.name = @name"
	}
	if version != "" {
		args["version"] = version
		whereClause += "\n\t\tAND rp.version = @version"
	}

	innerUnion, err := contentIdsInVersions(ctx, conn, repoVerMap, &args)
	if err != nil {
		return NpmBuildListResponse{}, err
	}

	buildFrom := `
		FROM npm_package rp
		INNER JOIN core_content cc ON rp.content_ptr_id = cc.pulp_id
	` + innerUnion + whereClause

	countQuery := `
		SELECT COUNT(*)
		FROM (
			SELECT rp.name, rp.version
		` + buildFrom + `
			GROUP BY rp.name, rp.version
		) builds`

	var countTotal int
	err = conn.QueryRow(ctx, countQuery, args).Scan(&countTotal)
	if err != nil {
		return NpmBuildListResponse{}, err
	}

	args["limit"] = pageOpts.Limit
	args["offset"] = pageOpts.Offset

	query := `
		SELECT rp.name, rp.version, MAX(cc.pulp_created) AS created_at
	` + buildFrom + `
		GROUP BY rp.name, rp.version
		ORDER BY created_at DESC
		LIMIT @limit OFFSET @offset`

	rows, err := conn.Query(ctx, query, args)
	if err != nil {
		return NpmBuildListResponse{}, err
	}

	buildRows, err := pgx.CollectRows(rows, pgx.RowToStructByName[npmPackageVersionRow])
	if err != nil {
		return NpmBuildListResponse{}, err
	}

	results := make([]NpmBuildListItem, len(buildRows))
	for i, row := range buildRows {
		results[i] = NpmBuildListItem{
			Name:      row.Name,
			Version:   row.Version,
			CreatedAt: row.CreatedAt.Format(time.RFC3339),
		}
	}

	return NpmBuildListResponse{
		Results: results,
		Total:   countTotal,
		Limit:   pageOpts.Limit,
		Offset:  pageOpts.Offset,
	}, nil
}

func (t *tangyImpl) prepareNpmPackageQuery(ctx context.Context, repositoryHref, name string) (*pgxpool.Conn, string, pgx.NamedArgs, error) {
	conn, err := t.pool.Acquire(ctx)
	if err != nil {
		return nil, "", nil, err
	}

	repoUUID, err := parseNpmRepositoryHref(repositoryHref)
	if err != nil {
		conn.Release()
		return nil, "", nil, fmt.Errorf("error parsing repository href: %w", err)
	}

	latestVersion, err := getLatestRepositoryVersion(ctx, conn, repoUUID)
	if err != nil {
		conn.Release()
		return nil, "", nil, fmt.Errorf("error getting latest repository version: %w", err)
	}

	repoVerMap := []ParsedRepoVersion{{
		RepositoryUUID: repoUUID,
		Version:        latestVersion,
	}}

	args := pgx.NamedArgs{
		"name": name,
	}
	innerUnion, err := contentIdsInVersions(ctx, conn, repoVerMap, &args)
	if err != nil {
		conn.Release()
		return nil, "", nil, err
	}

	return conn, innerUnion, args, nil
}

func fetchNpmPackageDetailRows(ctx context.Context, conn *pgxpool.Conn, innerUnion string, args pgx.NamedArgs, version string) ([]npmPackageDetailRow, error) {
	detailFilter := ""
	orderBy := "ORDER BY d.version"
	if version != "" {
		args["version"] = version
		detailFilter = "WHERE f.version = @version"
		orderBy = ""
	}

	query := `
		WITH filtered AS (
			SELECT rp.name, rp.version, cc.pulp_created,
			       cca.relative_path, ca.sha256, ca.size
			FROM npm_package rp
			INNER JOIN core_content cc ON rp.content_ptr_id = cc.pulp_id
			LEFT JOIN core_contentartifact cca ON cca.content_id = rp.content_ptr_id
			LEFT JOIN core_artifact ca ON ca.pulp_id = cca.artifact_id
	` + innerUnion + `
			AND rp.name = @name
		),
		version_agg AS (
			SELECT
				ARRAY_AGG(version ORDER BY version) AS versions,
				COALESCE(
					JSON_AGG(
						JSON_BUILD_OBJECT('version', version, 'created_at', created_at)
						ORDER BY version
					),
					'[]'::json
				) AS latest_versions_json
			FROM (
				SELECT version, MAX(pulp_created) AS created_at
				FROM filtered
				GROUP BY version
			) v
		),
		detail AS (
			SELECT f.*
			FROM filtered f
			` + detailFilter + `
		)
		SELECT d.name, d.version, d.pulp_created AS created_at,
		       d.relative_path, d.sha256, d.size,
		       va.versions, va.latest_versions_json
		FROM detail d
		CROSS JOIN version_agg va
		` + orderBy

	rows, err := conn.Query(ctx, query, args)
	if err != nil {
		return nil, err
	}

	return pgx.CollectRows(rows, pgx.RowToStructByName[npmPackageDetailRow])
}

func npmPackageDetailFromRow(row npmPackageDetailRow, latestVersions []NpmVersionInfo) NpmPackageDetail {
	return NpmPackageDetail{
		Name:           row.Name,
		Version:        row.Version,
		CreatedAt:      row.CreatedAt.Format(time.RFC3339),
		Tarball:        npmTarballFromRow(row.RelativePath, row.Sha256, row.Size),
		Versions:       row.Versions,
		LatestVersions: latestVersions,
	}
}

func npmTarballFromRow(relativePath, sha256 *string, size *int64) NpmTarballInfo {
	info := NpmTarballInfo{}
	if relativePath != nil {
		info.RelativePath = *relativePath
		info.Filename = path.Base(*relativePath)
	}
	if sha256 != nil {
		info.Sha256 = *sha256
	}
	if size != nil {
		info.Size = *size
	}
	return info
}

func parseNpmLatestVersionsJSON(data []byte) ([]NpmVersionInfo, error) {
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

	latestVersions := make([]NpmVersionInfo, len(raw))
	for i, item := range raw {
		latestVersions[i] = NpmVersionInfo{
			Version:   item.Version,
			CreatedAt: item.CreatedAt.Format(time.RFC3339),
		}
	}
	return latestVersions, nil
}

func assembleNpmPackageListFromRows(rows []npmPackageVersionRow) []NpmPackageListItem {
	if len(rows) == 0 {
		return nil
	}

	results := make([]NpmPackageListItem, 0)
	var current NpmPackageListItem

	for i, row := range rows {
		if i == 0 || row.Name != current.Name {
			if i > 0 {
				results = append(results, current)
			}
			current = NpmPackageListItem{
				Name:     row.Name,
				Versions: []string{row.Version},
				LatestVersions: []NpmVersionInfo{{
					Version:   row.Version,
					CreatedAt: row.CreatedAt.Format(time.RFC3339),
				}},
			}
			continue
		}

		current.Versions = append(current.Versions, row.Version)
		current.LatestVersions = append(current.LatestVersions, NpmVersionInfo{
			Version:   row.Version,
			CreatedAt: row.CreatedAt.Format(time.RFC3339),
		})
	}

	return append(results, current)
}

// parseNpmRepositoryHref extracts the repository UUID from an npm repository href.
// Example: /api/pulp/default/api/v3/repositories/npm/npm/018c1c95-4281-76eb-b277-842cbad524f4/
func parseNpmRepositoryHref(href string) (string, error) {
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
