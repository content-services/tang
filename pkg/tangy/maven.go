package tangy

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type MavenReleaseInfo struct {
	Version   string `json:"version"`
	Release   string `json:"release"`
	CreatedAt string `json:"created_at"`
}

type MavenPackageListItem struct {
	GroupID        string             `json:"group_id"`
	ArtifactID     string             `json:"artifact_id"`
	Versions       []string           `json:"versions"`
	LatestReleases []MavenReleaseInfo `json:"latest_releases"`
}

type MavenPackageListResponse struct {
	Results []MavenPackageListItem `json:"results"`
	Total   int                    `json:"total"`
	Limit   int                    `json:"limit"`
	Offset  int                    `json:"offset"`
}

type MavenBuildListItem struct {
	GroupID    string `json:"group_id"`
	ArtifactID string `json:"artifact_id"`
	Version    string `json:"version"`
	Release    string `json:"release"`
	Filename   string `json:"filename"`
	CreatedAt  string `json:"created_at"`
}

type MavenBuildListResponse struct {
	Results []MavenBuildListItem `json:"results"`
	Total   int                  `json:"total"`
	Limit   int                  `json:"limit"`
	Offset  int                  `json:"offset"`
}

type mavenArtifactQueryResult struct {
	GroupID    string
	ArtifactID string
	Version    string
	Filename   string
	CreatedAt  time.Time
}

// MavenPackageList lists Maven packages from the latest version of a repository, grouped by group_id and artifact_id
// Only includes artifacts with .pom files
func (t *tangyImpl) MavenPackageList(ctx context.Context, repositoryHref string, pageOpts PageOptions) (MavenPackageListResponse, error) {
	if repositoryHref == "" {
		return MavenPackageListResponse{}, nil
	}

	conn, err := t.pool.Acquire(ctx)
	if err != nil {
		return MavenPackageListResponse{}, err
	}
	defer conn.Release()

	if pageOpts.Limit == 0 {
		pageOpts.Limit = DefaultLimit
	}

	// Parse repository UUID from href
	repoUUID, err := parseRepositoryHref(repositoryHref)
	if err != nil {
		return MavenPackageListResponse{}, fmt.Errorf("error parsing repository href: %w", err)
	}

	// Get the latest version for this repository
	latestVersion, err := getLatestRepositoryVersion(ctx, conn, repoUUID)
	if err != nil {
		return MavenPackageListResponse{}, fmt.Errorf("error getting latest repository version: %w", err)
	}

	repoVerMap := []ParsedRepoVersion{{
		RepositoryUUID: repoUUID,
		Version:        latestVersion,
	}}

	args := pgx.NamedArgs{}
	innerUnion, err := contentIdsInVersions(ctx, conn, repoVerMap, &args)
	if err != nil {
		return MavenPackageListResponse{}, err
	}

	// Count query for total grouped packages
	// Note: using 'rp' alias as required by contentIdsInVersions function
	countQuery := `
		SELECT COUNT(DISTINCT (rp.group_id, rp.artifact_id))
		FROM maven_mavenartifact rp
	` + innerUnion + ` AND rp.filename LIKE '%.pom'`

	var countTotal int
	err = conn.QueryRow(ctx, countQuery, args).Scan(&countTotal)
	if err != nil {
		return MavenPackageListResponse{}, err
	}

	// Main query using SQL aggregation and pagination
	// This query groups by group_id/artifact_id, collects versions, and finds latest release per version
	args["limit"] = pageOpts.Limit
	args["offset"] = pageOpts.Offset

	query := `
		WITH package_versions AS (
			SELECT
				rp.group_id,
				rp.artifact_id,
				rp.version,
				rp.filename,
				cc.pulp_created,
				ROW_NUMBER() OVER (PARTITION BY rp.group_id, rp.artifact_id, rp.version ORDER BY cc.pulp_created DESC) as rn
			FROM maven_mavenartifact rp
			INNER JOIN core_content cc ON rp.content_ptr_id = cc.pulp_id
		` + innerUnion + `
			AND rp.filename LIKE '%.pom'
		),
		latest_per_version AS (
			SELECT
				group_id,
				artifact_id,
				version,
				filename,
				pulp_created
			FROM package_versions
			WHERE rn = 1
		),
		packages AS (
			SELECT
				group_id,
				artifact_id,
				ARRAY_AGG(DISTINCT version ORDER BY version) as versions
			FROM latest_per_version
			GROUP BY group_id, artifact_id
			ORDER BY group_id, artifact_id
			LIMIT @limit OFFSET @offset
		)
		SELECT
			p.group_id,
			p.artifact_id,
			p.versions,
			COALESCE(
				JSON_AGG(
					JSON_BUILD_OBJECT(
						'version', lpv.version,
						'release', '',
						'filename', lpv.filename,
						'created_at', lpv.pulp_created
					) ORDER BY lpv.version
				) FILTER (WHERE lpv.version IS NOT NULL),
				'[]'::json
			) as latest_releases_json
		FROM packages p
		LEFT JOIN latest_per_version lpv ON p.group_id = lpv.group_id AND p.artifact_id = lpv.artifact_id
		GROUP BY p.group_id, p.artifact_id, p.versions
		ORDER BY p.group_id, p.artifact_id`

	rows, err := conn.Query(ctx, query, args)
	if err != nil {
		return MavenPackageListResponse{}, err
	}

	type queryResult struct {
		GroupID            string
		ArtifactID         string
		Versions           []string
		LatestReleasesJSON []byte
	}

	queryResults, err := pgx.CollectRows(rows, pgx.RowToStructByName[queryResult])
	if err != nil {
		return MavenPackageListResponse{}, err
	}

	// Convert query results to response format
	results := make([]MavenPackageListItem, 0, len(queryResults))
	for _, qr := range queryResults {
		var latestReleases []struct {
			Version   string    `json:"version"`
			Release   string    `json:"release"`
			Filename  string    `json:"filename"`
			CreatedAt time.Time `json:"created_at"`
		}

		if err := json.Unmarshal(qr.LatestReleasesJSON, &latestReleases); err != nil {
			return MavenPackageListResponse{}, fmt.Errorf("failed to parse latest_releases: %w", err)
		}

		// Extract release info and format timestamps
		releaseInfos := make([]MavenReleaseInfo, 0, len(latestReleases))
		for _, lr := range latestReleases {
			releaseInfos = append(releaseInfos, MavenReleaseInfo{
				Version:   lr.Version,
				Release:   extractRelease(lr.Filename),
				CreatedAt: lr.CreatedAt.Format(time.RFC3339),
			})
		}

		results = append(results, MavenPackageListItem{
			GroupID:        qr.GroupID,
			ArtifactID:     qr.ArtifactID,
			Versions:       qr.Versions,
			LatestReleases: releaseInfos,
		})
	}

	return MavenPackageListResponse{
		Results: results,
		Total:   countTotal,
		Limit:   pageOpts.Limit,
		Offset:  pageOpts.Offset,
	}, nil
}

// extractRelease extracts the release version from a filename
// Example: smallrye-mutiny-vertx-core-3.16.0.rhlw-3002.pom -> rhlw-3002
func extractRelease(filename string) string {
	// Pattern to match .rhlw-XXXX or similar release patterns before .pom
	// Looks for a dot followed by alphanumeric+dash, then .pom
	re := regexp.MustCompile(`\.([a-zA-Z]+-\d+)\.pom$`)
	matches := re.FindStringSubmatch(filename)
	if len(matches) > 1 {
		return matches[1]
	}
	return ""
}

// parseRepositoryHref extracts the repository UUID from a repository href
// Example: /api/pulp/default/api/v3/repositories/maven/maven/018c1c95-4281-76eb-b277-842cbad524f4/
func parseRepositoryHref(href string) (string, error) {
	parts := strings.Split(href, "/")
	// Filter out empty parts
	var nonEmptyParts []string
	for _, part := range parts {
		if part != "" {
			nonEmptyParts = append(nonEmptyParts, part)
		}
	}

	// Expected format: api/pulp/{domain}/api/v3/repositories/maven/maven/{uuid}
	// So we need at least 8 parts
	if len(nonEmptyParts) < 8 {
		return "", fmt.Errorf("invalid repository href format: %s", href)
	}

	// The UUID should be the last part (or second to last if there's a trailing slash)
	repoUUID := nonEmptyParts[len(nonEmptyParts)-1]

	return repoUUID, nil
}

// getLatestRepositoryVersion gets the highest version number for a repository
func getLatestRepositoryVersion(ctx context.Context, conn *pgxpool.Conn, repoUUID string) (int, error) {
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

// MavenBuildList lists all Maven artifacts (builds) for a specific group_id, artifact_id, and version
// from the latest version of a repository
func (t *tangyImpl) MavenBuildList(ctx context.Context, repositoryHref, groupID, artifactID, version string, pageOpts PageOptions) (MavenBuildListResponse, error) {
	if repositoryHref == "" {
		return MavenBuildListResponse{}, nil
	}

	conn, err := t.pool.Acquire(ctx)
	if err != nil {
		return MavenBuildListResponse{}, err
	}
	defer conn.Release()

	if pageOpts.Limit == 0 {
		pageOpts.Limit = DefaultLimit
	}

	// Parse repository UUID from href
	repoUUID, err := parseRepositoryHref(repositoryHref)
	if err != nil {
		return MavenBuildListResponse{}, fmt.Errorf("error parsing repository href: %w", err)
	}

	// Get the latest version for this repository
	latestVersion, err := getLatestRepositoryVersion(ctx, conn, repoUUID)
	if err != nil {
		return MavenBuildListResponse{}, fmt.Errorf("error getting latest repository version: %w", err)
	}

	repoVerMap := []ParsedRepoVersion{{
		RepositoryUUID: repoUUID,
		Version:        latestVersion,
	}}

	args := pgx.NamedArgs{
		"group_id":    groupID,
		"artifact_id": artifactID,
		"version":     version,
	}
	innerUnion, err := contentIdsInVersions(ctx, conn, repoVerMap, &args)
	if err != nil {
		return MavenBuildListResponse{}, err
	}

	// Count query for total artifacts
	countQuery := `
		SELECT COUNT(*)
		FROM maven_mavenartifact rp
	` + innerUnion + `
		AND rp.group_id = @group_id
		AND rp.artifact_id = @artifact_id
		AND rp.version = @version
		AND rp.filename LIKE '%.pom'`

	var countTotal int
	err = conn.QueryRow(ctx, countQuery, args).Scan(&countTotal)
	if err != nil {
		return MavenBuildListResponse{}, err
	}

	// Main query to get all matching artifacts
	query := `
		SELECT rp.group_id, rp.artifact_id, rp.version, rp.filename, cc.pulp_created as created_at
		FROM maven_mavenartifact rp
		INNER JOIN core_content cc ON rp.content_ptr_id = cc.pulp_id
	` + innerUnion + `
		AND rp.group_id = @group_id
		AND rp.artifact_id = @artifact_id
		AND rp.version = @version
		AND rp.filename LIKE '%.pom'
		ORDER BY cc.pulp_created DESC
		LIMIT @limit OFFSET @offset`

	args["limit"] = pageOpts.Limit
	args["offset"] = pageOpts.Offset

	rows, err := conn.Query(ctx, query, args)
	if err != nil {
		return MavenBuildListResponse{}, err
	}

	artifacts, err := pgx.CollectRows(rows, pgx.RowToStructByName[mavenArtifactQueryResult])
	if err != nil {
		return MavenBuildListResponse{}, err
	}

	// Convert to response format
	results := make([]MavenBuildListItem, len(artifacts))
	for i, artifact := range artifacts {
		results[i] = MavenBuildListItem{
			GroupID:    artifact.GroupID,
			ArtifactID: artifact.ArtifactID,
			Version:    artifact.Version,
			Release:    extractRelease(artifact.Filename),
			Filename:   artifact.Filename,
			CreatedAt:  artifact.CreatedAt.Format(time.RFC3339),
		}
	}

	return MavenBuildListResponse{
		Results: results,
		Total:   countTotal,
		Limit:   pageOpts.Limit,
		Offset:  pageOpts.Offset,
	}, nil
}
