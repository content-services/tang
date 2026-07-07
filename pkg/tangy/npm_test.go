package tangy

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseNpmRepositoryHref(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		href    string
		want    string
		wantErr bool
	}{
		{
			name: "valid npm repository href",
			href: "/api/pulp/default/api/v3/repositories/npm/npm/018c1c95-4281-76eb-b277-842cbad524f4/",
			want: "018c1c95-4281-76eb-b277-842cbad524f4",
		},
		{
			name: "valid href without trailing slash",
			href: "/api/pulp/default/api/v3/repositories/npm/npm/018c1c95-4281-76eb-b277-842cbad524f4",
			want: "018c1c95-4281-76eb-b277-842cbad524f4",
		},
		{
			name:    "invalid href",
			href:    "/api/pulp/default/api/v3/repositories/npm/",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := parseNpmRepositoryHref(tt.href)
			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestAssembleNpmPackageListFromRows(t *testing.T) {
	t.Parallel()

	createdAt1 := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	createdAt2 := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name string
		rows []npmPackageVersionRow
		want []NpmPackageListItem
	}{
		{
			name: "empty rows",
			rows: nil,
			want: nil,
		},
		{
			name: "single package single version",
			rows: []npmPackageVersionRow{
				{Name: "is-odd", Version: "3.0.1", CreatedAt: createdAt1},
			},
			want: []NpmPackageListItem{
				{
					Name:     "is-odd",
					Versions: []string{"3.0.1"},
					LatestVersions: []NpmVersionInfo{
						{Version: "3.0.1", CreatedAt: "2024-01-01T12:00:00Z"},
					},
				},
			},
		},
		{
			name: "single package multiple versions",
			rows: []npmPackageVersionRow{
				{Name: "is-odd", Version: "1.0.0", CreatedAt: createdAt1},
				{Name: "is-odd", Version: "3.0.1", CreatedAt: createdAt2},
			},
			want: []NpmPackageListItem{
				{
					Name:     "is-odd",
					Versions: []string{"1.0.0", "3.0.1"},
					LatestVersions: []NpmVersionInfo{
						{Version: "1.0.0", CreatedAt: "2024-01-01T12:00:00Z"},
						{Version: "3.0.1", CreatedAt: "2024-06-01T12:00:00Z"},
					},
				},
			},
		},
		{
			name: "multiple packages",
			rows: []npmPackageVersionRow{
				{Name: "is-number", Version: "6.0.0", CreatedAt: createdAt1},
				{Name: "is-odd", Version: "3.0.1", CreatedAt: createdAt2},
			},
			want: []NpmPackageListItem{
				{
					Name:     "is-number",
					Versions: []string{"6.0.0"},
					LatestVersions: []NpmVersionInfo{
						{Version: "6.0.0", CreatedAt: "2024-01-01T12:00:00Z"},
					},
				},
				{
					Name:     "is-odd",
					Versions: []string{"3.0.1"},
					LatestVersions: []NpmVersionInfo{
						{Version: "3.0.1", CreatedAt: "2024-06-01T12:00:00Z"},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, assembleNpmPackageListFromRows(tt.rows))
		})
	}
}

func TestNpmTarballFromRow(t *testing.T) {
	t.Parallel()

	relativePath := "is-odd/-/is-odd-3.0.1.tgz"
	sha256 := "abc123"
	size := int64(1024)

	got := npmTarballFromRow(&relativePath, &sha256, &size)
	assert.Equal(t, NpmTarballInfo{
		RelativePath: relativePath,
		Filename:     "is-odd-3.0.1.tgz",
		Sha256:       sha256,
		Size:         size,
	}, got)
	assert.Empty(t, npmTarballFromRow(nil, nil, nil))
}

func TestParseNpmLatestVersionsJSON(t *testing.T) {
	t.Parallel()

	createdAt := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	got, err := parseNpmLatestVersionsJSON([]byte(`[{"version":"3.0.1","created_at":"2024-01-01T12:00:00Z"}]`))
	require.NoError(t, err)
	assert.Equal(t, []NpmVersionInfo{{Version: "3.0.1", CreatedAt: createdAt.Format(time.RFC3339)}}, got)

	got, err = parseNpmLatestVersionsJSON(nil)
	require.NoError(t, err)
	assert.Nil(t, got)

	_, err = parseNpmLatestVersionsJSON([]byte(`not json`))
	require.Error(t, err)
}

func TestMockTangyNpmPackageList(t *testing.T) {
	t.Parallel()

	mockTangy := NewMockTangy(t)
	ctx := context.Background()
	repoHref := "/api/pulp/default/api/v3/repositories/npm/npm/018c1c95-4281-76eb-b277-842cbad524f4/"
	pageOpts := PageOptions{Offset: 0, Limit: 10}
	filterOpts := NpmPackageListFilters{Search: "is-odd"}

	expected := NpmPackageListResponse{
		Results: []NpmPackageListItem{
			{
				Name:     "is-odd",
				Versions: []string{"3.0.1"},
				LatestVersions: []NpmVersionInfo{
					{Version: "3.0.1", CreatedAt: "2024-01-01T12:00:00Z"},
				},
			},
		},
		Total:  1,
		Limit:  10,
		Offset: 0,
	}

	mockTangy.On("NpmPackageList", ctx, repoHref, filterOpts, pageOpts).Return(expected, nil)

	got, err := mockTangy.NpmPackageList(ctx, repoHref, filterOpts, pageOpts)
	require.NoError(t, err)
	assert.Equal(t, expected, got)
}

func TestMockTangyNpmPackageGet(t *testing.T) {
	t.Parallel()

	mockTangy := NewMockTangy(t)
	ctx := context.Background()
	repoHref := "/api/pulp/default/api/v3/repositories/npm/npm/018c1c95-4281-76eb-b277-842cbad524f4/"

	expected := NpmPackageDetail{
		Name:      "is-odd",
		Version:   "3.0.1",
		CreatedAt: "2024-01-01T12:00:00Z",
		Tarball: NpmTarballInfo{
			RelativePath: "is-odd/-/is-odd-3.0.1.tgz",
			Filename:     "is-odd-3.0.1.tgz",
			Sha256:       "abc123",
			Size:         1024,
		},
		Versions:       []string{"3.0.1"},
		LatestVersions: []NpmVersionInfo{{Version: "3.0.1", CreatedAt: "2024-01-01T12:00:00Z"}},
	}

	mockTangy.On("NpmPackageGet", ctx, repoHref, "is-odd", "3.0.1").Return(expected, nil)

	got, err := mockTangy.NpmPackageGet(ctx, repoHref, "is-odd", "3.0.1")
	require.NoError(t, err)
	assert.Equal(t, expected, got)
}

func TestMockTangyNpmPackageVersionsGet(t *testing.T) {
	t.Parallel()

	mockTangy := NewMockTangy(t)
	ctx := context.Background()
	repoHref := "/api/pulp/default/api/v3/repositories/npm/npm/018c1c95-4281-76eb-b277-842cbad524f4/"

	expected := []NpmPackageDetail{
		{
			Name:      "is-odd",
			Version:   "3.0.1",
			CreatedAt: "2024-01-01T12:00:00Z",
			Tarball: NpmTarballInfo{
				RelativePath: "is-odd/-/is-odd-3.0.1.tgz",
				Filename:     "is-odd-3.0.1.tgz",
				Sha256:       "abc123",
				Size:         1024,
			},
			Versions:       []string{"3.0.1"},
			LatestVersions: []NpmVersionInfo{{Version: "3.0.1", CreatedAt: "2024-01-01T12:00:00Z"}},
		},
	}

	mockTangy.On("NpmPackageVersionsGet", ctx, repoHref, "is-odd").Return(expected, nil)

	got, err := mockTangy.NpmPackageVersionsGet(ctx, repoHref, "is-odd")
	require.NoError(t, err)
	assert.Equal(t, expected, got)
}

func TestMockTangyNpmBuildList(t *testing.T) {
	t.Parallel()

	mockTangy := NewMockTangy(t)
	ctx := context.Background()
	repoHref := "/api/pulp/default/api/v3/repositories/npm/npm/018c1c95-4281-76eb-b277-842cbad524f4/"
	pageOpts := PageOptions{Offset: 0, Limit: 10}

	expected := NpmBuildListResponse{
		Results: []NpmBuildListItem{
			{Name: "is-odd", Version: "3.0.1", CreatedAt: "2024-01-01T12:00:00Z"},
		},
		Total:  1,
		Limit:  10,
		Offset: 0,
	}

	mockTangy.On("NpmBuildList", ctx, repoHref, "", "", pageOpts).Return(expected, nil)

	got, err := mockTangy.NpmBuildList(ctx, repoHref, "", "", pageOpts)
	require.NoError(t, err)
	assert.Equal(t, expected, got)
}
