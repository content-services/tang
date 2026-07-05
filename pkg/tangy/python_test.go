package tangy

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParsePythonRepositoryHref(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		href    string
		want    string
		wantErr bool
	}{
		{
			name: "valid python repository href",
			href: "/api/pulp/default/api/v3/repositories/python/python/018c1c95-4281-76eb-b277-842cbad524f4/",
			want: "018c1c95-4281-76eb-b277-842cbad524f4",
		},
		{
			name: "valid href without trailing slash",
			href: "/api/pulp/default/api/v3/repositories/python/python/018c1c95-4281-76eb-b277-842cbad524f4",
			want: "018c1c95-4281-76eb-b277-842cbad524f4",
		},
		{
			name:    "invalid href",
			href:    "/api/pulp/default/api/v3/repositories/python/",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := parsePythonRepositoryHref(tt.href)
			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestAssemblePythonPackageListFromRows(t *testing.T) {
	t.Parallel()

	createdAt1 := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	createdAt2 := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	createdAt3 := time.Date(2024, 3, 1, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name string
		rows []pythonPackageVersionRow
		want []PythonPackageListItem
	}{
		{
			name: "empty rows",
			rows: nil,
			want: nil,
		},
		{
			name: "single package single version",
			rows: []pythonPackageVersionRow{
				{NameNormalized: "django", Name: "Django", Version: "5.0", CreatedAt: createdAt1},
			},
			want: []PythonPackageListItem{
				{
					Name:           "Django",
					NameNormalized: "django",
					Versions:       []string{"5.0"},
					LatestVersions: []PythonVersionInfo{
						{Version: "5.0", CreatedAt: createdAt1.Format(time.RFC3339)},
					},
				},
			},
		},
		{
			name: "single package multiple versions",
			rows: []pythonPackageVersionRow{
				{NameNormalized: "django", Name: "Django", Version: "4.2", CreatedAt: createdAt1},
				{NameNormalized: "django", Name: "Django", Version: "5.0", CreatedAt: createdAt2},
			},
			want: []PythonPackageListItem{
				{
					Name:           "Django",
					NameNormalized: "django",
					Versions:       []string{"4.2", "5.0"},
					LatestVersions: []PythonVersionInfo{
						{Version: "4.2", CreatedAt: createdAt1.Format(time.RFC3339)},
						{Version: "5.0", CreatedAt: createdAt2.Format(time.RFC3339)},
					},
				},
			},
		},
		{
			name: "multiple packages",
			rows: []pythonPackageVersionRow{
				{NameNormalized: "django", Name: "Django", Version: "5.0", CreatedAt: createdAt1},
				{NameNormalized: "requests", Name: "requests", Version: "2.31.0", CreatedAt: createdAt3},
			},
			want: []PythonPackageListItem{
				{
					Name:           "Django",
					NameNormalized: "django",
					Versions:       []string{"5.0"},
					LatestVersions: []PythonVersionInfo{
						{Version: "5.0", CreatedAt: createdAt1.Format(time.RFC3339)},
					},
				},
				{
					Name:           "requests",
					NameNormalized: "requests",
					Versions:       []string{"2.31.0"},
					LatestVersions: []PythonVersionInfo{
						{Version: "2.31.0", CreatedAt: createdAt3.Format(time.RFC3339)},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := assemblePythonPackageListFromRows(tt.rows)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestMockTangyPythonPackageList(t *testing.T) {
	t.Parallel()

	mockTangy := NewMockTangy(t)
	ctx := context.Background()
	repoHref := "/api/pulp/default/api/v3/repositories/python/python/018c1c95-4281-76eb-b277-842cbad524f4/"
	pageOpts := PageOptions{Offset: 0, Limit: 10}
	filterOpts := PythonPackageListFilters{Search: "dj"}

	expected := PythonPackageListResponse{
		Results: []PythonPackageListItem{
			{
				Name:           "Django",
				NameNormalized: "django",
				Versions:       []string{"5.0"},
				LatestVersions: []PythonVersionInfo{
					{Version: "5.0", CreatedAt: "2024-01-01T12:00:00Z"},
				},
			},
		},
		Total:  1,
		Limit:  10,
		Offset: 0,
	}

	mockTangy.On("PythonPackageList", ctx, repoHref, filterOpts, pageOpts).Return(expected, nil)

	got, err := mockTangy.PythonPackageList(ctx, repoHref, filterOpts, pageOpts)
	require.NoError(t, err)
	assert.Equal(t, expected, got)
}

func TestMockTangyPythonDistributionList(t *testing.T) {
	t.Parallel()

	mockTangy := NewMockTangy(t)
	ctx := context.Background()
	repoHref := "/api/pulp/default/api/v3/repositories/python/python/018c1c95-4281-76eb-b277-842cbad524f4/"
	pageOpts := PageOptions{Offset: 0, Limit: 10}

	expected := PythonDistributionListResponse{
		Results: []PythonDistributionListItem{
			{
				Name:           "Django",
				NameNormalized: "django",
				Version:        "5.0",
				Filename:       "django-5.0-py3-none-any.whl",
				PackageType:    "bdist_wheel",
				PythonVersion:  "py3",
				Sha256:         "abc123",
				Size:           1024,
				CreatedAt:      "2024-01-01T12:00:00Z",
			},
		},
		Total:  1,
		Limit:  10,
		Offset: 0,
	}

	mockTangy.On("PythonDistributionList", ctx, repoHref, "django", "5.0", pageOpts).Return(expected, nil)

	got, err := mockTangy.PythonDistributionList(ctx, repoHref, "django", "5.0", pageOpts)
	require.NoError(t, err)
	assert.Equal(t, expected, got)
}

func TestMockTangyPythonPackageGet(t *testing.T) {
	t.Parallel()

	mockTangy := NewMockTangy(t)
	ctx := context.Background()
	repoHref := "/api/pulp/default/api/v3/repositories/python/python/018c1c95-4281-76eb-b277-842cbad524f4/"

	expected := PythonPackageDetail{
		Name:           "Django",
		NameNormalized: "django",
		Version:        "5.0",
		Summary:        "A high-level Python web framework",
		Description:    "Django is a high-level Python web framework.",
		Author:         "Django Software Foundation",
		License:        "BSD-3-Clause",
		ProjectURL:     "https://www.djangoproject.com/",
		LastUpdated:    "2024-01-01T12:00:00Z",
		Versions:       []string{"4.2", "5.0"},
		LatestVersions: []PythonVersionInfo{
			{Version: "4.2", CreatedAt: "2023-01-01T12:00:00Z"},
			{Version: "5.0", CreatedAt: "2024-01-01T12:00:00Z"},
		},
		Distributions: []PythonDistributionListItem{
			{
				Name:           "Django",
				NameNormalized: "django",
				Version:        "5.0",
				Filename:       "django-5.0-py3-none-any.whl",
				PackageType:    "bdist_wheel",
				PythonVersion:  "py3",
				Sha256:         "abc123",
				Size:           1024,
				CreatedAt:      "2024-01-01T12:00:00Z",
			},
		},
	}

	mockTangy.On("PythonPackageGet", ctx, repoHref, "django", "5.0").Return(expected, nil)

	got, err := mockTangy.PythonPackageGet(ctx, repoHref, "django", "5.0")
	require.NoError(t, err)
	assert.Equal(t, expected, got)
}

func TestMockTangyPythonPackageVersionsGet(t *testing.T) {
	t.Parallel()

	mockTangy := NewMockTangy(t)
	ctx := context.Background()
	repoHref := "/api/pulp/default/api/v3/repositories/python/python/018c1c95-4281-76eb-b277-842cbad524f4/"

	expected := []PythonPackageDetail{
		{
			Name:           "Django",
			NameNormalized: "django",
			Version:        "4.2",
			Summary:        "A high-level Python web framework",
			LastUpdated:    "2023-01-01T12:00:00Z",
			Versions:       []string{"4.2", "5.0"},
			LatestVersions: []PythonVersionInfo{
				{Version: "4.2", CreatedAt: "2023-01-01T12:00:00Z"},
				{Version: "5.0", CreatedAt: "2024-01-01T12:00:00Z"},
			},
		},
		{
			Name:           "Django",
			NameNormalized: "django",
			Version:        "5.0",
			Summary:        "A high-level Python web framework",
			LastUpdated:    "2024-01-01T12:00:00Z",
			Versions:       []string{"4.2", "5.0"},
			LatestVersions: []PythonVersionInfo{
				{Version: "4.2", CreatedAt: "2023-01-01T12:00:00Z"},
				{Version: "5.0", CreatedAt: "2024-01-01T12:00:00Z"},
			},
		},
	}

	mockTangy.On("PythonPackageVersionsGet", ctx, repoHref, "django").Return(expected, nil)

	got, err := mockTangy.PythonPackageVersionsGet(ctx, repoHref, "django")
	require.NoError(t, err)
	assert.Equal(t, expected, got)
}

func TestParsePythonJSONStringSlice(t *testing.T) {
	t.Parallel()

	assert.Nil(t, parsePythonJSONStringSlice(nil))
	assert.Nil(t, parsePythonJSONStringSlice([]byte("null")))

	got := parsePythonJSONStringSlice([]byte(`["requests", "urllib3"]`))
	assert.Equal(t, []string{"requests", "urllib3"}, got)
}

func TestParsePythonJSONStringMap(t *testing.T) {
	t.Parallel()

	assert.Nil(t, parsePythonJSONStringMap(nil))
	assert.Nil(t, parsePythonJSONStringMap([]byte("null")))

	got := parsePythonJSONStringMap([]byte(`{"Homepage": "https://example.com", "Source": "https://github.com/example"}`))
	assert.Equal(t, map[string]string{
		"Homepage": "https://example.com",
		"Source":   "https://github.com/example",
	}, got)
}

func TestParsePythonLatestVersionsJSON(t *testing.T) {
	t.Parallel()

	got, err := parsePythonLatestVersionsJSON([]byte(`[
		{"version": "1.0", "created_at": "2024-01-01T12:00:00Z"},
		{"version": "2.0", "created_at": "2024-06-01T12:00:00Z"}
	]`))
	require.NoError(t, err)
	assert.Equal(t, []PythonVersionInfo{
		{Version: "1.0", CreatedAt: "2024-01-01T12:00:00Z"},
		{Version: "2.0", CreatedAt: "2024-06-01T12:00:00Z"},
	}, got)
}
