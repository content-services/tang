package tangy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseRepositoryHref(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		href    string
		want    string
		wantErr bool
	}{
		{
			name: "valid maven repository href",
			href: "/api/pulp/default/api/v3/repositories/maven/maven/018c1c95-4281-76eb-b277-842cbad524f4/",
			want: "018c1c95-4281-76eb-b277-842cbad524f4",
		},
		{
			name: "valid href without trailing slash",
			href: "/api/pulp/default/api/v3/repositories/maven/maven/018c1c95-4281-76eb-b277-842cbad524f4",
			want: "018c1c95-4281-76eb-b277-842cbad524f4",
		},
		{
			name:    "invalid href",
			href:    "/api/pulp/default/api/v3/repositories/maven/",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := parseRepositoryHref(tt.href)
			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestExtractRelease(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		filename string
		want     string
	}{
		{
			name:     "release suffix in pom filename",
			filename: "smallrye-mutiny-vertx-core-3.16.0.rhlw-3002.pom",
			want:     "rhlw-3002",
		},
		{
			name:     "plain pom filename",
			filename: "junit-4.13.2.pom",
			want:     "",
		},
		{
			name:     "empty filename",
			filename: "",
			want:     "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, extractRelease(tt.filename))
		})
	}
}

func TestMockTangyMavenPackageList(t *testing.T) {
	t.Parallel()

	mockTangy := NewMockTangy(t)
	ctx := context.Background()
	repoHref := "/api/pulp/default/api/v3/repositories/maven/maven/018c1c95-4281-76eb-b277-842cbad524f4/"
	pageOpts := PageOptions{Offset: 0, Limit: 10}
	filterOpts := MavenPackageListFilters{Search: "junit"}

	expected := MavenPackageListResponse{
		Results: []MavenPackageListItem{
			{
				GroupID:    "junit",
				ArtifactID: "junit",
				Versions:   []string{"4.13.2"},
				LatestReleases: []MavenReleaseInfo{
					{Version: "4.13.2", Release: "", CreatedAt: "2024-01-01T12:00:00Z"},
				},
			},
		},
		Total:  1,
		Limit:  10,
		Offset: 0,
	}

	mockTangy.On("MavenPackageList", ctx, repoHref, filterOpts, pageOpts).Return(expected, nil)

	got, err := mockTangy.MavenPackageList(ctx, repoHref, filterOpts, pageOpts)
	require.NoError(t, err)
	assert.Equal(t, expected, got)
}

func TestMockTangyMavenBuildList(t *testing.T) {
	t.Parallel()

	mockTangy := NewMockTangy(t)
	ctx := context.Background()
	repoHref := "/api/pulp/default/api/v3/repositories/maven/maven/018c1c95-4281-76eb-b277-842cbad524f4/"
	pageOpts := PageOptions{Offset: 0, Limit: 10}

	expected := MavenBuildListResponse{
		Results: []MavenBuildListItem{
			{
				GroupID:    "junit",
				ArtifactID: "junit",
				Version:    "4.13.2",
				Release:    "",
				Filename:   "junit-4.13.2.pom",
				CreatedAt:  "2024-01-01T12:00:00Z",
			},
		},
		Total:  1,
		Limit:  10,
		Offset: 0,
	}

	mockTangy.On("MavenBuildList", ctx, repoHref, "junit", "junit", "4.13.2", pageOpts).Return(expected, nil)

	got, err := mockTangy.MavenBuildList(ctx, repoHref, "junit", "junit", "4.13.2", pageOpts)
	require.NoError(t, err)
	assert.Equal(t, expected, got)
}

func TestMockTangyMavenRepositoryMetrics(t *testing.T) {
	t.Parallel()

	mockTangy := NewMockTangy(t)
	ctx := context.Background()
	repoHref := "/api/pulp/default/api/v3/repositories/maven/maven/018c1c95-4281-76eb-b277-842cbad524f4/"

	expected := MavenRepositoryMetrics{
		PackageCount: 1,
		BuildCount:   1,
	}

	mockTangy.On("MavenRepositoryMetrics", ctx, repoHref).Return(expected, nil)

	got, err := mockTangy.MavenRepositoryMetrics(ctx, repoHref)
	require.NoError(t, err)
	assert.Equal(t, expected, got)
}
