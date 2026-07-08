package tangy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testRepoVersionUUID = "018c1c95-4281-76eb-b277-842cbad524f4"

func TestParseRepositoryVersionHrefsMap(t *testing.T) {
	t.Parallel()

	validHref := "/api/pulp/default/api/v3/repositories/rpm/rpm/" + testRepoVersionUUID + "/versions/1/"

	tests := []struct {
		name    string
		hrefs   []string
		want    []ParsedRepoVersion
		wantErr bool
	}{
		{
			name:  "single valid href",
			hrefs: []string{validHref},
			want: []ParsedRepoVersion{{
				RepositoryUUID: testRepoVersionUUID,
				Version:        1,
			}},
		},
		{
			name: "multiple valid hrefs",
			hrefs: []string{
				validHref,
				"/api/pulp/default/api/v3/repositories/rpm/rpm/" + testRepoVersionUUID + "/versions/2/",
			},
			want: []ParsedRepoVersion{
				{RepositoryUUID: testRepoVersionUUID, Version: 1},
				{RepositoryUUID: testRepoVersionUUID, Version: 2},
			},
		},
		{
			name:    "href too short",
			hrefs:   []string{"/api/pulp/default/api/v3/repositories/rpm/rpm/"},
			wantErr: true,
		},
		{
			name:    "invalid uuid",
			hrefs:   []string{"/api/pulp/default/api/v3/repositories/rpm/rpm/not-a-uuid/versions/1/"},
			wantErr: true,
		},
		{
			name:    "invalid version number",
			hrefs:   []string{"/api/pulp/default/api/v3/repositories/rpm/rpm/" + testRepoVersionUUID + "/versions/x/"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := parseRepositoryVersionHrefsMap(tt.hrefs)
			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestParsePackages(t *testing.T) {
	t.Parallel()

	got, err := parsePackages([]map[string]any{
		{"name": "kernel"},
		{"name": "bash"},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"kernel", "bash"}, got)

	_, err = parsePackages([]map[string]any{
		{"name": 123},
	})
	require.Error(t, err)
}

func TestUnionSlices(t *testing.T) {
	t.Parallel()

	assert.Equal(t, []string{"a", "b", "c"}, unionSlices([]string{"a", "b"}, []string{"b", "c"}))
	assert.Equal(t, []int{1, 2}, unionSlices([]int{1}, []int{2}))
	assert.Empty(t, unionSlices([]string{}, []string{}))
}

func TestContainsString(t *testing.T) {
	t.Parallel()

	assert.True(t, containsString([]string{"kernel", "bash"}, "kernel"))
	assert.False(t, containsString([]string{"kernel", "bash"}, "vim"))
	assert.False(t, containsString(nil, "kernel"))
}

func TestMockTangyRpmRepositoryVersionPackageList(t *testing.T) {
	t.Parallel()

	mockTangy := NewMockTangy(t)
	ctx := context.Background()
	href := "/api/pulp/default/api/v3/repositories/rpm/rpm/" + testRepoVersionUUID + "/versions/1/"
	filterOpts := RpmListFilters{Name: "kernel"}
	pageOpts := PageOptions{Offset: 0, Limit: 20}

	expected := []RpmListItem{
		{
			Id:      "pkg-1",
			Name:    "kernel",
			Arch:    "x86_64",
			Version: "6.1.0",
			Release: "1",
			Epoch:   "0",
			Summary: "The Linux kernel",
		},
	}

	mockTangy.On("RpmRepositoryVersionPackageList", ctx, []string{href}, filterOpts, pageOpts).Return(expected, 1, nil)

	got, total, err := mockTangy.RpmRepositoryVersionPackageList(ctx, []string{href}, filterOpts, pageOpts)
	require.NoError(t, err)
	assert.Equal(t, expected, got)
	assert.Equal(t, 1, total)
}

func TestMockTangyRpmRepositoryVersionPackageSearch(t *testing.T) {
	t.Parallel()

	mockTangy := NewMockTangy(t)
	ctx := context.Background()
	href := "/api/pulp/default/api/v3/repositories/rpm/rpm/" + testRepoVersionUUID + "/versions/1/"

	expected := []RpmPackageSearch{
		{Name: "kernel", Summary: "The Linux kernel"},
	}

	mockTangy.On("RpmRepositoryVersionPackageSearch", ctx, []string{href}, "kern", 100).Return(expected, nil)

	got, err := mockTangy.RpmRepositoryVersionPackageSearch(ctx, []string{href}, "kern", 100)
	require.NoError(t, err)
	assert.Equal(t, expected, got)
}
