package zestwrapper

import "testing"

func TestNormalizePulpHref(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		href string
		want string
	}{
		{
			name: "leading slash",
			href: "/api/pulp/default/api/v3/tasks/abc/",
			want: "api/pulp/default/api/v3/tasks/abc/",
		},
		{
			name: "no leading slash",
			href: "api/pulp/default/api/v3/tasks/abc/",
			want: "api/pulp/default/api/v3/tasks/abc/",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := normalizePulpHref(tt.href); got != tt.want {
				t.Fatalf("normalizePulpHref() = %q, want %q", got, tt.want)
			}
		})
	}
}
