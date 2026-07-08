package zestwrapper

import "strings"

// normalizePulpHref strips a leading slash from Pulp resource hrefs before they are
// embedded in zest URL paths. Zest v2026 builds paths as baseURL + "/" + href, so hrefs
// returned by Pulp (e.g. "/api/pulp/default/api/v3/...") would otherwise produce a
// double slash and 404 responses.
func normalizePulpHref(href string) string {
	return strings.TrimPrefix(href, "/")
}
