package tangy

import (
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// pythonVersionPattern matches a PEP 440 version. The v prefix is ignored.
var pythonVersionPattern = regexp.MustCompile(`(?i)^v?(?:(?P<epoch>[0-9]+)!)?(?P<release>[0-9]+(?:\.[0-9]+)*)(?P<pre>[-_.]?(?P<pre_l>alpha|a|beta|b|preview|pre|c|rc)[-_.]?(?P<pre_n>[0-9]+)?)?(?P<post>(?:-(?P<post_n1>[0-9]+))|(?:[-_.]?(?P<post_l>post|rev|r)[-_.]?(?P<post_n2>[0-9]+)?))?(?P<dev>[-_.]?(?P<dev_l>dev)[-_.]?(?P<dev_n>[0-9]+)?)?(?:\+(?P<local>[a-z0-9]+(?:[-_.][a-z0-9]+)*))?$`)

var pythonLocalVersionSeparator = regexp.MustCompile(`[-_.]`)

const (
	pythonVersionNegInf = -1
	pythonVersionValue  = 0
	pythonVersionPosInf = 1
)

type pythonVersionKey struct {
	epoch    int
	release  []int
	preRank  int
	preKind  int
	preNum   int
	postRank int
	postNum  int
	devRank  int
	devNum   int
	hasLocal bool
	local    []pythonLocalPart
}

type pythonLocalPart struct {
	numeric bool
	num     int
	text    string
}

func sortPythonPackageVersionDetails(versions []PythonPackageVersionDetail) {
	sort.SliceStable(versions, func(i, j int) bool {
		return comparePythonVersions(versions[i].Version, versions[j].Version) < 0
	})
}

func comparePythonVersions(a, b string) int {
	aKey, aOK := parsePythonVersionKey(a)
	bKey, bOK := parsePythonVersionKey(b)
	switch {
	case aOK && bOK:
		return comparePythonVersionKeys(aKey, bKey)
	case aOK:
		return -1
	case bOK:
		return 1
	default:
		return strings.Compare(a, b)
	}
}

func parsePythonVersionKey(raw string) (pythonVersionKey, bool) {
	match := pythonVersionPattern.FindStringSubmatch(strings.TrimSpace(raw))
	if match == nil {
		return pythonVersionKey{}, false
	}

	parts := make(map[string]string, len(match))
	for i, name := range pythonVersionPattern.SubexpNames() {
		if name != "" {
			parts[name] = match[i]
		}
	}

	key := pythonVersionKey{
		epoch:    parsePythonVersionInt(parts["epoch"]),
		release:  trimTrailingZeros(parsePythonRelease(parts["release"])),
		preRank:  pythonVersionPosInf,
		postRank: pythonVersionNegInf,
		devRank:  pythonVersionPosInf,
	}

	hasPre := parts["pre"] != ""
	hasPost := parts["post"] != ""
	hasDev := parts["dev"] != ""

	if hasPre {
		key.preRank = pythonVersionValue
		key.preKind = pythonPreReleaseKind(parts["pre_l"])
		key.preNum = parsePythonVersionInt(parts["pre_n"])
	}
	if hasPost {
		key.postRank = pythonVersionValue
		postNum := parts["post_n1"]
		if postNum == "" {
			postNum = parts["post_n2"]
		}
		key.postNum = parsePythonVersionInt(postNum)
	}
	if hasDev {
		key.devRank = pythonVersionValue
		key.devNum = parsePythonVersionInt(parts["dev_n"])
	}
	// A dev release with no pre or post sorts before every pre-release of the same version.
	if !hasPre && !hasPost && hasDev {
		key.preRank = pythonVersionNegInf
	}

	if local := parts["local"]; local != "" {
		key.hasLocal = true
		key.local = parsePythonLocalVersion(local)
	}

	return key, true
}

func parsePythonRelease(release string) []int {
	segments := strings.Split(release, ".")
	parts := make([]int, len(segments))
	for i, segment := range segments {
		parts[i] = parsePythonVersionInt(segment)
	}
	return parts
}

func parsePythonVersionInt(value string) int {
	if value == "" {
		return 0
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0
	}
	return parsed
}

func trimTrailingZeros(parts []int) []int {
	end := len(parts)
	for end > 0 && parts[end-1] == 0 {
		end--
	}
	return parts[:end]
}

func pythonPreReleaseKind(label string) int {
	switch strings.ToLower(label) {
	case "a", "alpha":
		return 0
	case "b", "beta":
		return 1
	default:
		return 2
	}
}

func parsePythonLocalVersion(local string) []pythonLocalPart {
	segments := pythonLocalVersionSeparator.Split(local, -1)
	parts := make([]pythonLocalPart, 0, len(segments))
	for _, segment := range segments {
		if segment == "" {
			continue
		}
		if num, err := strconv.Atoi(segment); err == nil {
			parts = append(parts, pythonLocalPart{numeric: true, num: num})
			continue
		}
		parts = append(parts, pythonLocalPart{text: strings.ToLower(segment)})
	}
	return parts
}

func comparePythonVersionKeys(a, b pythonVersionKey) int {
	if cmp := compareInt(a.epoch, b.epoch); cmp != 0 {
		return cmp
	}
	if cmp := compareIntSlices(a.release, b.release); cmp != 0 {
		return cmp
	}
	if cmp := comparePythonVersionSegment(a.preRank, a.preKind, a.preNum, b.preRank, b.preKind, b.preNum); cmp != 0 {
		return cmp
	}
	if cmp := comparePythonVersionSegment(a.postRank, 0, a.postNum, b.postRank, 0, b.postNum); cmp != 0 {
		return cmp
	}
	if cmp := comparePythonVersionSegment(a.devRank, 0, a.devNum, b.devRank, 0, b.devNum); cmp != 0 {
		return cmp
	}
	return comparePythonLocalVersions(a, b)
}

func comparePythonVersionSegment(aRank, aKind, aNum, bRank, bKind, bNum int) int {
	if cmp := compareInt(aRank, bRank); cmp != 0 {
		return cmp
	}
	if aRank != pythonVersionValue {
		return 0
	}
	if cmp := compareInt(aKind, bKind); cmp != 0 {
		return cmp
	}
	return compareInt(aNum, bNum)
}

func comparePythonLocalVersions(a, b pythonVersionKey) int {
	if !a.hasLocal && !b.hasLocal {
		return 0
	}
	if !a.hasLocal {
		return -1
	}
	if !b.hasLocal {
		return 1
	}

	n := len(a.local)
	if len(b.local) < n {
		n = len(b.local)
	}
	for i := 0; i < n; i++ {
		if cmp := comparePythonLocalPart(a.local[i], b.local[i]); cmp != 0 {
			return cmp
		}
	}
	return compareInt(len(a.local), len(b.local))
}

func comparePythonLocalPart(a, b pythonLocalPart) int {
	if a.numeric != b.numeric {
		if a.numeric {
			return -1
		}
		return 1
	}
	if a.numeric {
		return compareInt(a.num, b.num)
	}
	return strings.Compare(a.text, b.text)
}

func compareIntSlices(a, b []int) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if cmp := compareInt(a[i], b[i]); cmp != 0 {
			return cmp
		}
	}
	return compareInt(len(a), len(b))
}

func compareInt(a, b int) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}
