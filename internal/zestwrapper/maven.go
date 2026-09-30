package zestwrapper

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/content-services/tang/internal/config"
	zest "github.com/content-services/zest/release/v2026"
)

func NewMavenZest(ctx context.Context, server config.Server) MavenZest {
	ctx2 := context.WithValue(ctx, zest.ContextServerIndex, 0)
	timeout := 120 * time.Second
	transport := &http.Transport{ResponseHeaderTimeout: timeout}
	httpClient := http.Client{Transport: transport, Timeout: timeout}

	pulpConfig := zest.NewConfiguration()
	pulpConfig.HTTPClient = &httpClient
	pulpConfig.Servers = zest.ServerConfigurations{zest.ServerConfiguration{
		URL: server.Url,
	}}
	ctx2 = context.WithValue(ctx2, zest.ContextBasicAuth, zest.BasicAuth{
		UserName: server.Username,
		Password: server.Password,
	})

	return MavenZest{
		client:     zest.NewAPIClient(pulpConfig),
		ctx:        ctx2,
		httpClient: &httpClient,
	}
}

type MavenZest struct {
	client     *zest.APIClient
	ctx        context.Context
	httpClient *http.Client
}

func (m *MavenZest) LookupOrCreateDomain(name string) (string, error) {
	d, err := m.LookupDomain(name)
	if err != nil {
		return "", err
	}
	if d != "" {
		return d, nil
	}

	localStorage := zest.STORAGECLASSENUM_PULPCORE_APP_MODELS_STORAGE_FILE_SYSTEM
	domain := *zest.NewDomain(name, localStorage, map[string]interface{}{
		"location": fmt.Sprintf("/var/lib/pulp/%v/", name),
	})
	domainResp, resp, err := m.client.DomainsAPI.DomainsCreate(m.ctx, DefaultDomain).Domain(domain).Execute()
	if resp != nil && resp.Body != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		return "", err
	}
	return *domainResp.PulpHref, nil
}

func (m *MavenZest) LookupDomain(name string) (string, error) {
	list, resp, err := m.client.DomainsAPI.DomainsList(m.ctx, DefaultDomain).Name(name).Execute()
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if len(list.Results) == 0 {
		return "", nil
	}
	if list.Results[0].PulpHref == nil {
		return "", fmt.Errorf("unexpectedly got a nil href for domain %v", name)
	}
	return *list.Results[0].PulpHref, nil
}

// CreateRepository creates a Maven remote, repository, and distribution for pull-through caching.
func (m *MavenZest) CreateRepository(domain, name, remoteURL, distributionBasePath string) (repoHref string, remoteHref string, err error) {
	mavenRemote := zest.NewMavenMavenRemote(name, remoteURL)

	remoteResponse, httpResp, err := m.client.RemotesMavenAPI.RemotesMavenMavenCreate(m.ctx, domain).
		MavenMavenRemote(*mavenRemote).Execute()
	if err != nil {
		return "", "", err
	}
	defer httpResp.Body.Close()

	mavenRepository := zest.NewMavenMavenRepository(name)
	if remoteResponse.PulpHref != nil {
		mavenRepository.SetRemote(*remoteResponse.PulpHref)
	}

	repoResponse, httpResp, err := m.client.RepositoriesMavenAPI.RepositoriesMavenMavenCreate(m.ctx, domain).
		MavenMavenRepository(*mavenRepository).Execute()
	if err != nil {
		return "", "", err
	}
	defer httpResp.Body.Close()

	distribution := zest.NewMavenMavenDistribution(distributionBasePath, name)
	distribution.SetRemote(*remoteResponse.PulpHref)
	distribution.SetRepository(*repoResponse.PulpHref)

	distResp, httpResp, err := m.client.DistributionsMavenAPI.DistributionsMavenMavenCreate(m.ctx, domain).
		MavenMavenDistribution(*distribution).Execute()
	if httpResp != nil {
		defer httpResp.Body.Close()
	}
	if err != nil {
		return "", "", err
	}

	distTask, err := m.PollTask(distResp.Task)
	if err != nil {
		return "", "", err
	}
	// New Pulp domains get a default identity content guard, which is copied onto
	// distributions and rejects anonymous pull-through with 403. The fixture is public.
	if err := m.clearDistributionContentGuard(distributionHref(distTask)); err != nil {
		return "", "", err
	}

	return *repoResponse.PulpHref, *remoteResponse.PulpHref, nil
}

func distributionHref(task *zest.TaskResponse) string {
	if task == nil {
		return ""
	}
	for _, href := range task.GetCreatedResources() {
		if strings.Contains(href, "/distributions/maven/maven/") {
			return href
		}
	}
	return ""
}

func (m *MavenZest) clearDistributionContentGuard(distHref string) error {
	if distHref == "" {
		return fmt.Errorf("maven distribution href missing from create task")
	}

	patch := zest.NewPatchedmavenMavenDistribution()
	patch.SetContentGuardNil()
	resp, httpResp, err := m.client.DistributionsMavenAPI.DistributionsMavenMavenPartialUpdate(m.ctx, normalizePulpHref(distHref)).
		PatchedmavenMavenDistribution(*patch).Execute()
	if httpResp != nil && httpResp.Body != nil {
		defer httpResp.Body.Close()
	}
	if err != nil {
		return err
	}
	_, err = m.PollTask(resp.Task)
	return err
}

// FetchArtifact triggers pull-through caching for an artifact path via the content app.
func (m *MavenZest) FetchArtifact(contentOrigin, contentPathPrefix, domain, distributionBasePath, artifactPath string) error {
	artifactPath = strings.TrimPrefix(artifactPath, "/")
	contentPathPrefix = strings.Trim(contentPathPrefix, "/")
	url := fmt.Sprintf("%s/%s/%s/%s/%s", strings.TrimRight(contentOrigin, "/"), contentPathPrefix, domain, distributionBasePath, artifactPath)

	resp, err := m.httpClient.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("fetch artifact %s: %s", url, resp.Status)
	}
	return nil
}

// WaitForPullThrough waits until pull-through downloads have been added to the repository.
// Each download creates its own repository version on current Pulp.
func (m *MavenZest) WaitForPullThrough(domain, repoName string, minVersion int) error {
	deadline := time.Now().Add(60 * time.Second)
	for {
		repo, err := m.GetMavenRepositoryByName(domain, repoName)
		if err != nil {
			return err
		}
		if repo != nil && repositoryVersionNumber(repo.GetLatestVersionHref()) >= minVersion {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("maven repository %s did not reach version %d after pull-through", repoName, minVersion)
		}
		time.Sleep(time.Second)
	}
}

func repositoryVersionNumber(href string) int {
	href = strings.TrimRight(href, "/")
	slash := strings.LastIndex(href, "/")
	if slash < 0 {
		return 0
	}
	number, err := strconv.Atoi(href[slash+1:])
	if err != nil {
		return 0
	}
	return number
}

func (m *MavenZest) PollTask(taskHref string) (*zest.TaskResponse, error) {
	rpmZest := RpmZest{client: m.client, ctx: m.ctx}
	return rpmZest.PollTask(taskHref)
}

func (m *MavenZest) GetMavenRepositoryByName(domain, name string) (*zest.MavenMavenRepositoryResponse, error) {
	resp, httpResp, err := m.client.RepositoriesMavenAPI.RepositoriesMavenMavenList(m.ctx, domain).Name(name).Execute()
	if err != nil {
		return nil, err
	}
	defer httpResp.Body.Close()

	results := resp.GetResults()
	if len(results) > 0 {
		return &results[0], nil
	}
	return nil, nil
}
