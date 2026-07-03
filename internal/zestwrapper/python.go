package zestwrapper

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/content-services/tang/internal/config"
	zest "github.com/content-services/zest/release/v2024"
)

func NewPythonZest(ctx context.Context, server config.Server) PythonZest {
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

	return PythonZest{
		client: zest.NewAPIClient(pulpConfig),
		ctx:    ctx2,
	}
}

type PythonZest struct {
	client *zest.APIClient
	ctx    context.Context
}

func (p *PythonZest) LookupOrCreateDomain(name string) (string, error) {
	d, err := p.LookupDomain(name)
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
	domainResp, resp, err := p.client.DomainsAPI.DomainsCreate(p.ctx, DefaultDomain).Domain(domain).Execute()
	if resp != nil && resp.Body != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		return "", err
	}
	return *domainResp.PulpHref, nil
}

func (p *PythonZest) LookupDomain(name string) (string, error) {
	list, resp, err := p.client.DomainsAPI.DomainsList(p.ctx, DefaultDomain).Name(name).Execute()
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

func (p *PythonZest) CreateRepository(domain, name, url string, includes []string) (repoHref string, remoteHref string, err error) {
	pythonRemote := zest.NewPythonPythonRemote(name, url)
	policy := zest.POLICY692ENUM_IMMEDIATE
	pythonRemote.Policy = &policy
	pythonRemote.Includes = includes

	remoteResponse, httpResp, err := p.client.RemotesPythonAPI.RemotesPythonPythonCreate(p.ctx, domain).
		PythonPythonRemote(*pythonRemote).Execute()
	if err != nil {
		return "", "", err
	}
	defer httpResp.Body.Close()

	pythonRepository := zest.NewPythonPythonRepository(name)
	if remoteResponse.PulpHref != nil {
		pythonRepository.SetRemote(*remoteResponse.PulpHref)
	}

	repoResponse, httpResp, err := p.client.RepositoriesPythonAPI.RepositoriesPythonPythonCreate(p.ctx, domain).
		PythonPythonRepository(*pythonRepository).Execute()
	if err != nil {
		return "", "", err
	}
	defer httpResp.Body.Close()

	return *repoResponse.PulpHref, *remoteResponse.PulpHref, nil
}

func (p *PythonZest) SyncPythonRepository(repoHref, remoteHref string) (string, error) {
	syncURL := zest.NewRepositorySyncURL()
	syncURL.SetRemote(remoteHref)
	mirror := true
	syncURL.SetMirror(mirror)

	resp, httpResp, err := p.client.RepositoriesPythonAPI.RepositoriesPythonPythonSync(p.ctx, repoHref).
		RepositorySyncURL(*syncURL).Execute()
	if httpResp != nil {
		defer httpResp.Body.Close()
	}
	if err != nil {
		return "", err
	}
	return resp.Task, nil
}

func (p *PythonZest) PollTask(taskHref string) (*zest.TaskResponse, error) {
	rpmZest := RpmZest{client: p.client, ctx: p.ctx}
	return rpmZest.PollTask(taskHref)
}

func (p *PythonZest) GetPythonRepositoryByName(domain, name string) (*zest.PythonPythonRepositoryResponse, error) {
	resp, httpResp, err := p.client.RepositoriesPythonAPI.RepositoriesPythonPythonList(p.ctx, domain).Name(name).Execute()
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
