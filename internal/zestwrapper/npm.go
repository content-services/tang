package zestwrapper

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/content-services/tang/internal/config"
	zest "github.com/content-services/zest/release/v2026"
)

func NewNpmZest(ctx context.Context, server config.Server) NpmZest {
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

	return NpmZest{
		client: zest.NewAPIClient(pulpConfig),
		ctx:    ctx2,
	}
}

type NpmZest struct {
	client *zest.APIClient
	ctx    context.Context
}

func (n *NpmZest) LookupOrCreateDomain(name string) (string, error) {
	d, err := n.LookupDomain(name)
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
	domainResp, resp, err := n.client.DomainsAPI.DomainsCreate(n.ctx, DefaultDomain).Domain(domain).Execute()
	if resp != nil && resp.Body != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		return "", err
	}
	return *domainResp.PulpHref, nil
}

func (n *NpmZest) LookupDomain(name string) (string, error) {
	list, resp, err := n.client.DomainsAPI.DomainsList(n.ctx, DefaultDomain).Name(name).Execute()
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

func (n *NpmZest) CreateRepository(domain, name, url string) (repoHref string, remoteHref string, err error) {
	npmRemote := zest.NewNpmNpmRemote(name, url)
	policy := zest.POLICY692ENUM_IMMEDIATE
	npmRemote.Policy = &policy

	remoteResponse, httpResp, err := n.client.RemotesNpmAPI.RemotesNpmNpmCreate(n.ctx, domain).
		NpmNpmRemote(*npmRemote).Execute()
	if err != nil {
		return "", "", err
	}
	defer httpResp.Body.Close()

	npmRepository := zest.NewNpmNpmRepository(name)
	if remoteResponse.PulpHref != nil {
		npmRepository.SetRemote(*remoteResponse.PulpHref)
	}

	repoResponse, httpResp, err := n.client.RepositoriesNpmAPI.RepositoriesNpmNpmCreate(n.ctx, domain).
		NpmNpmRepository(*npmRepository).Execute()
	if err != nil {
		return "", "", err
	}
	defer httpResp.Body.Close()

	return *repoResponse.PulpHref, *remoteResponse.PulpHref, nil
}

func (n *NpmZest) SyncNpmRepository(repoHref, remoteHref string) (string, error) {
	syncURL := zest.NewRepositorySyncURL()
	syncURL.SetRemote(remoteHref)
	mirror := true
	syncURL.SetMirror(mirror)

	resp, httpResp, err := n.client.RepositoriesNpmAPI.RepositoriesNpmNpmSync(n.ctx, normalizePulpHref(repoHref)).
		RepositorySyncURL(*syncURL).Execute()
	if httpResp != nil {
		defer httpResp.Body.Close()
	}
	if err != nil {
		return "", err
	}
	return resp.Task, nil
}

func (n *NpmZest) PollTask(taskHref string) (*zest.TaskResponse, error) {
	rpmZest := RpmZest{client: n.client, ctx: n.ctx}
	return rpmZest.PollTask(taskHref)
}
