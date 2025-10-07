package zestwrapper

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/content-services/tang/internal/config"
	zest "github.com/content-services/zest/release/v2024"
	"golang.org/x/exp/slices"
)

const DefaultDomain = "default"

const (
	COMPLETED string = "completed"
	WAITING   string = "waiting"
	RUNNING   string = "running"
	SKIPPED   string = "skipped"
	CANCELED  string = "canceled"
	CANCELING string = "canceling"
	FAILED    string = "failed"
)

func NewRpmZest(ctx context.Context, server config.Server) RpmZest {
	ctx2 := context.WithValue(ctx, zest.ContextServerIndex, 0)
	timeout := 60 * time.Second
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

	return RpmZest{
		client: zest.NewAPIClient(pulpConfig),
		ctx:    ctx2,
	}
}

type RpmZest struct {
	client *zest.APIClient
	ctx    context.Context
}

func (r *RpmZest) LookupOrCreateDomain(name string) (string, error) {
	d, err := r.LookupDomain(name)
	if err != nil {
		return "", err
	}
	if d != "" {
		return d, nil
	}

	localStorage := zest.STORAGECLASSENUM_PULPCORE_APP_MODELS_STORAGE_FILE_SYSTEM
	var domain zest.Domain
	emptyConfig := make(map[string]interface{})
	emptyConfig["location"] = fmt.Sprintf("/var/lib/pulp/%v/", name)
	domain = *zest.NewDomain(name, localStorage, emptyConfig)
	domainResp, resp, err := r.client.DomainsAPI.DomainsCreate(r.ctx, DefaultDomain).Domain(domain).Execute()
	if resp != nil && resp.Body != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		return "", err
	}
	return *domainResp.PulpHref, nil
}

func (r *RpmZest) LookupDomain(name string) (string, error) {
	list, resp, err := r.client.DomainsAPI.DomainsList(r.ctx, "default").Name(name).Execute()
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if len(list.Results) == 0 {
		return "", nil
	} else if list.Results[0].PulpHref == nil {
		return "", fmt.Errorf("unexpectedly got a nil href for domain %v", name)
	} else {
		return *list.Results[0].PulpHref, nil
	}
}

func (r *RpmZest) CreateRepository(domain, name, url string) (repoHref string, remoteHref string, err error) {
	rpmRpmRemote := *zest.NewRpmRpmRemote(name, url)

	remoteResponse, httpResp, err := r.client.RemotesRpmAPI.RemotesRpmRpmCreate(r.ctx, domain).
		RpmRpmRemote(rpmRpmRemote).Execute()
	if err != nil {
		return "", "", err
	}
	defer httpResp.Body.Close()

	rpmRpmRepository := *zest.NewRpmRpmRepository(name)
	if remoteResponse.PulpHref != nil {
		rpmRpmRepository.SetRemote(*remoteResponse.PulpHref)
	}
	resp, httpResp, err := r.client.RepositoriesRpmAPI.RepositoriesRpmRpmCreate(r.ctx, domain).
		RpmRpmRepository(rpmRpmRepository).Execute()

	if err != nil {
		return "", "", err
	}
	defer httpResp.Body.Close()

	return *resp.PulpHref, *remoteResponse.PulpHref, nil
}

func (r *RpmZest) UpdateRemote(remoteHref string, url string) error {
	_, httpResp, err := r.client.RemotesRpmAPI.RemotesRpmRpmPartialUpdate(r.ctx, remoteHref).PatchedrpmRpmRemote(zest.PatchedrpmRpmRemote{Url: &url}).Execute()
	if httpResp != nil {
		defer httpResp.Body.Close()
	}
	if err != nil {
		return err
	}
	return nil
}

func (r *RpmZest) SyncRpmRepository(rpmRpmRepositoryHref string, remoteHref string) (string, error) {
	rpmRepositoryHref := *zest.NewRpmRepositorySyncURL()
	rpmRepositoryHref.SetRemote(remoteHref)
	rpmRepositoryHref.SetSyncPolicy(zest.SYNCPOLICYENUM_MIRROR_CONTENT_ONLY)

	resp, httpResp, err := r.client.RepositoriesRpmAPI.RepositoriesRpmRpmSync(r.ctx, rpmRpmRepositoryHref).
		RpmRepositorySyncURL(rpmRepositoryHref).Execute()
	defer httpResp.Body.Close()
	if err != nil {
		return "", err
	}
	return resp.Task, nil
}

// GetTask Fetch a pulp task
func (r *RpmZest) GetTask(taskHref string) (zest.TaskResponse, error) {
	task, httpResp, err := r.client.TasksAPI.TasksRead(r.ctx, taskHref).Execute()

	if err != nil {
		return zest.TaskResponse{}, err
	}
	defer httpResp.Body.Close()

	return *task, nil
}

// PollTask Poll a task and return the final task object
func (r *RpmZest) PollTask(taskHref string) (*zest.TaskResponse, error) {
	var task zest.TaskResponse
	var err error
	inProgress := true
	pollCount := 1
	for inProgress {
		task, err = r.GetTask(taskHref)
		if err != nil {
			return nil, err
		}
		taskState := *task.State
		switch {
		case slices.Contains([]string{COMPLETED, SKIPPED, CANCELED}, taskState):
			inProgress = false
		case slices.Contains([]string{WAITING, RUNNING, CANCELING}, taskState):
		case taskState == FAILED:
			errorStr := TaskErrorString(task)
			return &task, errors.New(errorStr)
		default:
			inProgress = false
		}

		if inProgress {
			SleepWithBackoff(pollCount)
			pollCount += 1
		}
	}
	return &task, nil
}

func (r *RpmZest) GetRpmRepositoryByName(domain, name string) (*zest.RpmRpmRepositoryResponse, error) {
	resp, httpResp, err := r.client.RepositoriesRpmAPI.RepositoriesRpmRpmList(r.ctx, domain).Name(name).Execute()

	if err != nil {
		return nil, err
	}
	defer httpResp.Body.Close()

	results := resp.GetResults()
	if len(results) > 0 {
		return &results[0], nil
	} else {
		return nil, nil
	}
}

func TaskErrorString(task zest.TaskResponse) string {
	str := ""
	if task.Error != nil {
		for key, element := range *task.Error {
			str = str + fmt.Sprintf("%v: %v.  ", key, element)
		}
	}
	return str
}

func SleepWithBackoff(iteration int) {
	var secs int
	if iteration <= 5 {
		secs = 1
	} else if iteration > 5 && iteration <= 10 {
		secs = 5
	} else if iteration > 10 && iteration <= 20 {
		secs = 10
	} else {
		secs = 30
	}
	time.Sleep(time.Duration(secs) * time.Second)
}
