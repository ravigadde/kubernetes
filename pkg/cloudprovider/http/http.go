/*
Copyright 2014 The Kubernetes Authors All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package http

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/GoogleCloudPlatform/kubernetes/pkg/api"
	"github.com/GoogleCloudPlatform/kubernetes/pkg/cloudprovider"

	"code.google.com/p/gcfg"
)

// httpCloud represents the parsed configuration for the http cloud provider
type httpCloud struct {
	instancesSupported          bool
	instancesURL                string
	tcpLoadBalancerSupported    bool
	zonesSupported              bool
	clustersSupported           bool
	schedulerExtensionSupported bool
	schedulerExtensionURL       string
}

// Config represents the supplied configuration for the http cloud provider
type Config struct {
	Global struct {
		InstancesSupported          bool   `gcfg:"instances"`
		InstancesURL                string `gcfg:"instances-url"`
		TcpLoadBalancerSupported    bool   `gcfg:"tcp-load-balancer"`
		ZonesSupported              bool   `gcfg:"zones"`
		ClustersSupported           bool   `gcfg:"clusters"`
		SchedulerExtensionSupported bool   `gcfg:"scheduler-extension"`
		SchedulerExtensionURL       string `gcfg:"scheduler-extension-url"`
	}
}

const (
	HttpProviderTimeout          = 5 * time.Second
	InstancesPath                = "/v1/instances"
	InstanceResourcesPath        = "resources"
	InstanceAddressesPath        = "addresses"
	SchedulerExtensionPath       = "/v1/scheduler"
	SchedulerExtensionFilter     = "filter"
	SchedulerExtensionPrioritize = "prioritize"
	SchedulerExtensionBind       = "bind"
	SchedulerExtensionUnbind     = "unbind"
)

// FilterArgs represents the arguments needed for filtering nodes for a pod
type FilterArgs struct {
	Pod   api.Pod      `json:"pod"`
	Nodes api.NodeList `json:"nodes"`
}

type PriorityArgs FilterArgs

// BindArgs represents the arguments needed to bind a pod to a host
type BindArgs struct {
	Pod  api.Pod `json:"pod"`
	Host string  `json:"host"`
}

func init() {
	cloudprovider.RegisterCloudProvider("http", func(config io.Reader) (cloudprovider.Interface, error) { return newHTTPCloud(config) })
}

// Creates a new instance of httpCloud through the config file sent.
func newHTTPCloud(config io.Reader) (*httpCloud, error) {
	if config != nil {
		var cfg Config
		if err := gcfg.ReadInto(&cfg, config); err != nil {
			return nil, fmt.Errorf("Couldn't read config: %v", err)
		}

		instancesURL := cfg.Global.InstancesURL
		// Validate URL
		_, err := url.ParseRequestURI(instancesURL)
		if err != nil {
			return nil, fmt.Errorf("Can't parse the instances-url provided: %s", err)
		}
		// Handle Trailing slashes
		instancesURL = strings.TrimRight(instancesURL, "/")

		schedulerExtensionURL := cfg.Global.SchedulerExtensionURL
		// Validate URL
		_, err = url.ParseRequestURI(schedulerExtensionURL)
		if err != nil {
			return nil, fmt.Errorf("Can't parse the scheduler-extension-url provided: %s", err)
		}
		// Handle Trailing slashes
		schedulerExtensionURL = strings.TrimRight(schedulerExtensionURL, "/")

		return &httpCloud{
			instancesURL:                instancesURL,
			instancesSupported:          cfg.Global.InstancesSupported,
			tcpLoadBalancerSupported:    cfg.Global.TcpLoadBalancerSupported,
			zonesSupported:              cfg.Global.ZonesSupported,
			clustersSupported:           cfg.Global.ClustersSupported,
			schedulerExtensionURL:       schedulerExtensionURL,
			schedulerExtensionSupported: cfg.Global.SchedulerExtensionSupported,
		}, nil
	}
	return nil, fmt.Errorf("Config file is empty or is not provided")
}

// Returns an implementation of Instances for HTTP cloud.
func (h *httpCloud) Instances() (cloudprovider.Instances, bool) {
	if h.instancesSupported {
		return h, true
	}

	return nil, false
}

// Returns an implementation of Clusters for HTTP cloud.
func (h *httpCloud) Clusters() (cloudprovider.Clusters, bool) {
	return nil, false
}

// Returns an implementation of TCPLoadBalancer for HTTP cloud.
func (h *httpCloud) TCPLoadBalancer() (cloudprovider.TCPLoadBalancer, bool) {
	return nil, false
}

// Returns an implementation of Zones for HTTP cloud.
func (h *httpCloud) Zones() (cloudprovider.Zones, bool) {
	return nil, false
}

// Returns an implementation of SchedulerExtension for HTTP cloud.
func (h *httpCloud) SchedulerExtension() (cloudprovider.SchedulerExtension, bool) {
	if h.schedulerExtensionSupported {
		return h, true
	}

	return nil, false
}

// Returns the addresses of a particular instance.
func (h *httpCloud) NodeAddresses(instance string) ([]api.NodeAddress, error) {
	var resp []api.NodeAddress
	if err := h.get(h.instancesURL+path.Join(InstancesPath, instance, InstanceAddressesPath), &resp); err != nil {
		return nil, err
	}

	return resp, nil
}

// Returns the cloud provider ID of the specified instance.
func (h *httpCloud) ExternalID(instance string) (string, error) {
	return instance, nil
}

// Enumerates the set of minions instances known by the cloud provider.
func (h *httpCloud) List(filter string) ([]string, error) {
	var resp []string
	if err := h.get(h.instancesURL+path.Join(InstancesPath, filter), &resp); err != nil {
		return nil, err
	}

	return resp, nil
}

// Gets the resources for a particular node.
func (h *httpCloud) GetNodeResources(instance string) (*api.NodeResources, error) {
	var resp api.NodeResources
	if err := h.get(h.instancesURL+path.Join(InstancesPath, instance, InstanceResourcesPath), &resp); err != nil {
		return nil, err
	}

	return &resp, nil
}

// Filter based on provider implemented predicate functions.
func (h *httpCloud) Filter(pod *api.Pod, nodes *api.NodeList) (*api.NodeList, error) {
	var resp api.NodeList

	args := &FilterArgs{
		Pod:   *pod,
		Nodes: *nodes,
	}

	if out, err := json.Marshal(args); err != nil {
		return nil, err
	} else {
		if err := h.post(h.schedulerExtensionURL+path.Join(SchedulerExtensionPath, SchedulerExtensionFilter), bytes.NewReader(out), &resp); err != nil {
			return nil, err
		} else {
			return &resp, nil
		}
	}
}

// Prioritize based on provider implemented priority functions. Weight*priority
// is added up for each such priority function. The returned score is added to
// the score computed by Kubernetes scheduler. The total score is used to do the
// host selection.
func (h *httpCloud) Prioritize(pod *api.Pod, nodes *api.NodeList) (*api.HostPriorityList, error) {
	var resp api.HostPriorityList

	args := &PriorityArgs{
		Pod:   *pod,
		Nodes: *nodes,
	}

	if out, err := json.Marshal(args); err != nil {
		return nil, err
	} else {
		if err := h.post(h.schedulerExtensionURL+path.Join(SchedulerExtensionPath, SchedulerExtensionPrioritize), bytes.NewReader(out), &resp); err != nil {
			return nil, err
		} else {
			return &resp, nil
		}
	}
}

// Inform the provider about the scheduling decision. Bind reserves resources
// for the pod. Returns annotations which can be used further down in the pod's
// lifecycle (network/storage plugin).
func (h *httpCloud) Bind(pod *api.Pod, host string) (map[string]string, error) {
	resp := make(map[string]string)

	args := &BindArgs{
		Pod:  *pod,
		Host: host,
	}

	if out, err := json.Marshal(args); err != nil {
		return nil, err
	} else {
		if err := h.post(h.schedulerExtensionURL+path.Join(SchedulerExtensionPath, SchedulerExtensionBind), bytes.NewReader(out), &resp); err != nil {
			return nil, err
		} else {
			return resp, nil
		}
	}
}

// Inform the provider about the unbind. Frees resources consumed by the pod.
// To be called by scheduler when Bind with apiserver fails or by apiserver in
// pod deletion path.
func (h *httpCloud) Unbind(pod *api.Pod) error {
	if out, err := json.Marshal(pod); err != nil {
		return err
	} else {
		if err := h.post(h.schedulerExtensionURL+path.Join(SchedulerExtensionPath, SchedulerExtensionUnbind), bytes.NewReader(out), nil); err != nil {
			return err
		} else {
			return nil
		}
	}
}

// Helper function to send a http request.
func (h *httpCloud) sendHTTPRequest(requestType string, url string, requestBody io.Reader) ([]byte, error) {
	req, err := http.NewRequest(requestType, url, requestBody)
	if err != nil {
		return nil, err
	}

	client := &http.Client{
		Transport: http.DefaultTransport,
		Timeout:   HttpProviderTimeout,
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	} else {
		return body, nil
	}
}

// Sends a HTTP Get Request and Unmarshals the JSON Response.
func (h *httpCloud) get(path string, resp interface{}) error {
	requestType := "GET"
	body, err := h.sendHTTPRequest(requestType, path, nil)
	if err != nil {
		return fmt.Errorf("HTTP request to cloudprovider failed: %v", err)
	}
	if body != nil {
		if err := json.Unmarshal(body, resp); err != nil {
			return fmt.Errorf("GET response Unmarshal for %s failed with error: %v\n", path, err)
		}
	}
	return nil
}

// Sends a HTTP Post Request and Unmarshals the JSON Response.
func (h *httpCloud) post(path string, req io.Reader, resp interface{}) error {
	requestType := "POST"
	body, err := h.sendHTTPRequest(requestType, path, req)
	if err != nil {
		return fmt.Errorf("HTTP request to cloudprovider failed: %v", err)
	}
	if body != nil && resp != nil {
		if err := json.Unmarshal(body, resp); err != nil {
			return fmt.Errorf("POST response Unmarshal for %s failed with error: %v\n", path, err)
		}
	}
	return nil
}
