package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// chartValues is the subset of the Helm chart's values we assert against.
type chartValues struct {
	Services map[string]struct {
		Enabled  bool   `yaml:"enabled"`
		Role     string `yaml:"role"`
		Protocol string `yaml:"protocol"`
		Port     int    `yaml:"port"`
	} `yaml:"services"`
	Loadgen struct {
		Enabled bool   `yaml:"enabled"`
		Name    string `yaml:"name"`
	} `yaml:"loadgen"`
}

// TestTrafficSourceIsNotNamedLoadgen guards the deliberate split between the
// implementation's role (loadgen — it is a load generator) and the identity it
// presents (web-client). A service called "loadgen" in Causely's topology
// announces that the application is synthetic, which is the opposite of what
// this demo is for.
func TestTrafficSourceIsNotNamedLoadgen(t *testing.T) {
	values := loadChartValues(t, "values.yaml")

	name := values.Loadgen.Name
	if name == "" {
		t.Fatal("loadgen.name is unset; the deployment would fall back to the template default")
	}
	for _, bad := range []string{"loadgen", "load-gen", "loadtest", "load-test", "traffic-gen"} {
		if strings.Contains(strings.ToLower(name), bad) {
			t.Errorf("loadgen.name is %q, which contains %q — it appears in Causely's topology "+
				"and should not reveal that the traffic is synthetic", name, bad)
		}
	}

	// The role must still be the one cmd/shopd implements.
	if _, ok := roles["loadgen"]; !ok {
		t.Error("the loadgen role is no longer registered; the traffic source would fail to start")
	}
}

func loadChartValues(t *testing.T, name string) chartValues {
	t.Helper()
	path := filepath.Join("..", "..", "deploy", "tracey-shop", name)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var v chartValues
	if err := yaml.Unmarshal(raw, &v); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	if len(v.Services) == 0 {
		t.Fatalf("%s declares no services", path)
	}
	return v
}

// TestChartRolesAreImplemented guards against the easiest way to break this
// demo: adding a service to the chart without registering its role in the
// binary. The pod would start, fail with "unknown ROLE", and CrashLoopBackOff.
func TestChartRolesAreImplemented(t *testing.T) {
	for _, file := range []string{"values.yaml", "values-kind.yaml"} {
		values := loadChartValues(t, file)
		for name, svc := range values.Services {
			role := svc.Role
			if role == "" {
				role = name
			}
			if _, ok := roles[role]; !ok {
				t.Errorf("%s: service %q declares role %q, which cmd/shopd does not implement",
					file, name, role)
			}
		}
	}
}

// TestImplementedRolesAreReachable is the converse: every role in the binary
// should be deployed by the chart, otherwise it is dead code.
func TestImplementedRolesAreReachable(t *testing.T) {
	values := loadChartValues(t, "values.yaml")

	deployed := map[string]bool{
		// Rendered by templates/loadgen.yaml rather than the services map.
		"loadgen": true,
	}
	for name, svc := range values.Services {
		role := svc.Role
		if role == "" {
			role = name
		}
		deployed[role] = true
	}

	for role := range roles {
		if !deployed[role] {
			t.Errorf("role %q is implemented but no chart entry deploys it", role)
		}
	}
}

// TestServicePortsAreDistinctAndSafe checks two invariants that are easy to
// violate by copy-paste and painful to debug in a cluster.
func TestServicePortsAreDistinctAndSafe(t *testing.T) {
	values := loadChartValues(t, "values.yaml")

	const adminPort = 8090
	// Causely drops any dependency whose destination port is 4317, so a
	// business service listening there would be missing from the topology.
	const otlpGRPCPort = 4317
	const otlpHTTPPort = 4318

	seen := map[int]string{}
	for name, svc := range values.Services {
		if svc.Protocol == "none" {
			if svc.Port != 0 {
				t.Errorf("service %q has protocol none but declares port %d", name, svc.Port)
			}
			continue
		}
		if svc.Port == 0 {
			t.Errorf("service %q has protocol %q but no port", name, svc.Protocol)
			continue
		}
		switch svc.Port {
		case adminPort:
			t.Errorf("service %q uses port %d, which collides with the admin listener", name, svc.Port)
		case otlpGRPCPort, otlpHTTPPort:
			t.Errorf("service %q uses port %d; Causely drops dependencies on OTLP ports", name, svc.Port)
		}
		if other, dup := seen[svc.Port]; dup {
			t.Errorf("services %q and %q both use port %d", other, name, svc.Port)
		}
		seen[svc.Port] = name
	}
}

// TestValuesFilesAgreeOnTopology ensures the kind overlay only scales the demo
// down and never changes its shape, since the docs describe one topology.
func TestValuesFilesAgreeOnTopology(t *testing.T) {
	base := loadChartValues(t, "values.yaml")
	kind := loadChartValues(t, "values-kind.yaml")

	for name, b := range base.Services {
		k, ok := kind.Services[name]
		if !ok {
			t.Errorf("values-kind.yaml is missing service %q", name)
			continue
		}
		if k.Protocol != b.Protocol {
			t.Errorf("service %q: protocol differs (%q vs %q)", name, b.Protocol, k.Protocol)
		}
		if k.Port != b.Port {
			t.Errorf("service %q: port differs (%d vs %d)", name, b.Port, k.Port)
		}
		bRole, kRole := b.Role, k.Role
		if bRole == "" {
			bRole = name
		}
		if kRole == "" {
			kRole = name
		}
		if bRole != kRole {
			t.Errorf("service %q: role differs (%q vs %q)", name, bRole, kRole)
		}
	}
	for name := range kind.Services {
		if _, ok := base.Services[name]; !ok {
			t.Errorf("values-kind.yaml declares service %q that values.yaml does not", name)
		}
	}
}
