package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
)

// Warm sources never fall through to a video encoder. Labels only rank sources
// after their configuration has established that they are safe to keep open.
type cameraStreamPlan struct {
	Sources []string `json:"sources"`
	Warm    []string `json:"warm_sources"`
	Detail  string   `json:"detail_source"`
	Reason  string   `json:"reason,omitempty"`
}

func copyOnlySource(name string, streams map[string]json.RawMessage, visiting map[string]bool) bool {
	if visiting[name] {
		return false
	}
	raw, ok := streams[name]
	if !ok {
		return false
	}
	visiting[name] = true
	defer delete(visiting, name)
	var sources []string
	var single string
	if json.Unmarshal(raw, &single) == nil {
		sources = []string{single}
	} else if json.Unmarshal(raw, &sources) != nil {
		return false
	}
	if len(sources) == 0 {
		return false
	}
	for _, source := range sources {
		if strings.HasPrefix(source, "ffmpeg:") {
			parts := strings.Split(strings.TrimPrefix(source, "ffmpeg:"), "#")
			for _, option := range parts[1:] {
				// Arbitrary templates/filters can encode even without video=h264.
				if option != "video=copy" && !strings.HasPrefix(option, "audio=") {
					return false
				}
			}
			source = parts[0]
		}
		if _, exists := streams[source]; exists {
			if !copyOnlySource(source, streams, visiting) {
				return false
			}
			continue
		}
		u, err := url.Parse(source)
		if err != nil || (u.Scheme != "rtsp" && u.Scheme != "rtsps") {
			return false
		}
		// A loopback restream can hide a transcoder; resolve it rather than trusting
		// the RTSP scheme alone.
		if u.Hostname() == "127.0.0.1" || u.Hostname() == "localhost" || u.Hostname() == "::1" {
			alias := strings.Trim(u.Path, "/")
			if !copyOnlySource(alias, streams, visiting) {
				return false
			}
		}
	}
	return true
}

func makeStreamPlans(cfg frigateConfig, quality, policy string) (map[string][]string, map[string]cameraStreamPlan) {
	configured := selectCameraStreams(cfg, quality)
	desired := make(map[string][]string)
	plans := make(map[string]cameraStreamPlan)
	for camera, sources := range configured {
		plan := cameraStreamPlan{Sources: sources, Detail: sources[0]}
		if policy == "all" || policy == "" {
			plan.Warm = append([]string(nil), sources...)
		} else {
			for _, source := range sources {
				if copyOnlySource(source, cfg.Go2RTC.Streams, map[string]bool{}) {
					plan.Warm = append(plan.Warm, source)
				}
			}
			// Prefer economical grid sources only when they are actually copy-only.
			rank := func(source string) int {
				for label, candidate := range cfg.Cameras[camera].Live.Streams {
					if candidate == source {
						for _, hint := range []string{"grid", "sub", "low", "sd", "mobile", "detect"} {
							if strings.Contains(strings.ToLower(label), hint) {
								return 0
							}
						}
					}
				}
				return 1
			}
			sort.SliceStable(plan.Warm, func(i, j int) bool { return rank(plan.Warm[i]) < rank(plan.Warm[j]) })
		}
		if len(plan.Warm) == 0 {
			plan.Reason = "no_copy_only_warm_source"
		} else {
			desired[camera] = plan.Warm
		}
		plans[camera] = plan
	}
	return desired, plans
}

func (m *streamManager) discoverPlans(ctx context.Context) (map[string][]string, map[string]cameraStreamPlan, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, m.cfg.frigateURL+"/api/config", nil)
	if err != nil {
		return nil, nil, err
	}
	resp, err := m.client.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, nil, fmt.Errorf("Frigate returned %s", resp.Status)
	}
	var cfg frigateConfig
	if err = json.NewDecoder(resp.Body).Decode(&cfg); err != nil {
		return nil, nil, err
	}
	desired, plans := makeStreamPlans(cfg, m.cfg.preferredQuality, m.cfg.warmPolicy)
	if m.cfg.warmPolicy != "" && m.cfg.warmPolicy != "safe" && m.cfg.warmPolicy != "all" {
		return nil, nil, fmt.Errorf("WARM_POLICY must be safe or all")
	}
	if m.cfg.warmOverrides != "" {
		var overrides map[string]string
		if json.Unmarshal([]byte(m.cfg.warmOverrides), &overrides) != nil {
			return nil, nil, fmt.Errorf("WARM_SOURCE_OVERRIDES must be a camera-to-stream JSON object")
		}
		for camera, source := range overrides {
			plan, exists := plans[camera]
			valid := false
			for _, candidate := range plan.Sources {
				valid = valid || candidate == source
			}
			if !exists || !valid {
				return nil, nil, fmt.Errorf("warm override must reference an enabled camera's configured live stream")
			}
			plan.Warm = []string{source}
			plan.Reason = "explicit_warm_override"
			plans[camera] = plan
			desired[camera] = plan.Warm
		}
	}
	return desired, plans, nil
}

func (m *streamManager) progressiveSources(requested string) (string, string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for camera, plan := range m.plans {
		match := camera == requested
		for _, source := range plan.Sources {
			match = match || requested == source
		}
		if !match {
			continue
		}
		warm := m.streams[camera]
		if warm == nil {
			return "", "", false
		}
		warm.mu.RLock()
		ready := warm.codec == "h264" && len(warm.cache) > 0
		source := warm.sourceNames[warm.sourceIndex]
		warm.mu.RUnlock()
		if !ready {
			return "", "", false
		}
		detail := requested
		if requested == camera {
			detail = plan.Detail
		}
		return source, detail, true
	}
	return "", "", false
}

func (m *streamManager) requiresDemand(source string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	configured := false
	for _, plan := range m.plans {
		for _, warm := range plan.Warm {
			if warm == source {
				return false
			}
		}
		for _, candidate := range plan.Sources {
			configured = configured || candidate == source
		}
	}
	return configured
}

// A lease limits unique expensive sources, not viewers. go2rtc owns producer
// sharing and teardown; Edge only releases its own consumers.
func (m *streamManager) acquireHD(source string) (func(), bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, plan := range m.plans {
		for _, warm := range plan.Warm {
			if warm == source {
				return func() {}, true
			}
		}
	}
	if m.hdUsers == nil {
		m.hdUsers = map[string]int{}
	}
	limit := m.cfg.maxHDStreams
	if limit < 1 {
		limit = 1
	}
	if m.hdUsers[source] == 0 && len(m.hdUsers) >= limit {
		return nil, false
	}
	m.hdUsers[source]++
	var once sync.Once
	return func() {
		once.Do(func() {
			m.mu.Lock()
			defer m.mu.Unlock()
			m.hdUsers[source]--
			if m.hdUsers[source] == 0 {
				delete(m.hdUsers, source)
			}
		})
	}, true
}

func (m *streamManager) policyStatus() map[string]any {
	m.mu.RLock()
	defer m.mu.RUnlock()
	plans := map[string]cameraStreamPlan{}
	for k, v := range m.plans {
		plans[k] = v
	}
	users := map[string]int{}
	for k, v := range m.hdUsers {
		users[k] = v
	}
	return map[string]any{"capabilities": []string{"progressive-video-v1"}, "policy": m.cfg.warmPolicy, "cameras": plans, "hd_users": users, "max_hd_streams": m.cfg.maxHDStreams}
}
