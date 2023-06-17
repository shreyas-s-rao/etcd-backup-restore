package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/compressor"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
)

type KeyInsight struct {
	KeyName    string
	Operations int64
	ValueSize  int64
}

type ResourceSummary struct {
	ResourceType string `json:"resourceType"`
	Operations   int64  `json:"operations"`
	ValueSize    int64  `json:"valueSize"`
}

type ClusterSummary struct {
	TotalOperations          int64             `json:"totalOperations"`
	TotalValueSize           int64             `json:"totalValueSize"`
	TopResourcesBySize       []ResourceSummary `json:"topResourcesBySize"`
	TopResourcesByOperations []ResourceSummary `json:"topResourcesByOperations"`
}

type Summary struct {
	ResourcesSummary map[string]ResourceSummary
	ClusterSummary   `json:"clusterSummary"`
}

func main() {
	// Snapstore secrets such as serviceaccount.json are currently set as env vars
	// TODO: specify usage docs as help command

	// TODO: take input from CLI flags
	config := brtypes.SnapstoreConfig{
		Provider:  "PROVIDER",
		Prefix:    "PREFIX",
		Container: "BUCKET_NAME",
	}

	ss, err := snapstore.GetSnapstore(&config)
	if err != nil {
		panic(err)
	}
	layout := "2006-01-02 15:04:05 -0700 MST"
	// TODO: take input from CLI args
	start := "2023-06-12 21:03:00 +0000 UTC"
	end := "2023-06-12 21:23:00 +0000 UTC"
	startTime, err := time.Parse(layout, start)
	if err != nil {
		panic(err)
	}
	endTime, err := time.Parse(layout, end)
	if err != nil {
		panic(err)
	}
	ki, err := Analyze(ss, startTime, endTime)
	if err != nil {
		panic(err)
	}
	fmt.Println()
	printSummary(SummarizeKeyInsights(ki))
}

func printSummary(summary Summary) {
	fmt.Println("=== Cluster Summary ===")
	fmt.Printf("Total Operations: %d\n", summary.ClusterSummary.TotalOperations)
	fmt.Printf("Total Value Size: %s\n", formatBytes(summary.ClusterSummary.TotalValueSize))
	fmt.Println()

	fmt.Println("=== Top Keys by Size ===")
	for index, resource := range summary.ClusterSummary.TopResourcesBySize {
		fmt.Printf("%d. Resource: %s\n", index+1, resource.ResourceType)
		fmt.Printf("   Total Size: %s\n", formatBytes(resource.ValueSize))
		fmt.Printf("   Total Operations: %d\n", resource.Operations)

	}

	fmt.Println("=== Top Keys by Operations ===")
	for index, resource := range summary.ClusterSummary.TopResourcesByOperations {
		fmt.Printf("%d. Resource: %s\n", index+1, resource.ResourceType)
		fmt.Printf("   Total Operations: %d\n", resource.Operations)
		fmt.Printf("   Total Size: %s\n", formatBytes(resource.ValueSize))
		fmt.Println()
	}
}

func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func SummarizeKeyInsights(insights []KeyInsight) Summary {
	summary := Summary{
		ClusterSummary: ClusterSummary{
			TopResourcesBySize:       make([]ResourceSummary, 0),
			TopResourcesByOperations: make([]ResourceSummary, 0),
		},
	}

	resourceSummaryMap := make(map[string]*ResourceSummary)

	for _, insight := range insights {
		summary.ClusterSummary.TotalOperations += insight.Operations
		summary.ClusterSummary.TotalValueSize += insight.ValueSize

		// resource := deriveResourceKey(insight.KeyName)
		resource := insight.KeyName
		rs, found := resourceSummaryMap[resource]
		if !found {
			rs = &ResourceSummary{
				// Key:          insight.KeyName,
				ResourceType: resource,
			}
			resourceSummaryMap[resource] = rs
		}

		rs.Operations += insight.Operations
		rs.ValueSize += insight.ValueSize
	}

	for _, rs := range resourceSummaryMap {
		summary.ClusterSummary.TopResourcesBySize = append(summary.ClusterSummary.TopResourcesBySize, *rs)
		summary.ClusterSummary.TopResourcesByOperations = append(summary.ClusterSummary.TopResourcesByOperations, *rs)
	}

	sort.Slice(summary.ClusterSummary.TopResourcesBySize, func(i, j int) bool {
		return summary.ClusterSummary.TopResourcesBySize[i].ValueSize > summary.ClusterSummary.TopResourcesBySize[j].ValueSize
	})

	sort.Slice(summary.ClusterSummary.TopResourcesByOperations, func(i, j int) bool {
		return summary.ClusterSummary.TopResourcesByOperations[i].Operations > summary.ClusterSummary.TopResourcesByOperations[j].Operations
	})

	if len(summary.ClusterSummary.TopResourcesBySize) > 10 {
		summary.ClusterSummary.TopResourcesBySize = summary.ClusterSummary.TopResourcesBySize[:10]
	}

	if len(summary.ClusterSummary.TopResourcesByOperations) > 10 {
		summary.ClusterSummary.TopResourcesByOperations = summary.ClusterSummary.TopResourcesByOperations[:10]
	}

	return summary
}

func deriveResourceKey(keyName string) string {
	// Assuming the keyName is in the format "/registry/{resourceType}/{namespace}/{name}"
	parts := strings.Split(keyName, "/")
	if len(parts) < 4 {
		return keyName
	}
	return fmt.Sprintf("%s", parts[2])
}

func deriveNamespace(keyName string) string {
	// Assuming the keyName is in the format "/registry/{resourceType}/{namespace}/{name}"
	parts := strings.Split(keyName, "/")
	if len(parts) < 4 {
		return ""
	}
	return parts[3]
}
func Analyze(snapStore brtypes.SnapStore, startTime, endTime time.Time) ([]KeyInsight, error) {
	// Get the list of snapshots
	snapshots, err := snapStore.List()
	if err != nil {
		return nil, err
	}

	// Initialize a map to store the insights for each key
	insights := make(map[string]*KeyInsight)
	fmt.Println("Starting to analyze  deltasnapshots", len(snapshots))
	// Iterate over the snapshots
	for _, snapshot := range snapshots {
		if !strings.HasPrefix(snapshot.SnapName, "Incr") || snapshot.IsChunk {
			// fmt.Println("skipping snapshot.SnapName", snapshot.SnapName)
			continue
		}
		// Check if the snapshot is in the specified time range
		if snapshot.CreatedOn.After(startTime) && snapshot.CreatedOn.Before(endTime) {
			fmt.Println("processed snapshot", snapshot.SnapName)
			// Fetch the snapshot
			rc, err := snapStore.Fetch(*snapshot)
			if err != nil {
				return nil, fmt.Errorf("failed to fetch delta snapshot %s from store : %v", snapshot.SnapName, err)
			}

			// Read the snapshot's contents
			eventsData, err := readSnapshotContentsFromReadCloser(rc, snapshot)
			if err != nil {
				return nil, fmt.Errorf("failed to read events data from delta snapshot %s : %v", snapshot.SnapName, err)
			}

			// Unmarshal the events
			var events []brtypes.Event
			if err := json.Unmarshal(eventsData, &events); err != nil {
				return nil, fmt.Errorf("failed to unmarshal events data from delta snapshot %s : %v", snapshot.SnapName, err)
			}

			// Iterate over the events and update the insights
			// Iterate over the events and update the insights
			for _, event := range events {
				key := string(event.EtcdEvent.Kv.Key)
				insight, ok := insights[key]
				if !ok {
					insight = &KeyInsight{
						KeyName: key,
					}
					insights[key] = insight
				}
				insight.Operations++
				insight.ValueSize += int64(len(event.EtcdEvent.Kv.Value))
			}
		}
	}

	// Convert the map to a slice
	var result []KeyInsight
	for _, insight := range insights {
		result = append(result, *insight)
	}

	return result, nil
}

func getNormalizedSnapshotReadCloser(rc io.ReadCloser, snap *brtypes.Snapshot) (io.ReadCloser, bool, string, error) {
	isCompressed, compressionPolicy, err := compressor.IsSnapshotCompressed(snap.CompressionSuffix)
	if err != nil {
		return rc, false, "", err
	}

	if isCompressed {
		// decompress the snapshot
		rc, err = compressor.DecompressSnapshot(rc, compressionPolicy)
		if err != nil {
			return rc, true, compressionPolicy, fmt.Errorf("unable to decompress the snapshot: %v", err)
		}
	}

	return rc, isCompressed, compressionPolicy, nil
}

func readSnapshotContentsFromReadCloser(rc io.ReadCloser, snap *brtypes.Snapshot) ([]byte, error) {
	startTime := time.Now()

	rc, wasCompressed, compressionPolicy, err := getNormalizedSnapshotReadCloser(rc, snap)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress delta snapshot %s : %v", snap.SnapName, err)
	}

	buf := new(bytes.Buffer)
	bufSize, err := buf.ReadFrom(rc)
	if err != nil {
		return nil, fmt.Errorf("failed to parse contents from delta snapshot %s : %v", snap.SnapName, err)
	}

	totalTime := time.Now().Sub(startTime).Seconds()
	if wasCompressed {
		fmt.Printf("successfully decompressed data of delta snapshot in %v seconds [CompressionPolicy:%v]", totalTime, compressionPolicy)
	} else {
		fmt.Printf("successfully read the data of delta snapshot in %v seconds", totalTime)
	}

	if bufSize <= sha256.Size {
		return nil, fmt.Errorf("delta snapshot is missing hash")
	}

	sha := buf.Bytes()
	data := sha[:bufSize-sha256.Size]
	snapHash := sha[bufSize-sha256.Size:]

	// check for match
	h := sha256.New()
	if _, err := h.Write(data); err != nil {
		return nil, fmt.Errorf("unable to check integrity of snapshot %s: %v", snap.SnapName, err)
	}

	computedSha := h.Sum(nil)
	if !reflect.DeepEqual(snapHash, computedSha) {
		return nil, fmt.Errorf("expected sha256 %v, got %v", snapHash, computedSha)
	}

	return data, nil
}
