package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/akshay-glide/mpcium/pkg/common/pathutil"
	"github.com/akshay-glide/mpcium/pkg/config"
	"github.com/akshay-glide/mpcium/pkg/infra"
	"github.com/akshay-glide/mpcium/pkg/logger"
	"github.com/hashicorp/consul/api"
	"github.com/urfave/cli/v3"
)

func registerPeers(ctx context.Context, c *cli.Command) error {
	inputPath := c.String("peers")
	environment := c.String("environment")

	// If no peers path specified, check for peers.json in current directory
	if inputPath == "" {
		inputPath = "peers.json"
	}

	// Hardcoded prefix for MPC peers in Consul
	prefix := "mpc_peers/"

	// Validate the input file path for security
	if err := pathutil.ValidateFilePath(inputPath); err != nil {
		return fmt.Errorf("invalid input file path: %w", err)
	}

	// Check if input file exists
	if _, err := os.Stat(inputPath); os.IsNotExist(err) {
		if inputPath == "peers.json" {
			return fmt.Errorf("peers.json not found in current directory. Please specify the path using --peers flag or create peers.json in the current directory")
		}
		return fmt.Errorf("input file %s does not exist", inputPath)
	}

	// Read peers JSON file
	data, err := os.ReadFile(inputPath)
	if err != nil {
		return fmt.Errorf("failed to read JSON file: %w", err)
	}

	// Parse peers data
	peerMap := make(map[string]string)
	if err := json.Unmarshal(data, &peerMap); err != nil {
		return fmt.Errorf("failed to unmarshal JSON data: %w", err)
	}

	if len(peerMap) == 0 {
		return fmt.Errorf("no peers found in the input file")
	}

	// Initialize config and logger
	config.InitViperConfig(c.String("config"))
	logger.Init(environment, true)

	// Connect to Consul
	client := infra.GetConsulClient(environment)
	kv := client.KV()

	// Register peers in Consul
	for nodeName, nodeID := range peerMap {
		key := prefix + nodeName

		// Check if the key already exists
		existing, _, err := kv.Get(key, nil)
		if err != nil {
			return fmt.Errorf("failed to check existing key %s: %w", key, err)
		}

		if existing != nil {
			existingID := string(existing.Value)
			if existingID != nodeID {
				return fmt.Errorf("conflict detected: peer %s already exists with ID %s, but trying to register with different ID %s", nodeName, existingID, nodeID)
			}
			fmt.Printf("Peer %s already registered with same ID %s, skipping\n", nodeName, nodeID)
			continue
		}

		p := &api.KVPair{Key: key, Value: []byte(nodeID)}

		// Store the key-value pair
		_, err = kv.Put(p, nil)
		if err != nil {
			return fmt.Errorf("failed to store key %s: %w", key, err)
		}
		fmt.Printf("Registered peer %s with ID %s to Consul\n", nodeName, nodeID)
	}

	logger.Info("Successfully registered peers to Consul", "peers", peerMap, "prefix", prefix)
	return nil
}
