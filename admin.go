package redis

import (
	"context"
	"fmt"
	"time"

	redis "github.com/go-redis/redis/v8"
	"k8s.io/klog/v2"

	"github.com/kubernetes-app/redisutil/utils"
)

const (
	// defaultHashMaxSlots higher value of slot
	// as slots start at 0, total number of slots is defaultHashMaxSlots+1
	defaultHashMaxSlots = 16383

	// ResetHard HARD mode for RESET command
	ResetHard = "HARD"
	// ResetSoft SOFT mode for RESET command
	ResetSoft = "SOFT"
)

// AdminInterface redis cluster admin interface
type AdminInterface interface {
	// Connections returns the connection map of all clients
	// Connections() *redis.Client
	// CloseClient the admin connections
	CloseClient()
	// CloseClusterClient the admin connections
	CloseClusterClient()
	// GetClusterInfos get node infos for all nodes
	GetClusterInfos() (*map[string]string, error)
	// GetClusterNodes get node infos for all nodes
	GetClusterNodes() (*Nodes, error)
	// SetConfigIfNeed set redis config
	SetConfigIfNeed(newConfig map[string]string) error
	// GetHashMaxSlot get the max slot value
	GetHashMaxSlot() Slot
}

// Admin wraps redis cluster admin logic
type Admin struct {
	hashMaxSlots Slot
	rc           *redis.Client
	rcc          *redis.ClusterClient
}

// NewAdmin returns new AdminInterface instance
// at the same time it connects to all Redis Nodes thanks to the addrs list
func NewAdmin(addrs []string, password string) AdminInterface {
	return &Admin{
		hashMaxSlots: defaultHashMaxSlots,
		rc:           NewClient(addrs[0], password),
		rcc:          NewClusterClient(addrs, password),
	}
}

func NewClient(addr, password string) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       0,
	})
}

func NewClusterClient(addrs []string, password string) *redis.ClusterClient {
	opt := &redis.ClusterOptions{
		DialTimeout:  10 * time.Second,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,

		MaxRedirects: 8,

		PoolSize:           10,
		PoolTimeout:        30 * time.Second,
		IdleTimeout:        time.Minute,
		IdleCheckFrequency: 100 * time.Millisecond,
	}
	opt.Addrs = addrs
	opt.Password = password
	return redis.NewClusterClient(opt)
}

// Close used to close all possible resources instantiate by the Admin
func (a *Admin) CloseClient() {
	a.rc.Close()
}

// CloseClusterClient used to close all possible resources instantiate by the Admin
func (a *Admin) CloseClusterClient() {
	a.rcc.Close()
}

// GetHashMaxSlot get the max slot value
func (a *Admin) GetHashMaxSlot() Slot {
	return a.hashMaxSlots
}

// GetClusterInfos return the Nodes infos for all nodes
func (a *Admin) GetClusterInfos() (*map[string]string, error) {
	ctx := context.Background()
	raw, err := a.rc.ClusterInfo(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("wrong format from CLUSTER INFO: %v", err)
	}
	clusterInfos := DecodeClusterInfos(&raw)
	return clusterInfos, err
}

var parseConfigMap = map[string]int8{
	"maxmemory":                  0,
	"proto-max-bulk-len":         0,
	"client-query-buffer-limit":  0,
	"repl-backlog-size":          0,
	"auto-aof-rewrite-min-size":  0,
	"active-defrag-ignore-bytes": 0,
	"hash-max-ziplist-entries":   0,
	"hash-max-ziplist-value":     0,
	"stream-node-max-bytes":      0,
	"set-max-intset-entries":     0,
	"zset-max-ziplist-entries":   0,
	"zset-max-ziplist-value":     0,
	"hll-sparse-max-bytes":       0,
	// TODO parse client-output-buffer-limit
	//"client-output-buffer-limit": 0,
}

// SetConfigIfNeed set redis config
func (a *Admin) SetConfigIfNeed(newConfig map[string]string) error {
	ctx := context.Background()
	if err := a.rcc.ForEachMaster(ctx, func(ctx context.Context, master *redis.Client) error {
		for key, value := range newConfig {
			if _, ok := parseConfigMap[key]; ok {
				value, err := utils.ParseRedisMemConf(value)
				if err != nil {
					klog.Errorf("redis config format err, key: %s, value: %s, err: %v", key, value, err)
					continue
				}
			}
			if err := master.ConfigSet(ctx, key, value).Err(); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}

	return nil
}

func SetRedisConfig(ctx context.Context, rc *redis.Client, newConfig map[string]string) error {
	for key, value := range newConfig {
		if _, ok := parseConfigMap[key]; ok {
			value, err := utils.ParseRedisMemConf(value)
			if err != nil {
				klog.Errorf("redis config format err, key: %s, value: %s, err: %v", key, value, err)
				continue
			}
		}
		if err := rc.ConfigSet(ctx, key, value).Err(); err != nil {
			return err
		}
	}
	return nil
}

// UpdateMasterConfig set redis master config
func (a *Admin) UpdateMasterConfig(newConfig map[string]string) error {
	ctx := context.Background()
	if err := a.rcc.ForEachMaster(ctx, func(ctx context.Context, master *redis.Client) error {
		return SetRedisConfig(ctx, master, newConfig)
	}); err != nil {
		return err
	}
	return nil
}

// SetConfigIfNeed set redis config
func (a *Admin) UpdateSlaveConfig(newConfig map[string]string) error {
	ctx := context.Background()
	if err := a.rcc.ForEachSlave(ctx, func(ctx context.Context, slave *redis.Client) error {
		return SetRedisConfig(ctx, slave, newConfig)
	}); err != nil {
		return err
	}
	return nil
}

func (a *Admin) GetClusterNodes() (*Nodes, error) {
	ctx := context.Background()
	cmd := a.rc.ClusterNodes(ctx)
	if err := a.rc.Process(ctx, cmd); err != nil {
		return nil, err
	}

	var raw string
	var err error
	raw, err = cmd.Result()

	if err != nil {
		return nil, fmt.Errorf("wrong format from CLUSTER NODES: %v", err)
	}

	nodeInfos := DecodeNodeInfos(&raw)
	return nodeInfos, nil
}
