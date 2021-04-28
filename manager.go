package redis

import corev1 "k8s.io/api/core/v1"

// RedisClusterStatus represent the Redis Cluster status
type RedisClusterStatus struct {
	Status               ClusterStatus
	NumberOfMaster       int32
	MinReplicationFactor int32
	MaxReplicationFactor int32
	NodesPlacement       NodesPlacementInfo

	// In theory, we always have NbPods > NbRedisRunning > NbPodsReady
	NbPods         int32
	NbPodsReady    int32
	NbRedisRunning int32
	Nodes          []RedisClusterNode
}

// RedisClusterNode represent a RedisCluster Node
type RedisClusterNode struct {
	ID        string
	Role      string
	IP        string
	Port      string
	Slots     []string
	MasterRef string
	PodName   string
	Pod       *corev1.Pod
}

// Manager regroups Function for managing a Redis Cluster
type Manager struct {
	admin *Admin
}

// NewManager builds and returns new Manager instance
func NewManager(admin *Admin) *Manager {
	return &Manager{
		admin: admin,
	}
}

// BuildClusterStatus builds and returns new instance of the RedisClusterClusterStatus
func (m *Manager) BuildClusterStatus() (*RedisClusterStatus, error) {
	status := &RedisClusterStatus{}

	return status, nil
}
