# HDFS Operator for Kubernetes

A production-focused Kubernetes operator that provisions and manages Apache Hadoop Distributed File System (HDFS) clusters in both single-node and highly available (HA) topologies. The operator automates the lifecycle of NameNodes, DataNodes, JournalNodes, ZooKeeper ensembles, and optional client workloads so that HDFS stays healthy as cluster specifications evolve.

## Highlights
- **Single & HA deployments** – Switch between 1x or 2x NameNode configurations with automatic JournalNode and ZooKeeper management when HA is enabled.
- **Declarative cluster config** – Tune `core-site.xml` and `hdfs-site.xml` properties directly from the `HDFSCluster` custom resource; changes trigger coordinated rolling restarts.
- **Safe lifecycle management** – Finalizers and status updates ensure clean teardown of managed objects, while restart annotations refresh workloads without manual intervention.
- **Extensible design** – Controller utilities are shared via `internal/controllerutils`, keeping reconciliation logic focused and testable.

## Repository Layout
```
📦 HDFS-operator
├── api/                  # Custom resource definitions and webhooks
├── controllers/          # Reconcilers for core HDFS components
├── internal/controllerutils/  # Shared helpers (resource sizing, restarts, XML diffs)
├── config/               # Kustomize manifests used for deployment
├── hack/                 # Local helper scripts
└── main.go               # Controller manager bootstrap
```

## Prerequisites
- Go 1.20+
- Docker or another OCI-compliant image builder
- Access to a Kubernetes cluster (KIND, Minikube, or managed)
- `kubectl` and `make`

## Quick Start
1. **Install CRDs**
   ```sh
   make install
   ```

2. **Build and push the operator image**
   ```sh
   make docker-build docker-push IMG=<registry>/hdfs-operator:<tag>
   ```

3. **Deploy the controller**
   ```sh
   make deploy IMG=<registry>/hdfs-operator:<tag>
   ```

4. **Create a cluster** – choose either sample:
   ```sh
   kubectl apply -f config/samples/hdfs_v1alpha1_hdfscluster_single.yaml
   # or
   kubectl apply -f config/samples/hdfs_v1alpha1_hdfscluster_ha.yaml
   ```

5. **Verify**
   ```sh
   kubectl get hdfsclusters
   kubectl get pods -l app=hdfsCluster
   ```

To remove everything:
```sh
make undeploy
make uninstall
```

## The `HDFSCluster` Custom Resource
```yaml
apiVersion: hdfs.aut.tech/v1alpha1
kind: HDFSCluster
metadata:
  name: hdfscluster-ha
spec:
  nameNode:
    replicas: 2
    resources:
      storage: 5Gi
  dataNode:
    replicas: 3
    resources:
      storage: 10Gi
  journalNode:
    replicas: 3
    resources:
      storage: 3Gi
  zookeeper:
    replicas: 3
    resources:
      storage: 3Gi
  clusterConfig:
    coreSite:
      fs.defaultFS: hdfs://hdfs-k8s
    hdfsSite:
      dfs.replication: "3"
```

### Key Spec Fields
- `nameNode`, `dataNode`, `journalNode`, `zookeeper` – Replica counts and resource settings (CPU, memory, storage). JournalNode/ZooKeeper are required when `nameNode.replicas` is `2`.
- `clusterConfig.coreSite` / `clusterConfig.hdfsSite` – Map of Hadoop configuration entries merged into generated XML.

The operator updates ConfigMaps and orchestrates rolling restarts when configuration changes, ensuring safe propagation without manual pod deletes.

## Development Workflow
```sh
# Install CRDs into the current cluster
make install

# Run the controller locally against the cluster
make run

# Regenerate manifests after API changes
go generate ./...
make manifests

# Execute unit tests (requires write access to your Go build cache)
go test ./...
```

The controllers use controller-runtime’s fake client extensively, and shared logic under `internal/controllerutils` is unit tested for deterministic behavior.

## Useful HDFS Commands
```sh
# Check DataNode health
ohdfs dfsadmin -report

# Inspect NameNode HA status
hdfs haadmin -getServiceState nn0

# Run a sample MapReduce job (executed inside the Hadoop client pod)
apt update && apt install -y wget
wget https://hadoop.s3.ir-thr-at1.arvanstorage.ir/WordCount-1.0-SNAPSHOT.jar
hadoop fs -mkdir /input
wget https://dumps.wikimedia.org/enwiki/20230301/enwiki-20230301-pages-articles-multistream-index.txt.bz2
bzip2 -dk enwiki-20230301-pages-articles-multistream-index.txt.bz2
hadoop fs -put enwiki-20230301-pages-articles-multistream-index.txt /input
hadoop jar WordCount-1.0-SNAPSHOT.jar org.codewitharjun.WC_Runner /input/enwiki-20230301-pages-articles-multistream-index.txt /output
hadoop fs -cat /output/part-00000
```

## Contributing
Issues and pull requests are welcome. If you intend to add new controllers or mutate the API surface, please open an issue first so we can align on design and avoid breaking changes. When submitting code:
- Follow Go best practices and run `gofmt`/`go test ./...`.
- Add or update relevant unit tests in `internal/controllerutils` or the controller packages.
- Keep documentation (samples, README) in sync with functional updates.

## License

Copyright 2023 AmirAllahveran.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy at

```
http://www.apache.org/licenses/LICENSE-2.0
```

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
