# HDFS operator
> BSc Thesis Supervisor: [Dr. Mahmoud Momtazpour](https://scholar.google.co.za/citations?user=uwozfWkAAAAJ&hl=en)

The HDFS Operator is used to set up HFDS in high-availability and single mode

## Description
Our problem is to design and implement a Kubernetes HDFS Operator capable of automating the process of creating, updating, and deleting HDFS clusters within a Kubernetes environment. This operator must ensure that the HDFS clusters are efficiently managed, highly available, and recoverable from failures while respecting resource constraints within the Kubernetes ecosystem.


## Getting Started
You’ll need a Kubernetes cluster to run against. You can use [KIND](https://sigs.k8s.io/kind) to get a local cluster for testing, or run against a remote cluster.
**Note:** Your controller will automatically use the current context in your kubeconfig file (i.e. whatever cluster `kubectl cluster-info` shows).

### Running on the cluster
1. Install Instances of Custom Resources:

```sh
kubectl apply -f config/samples/
```

2. Build and push your image to the location specified by `IMG`:

```sh
make docker-build docker-push IMG=<some-registry>/hdfs-operator:tag
```

3. Deploy the controller to the cluster with the image specified by `IMG`:

```sh
make deploy IMG=<some-registry>/hdfs-operator:tag
```

### Uninstall CRDs
To delete the CRDs from the cluster:

```sh
make uninstall
```

### Undeploy controller
UnDeploy the controller from the cluster:

```sh
make undeploy
```

## Contributing
// TODO(user): Add detailed information on how you would like others to contribute to this project

### How it works
This project aims to follow the Kubernetes [Operator pattern](https://kubernetes.io/docs/concepts/extend-kubernetes/operator/).

It uses [Controllers](https://kubernetes.io/docs/concepts/architecture/controller/),
which provide a reconcile function responsible for synchronizing resources until the desired state is reached on the cluster.

### Test It Out
1. Install the CRDs into the cluster:

```sh
make install
```

2. Run your controller (this will run in the foreground, so switch to a new terminal if you want to leave it running):

```sh
make run
```

**NOTE:** You can also run this in one step by running: `make install run`

### Modifying the API definitions
If you are editing the API definitions, generate the manifests such as CRs or CRDs using:

```sh
make manifests
```

**NOTE:** Run `make --help` for more information on all potential `make` targets

More information can be found via the [Kubebuilder Documentation](https://book.kubebuilder.io/introduction.html)

## License

Copyright 2023 AmirAllahveran.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

### Commands to create project
```bash
kubebuilder init --domain aut.tech --repo github.com/AmirAllahveran/HDFS-operator --owner AmirAllahveran --project-name hdfs-operator
kubebuilder create api --group hdfs --version v1alpha1 --kind HDFSCluster
kubebuilder create webhook --group hdfs --version v1alpha1 --kind HDFSCluster --defaulting --programmatic-validation
```


### Useful commands
```bash
# get cluster status (data nodes)
hdfs dfsadmin -report
# get name node status 
hdfs haadmin -getServiceState nn0
# run Map-Reduce
apt update && apt install wget
wget https://hadoop.s3.ir-thr-at1.arvanstorage.ir/WordCount-1.0-SNAPSHOT.jar
hadoop fs -mkdir /input
wget https://dumps.wikimedia.org/enwiki/20230301/enwiki-20230301-pages-articles-multistream-index.txt.bz2
bzip2 -dk enwiki-20230301-pages-articles-multistream-index.txt.bz2
hadoop fs -put enwiki-20230301-pages-articles-multistream-index.txt /input
hadoop jar WordCount-1.0-SNAPSHOT.jar org.codewitharjun.WC_Runner /input/enwiki-20230301-pages-articles-multistream-index.txt /output
hadoop fs -ls /output
hadoop fs -cat /output/part-00000
```