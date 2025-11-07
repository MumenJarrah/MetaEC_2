# MetaEC: An Efficient and Resilient Erasure-coded KV Store on Disaggregated Memory

This is the implementation repository of our TACO'24 paper: **MetaEC: An Efficient and Resilient Erasure-coded KV Store on Disaggregated Memory**.

## Description

We proposes *MetaEC*, the erasure-coded KV store on disaggregated memory with high efficiency and resilience First, for organizing KV pairs to stripes, *MetaEC* logically forms data chunks and leverages lazy coding to remove the accumulating and coding latency from the critical path. Second, for efficient EC metadata management, *MetaEC* designs EC metadata structures based on accessing features, and employs a hybrid redundancy schema with deterministic distribution to provide fault tolerance with high storage efficiency. Third, for consistent parity updating, we design a parity updating protocol based on parity logging and co-design EC metadata structures to handle concurrent conflicts by allowing only concurrent reads or writes. Experimental results show that compared with the state-of-the-art replication-based KV stores on DM, *MetaEC* achieves up to 53.33% latency reduction, up to 31.01% throughput improvement, and 58.17% memory consumption savings.

## Environment

* For hardware, We run all experiments on a cluster with 14 physical nodes (8 CNs and 6 MNs). Each node
is equipped with two **12-core Intel(R) Xeon(R) E5-2650 V4 CPUs**, **64GB DRAM**, and a
**56Gbps Mellanox ConnectX-3 IB RNIC**. They are interconnected with a **56Gbps Mellanox
Infiniband switch**. Each CN owns **8GB DRAM and 24 CPU cores**, where each core can
serve as a client, and each MN owns **64GB and one CPU core** for memory allocation and
network connection.

* For software, **CentOS 7.6.1810** is recommended for each machine.  In our experiments, **21504 HugePages** of 2MB size in each memory node and **6144** ones in compute nodes is need to be allocated. You can set up this with  `echo 21504 > /proc/sys/vm/nr_hugepages` command for memory nodes and `echo 6144 > /proc/sys/vm/nr_hugepages` for compute nodes.

* For code environment, The recommended version numbers for all **C language libraries** are as follows：

```shell
cmake   3.21.0
make    3.82
boost   1.80
oneTBB
GTEST   1.11.0
GLIBC   2.18
ISAL
```

## Configurations

Configuration files for servers and clients should be provided to the program. Here are two example configuration files below.

#### 1. Servers configuration

For each memory node, you should provide a configuration file `server_config.json`  where you can flexibly configure the server:

```json
{
  "role": "SERVER",
  "conn_type": "IB",
  "server_id": 0,
  "udp_port": 2330,
  "memory_num": 6,
  "memory_ips": [
    "10.0.0.1",
    "10.0.0.2",
    "10.0.0.3",
    "10.0.0.4",
    "10.0.0.5",
    "10.0.0.6"
  ],
  "ib_dev_id": 0,
  "ib_port_id": 1,
  "ib_gid_idx": 0,
  "server_base_addr": "0x10000000",
  "server_data_len": 10737418240,
  "allocate_size": 134217728,
  "block_size": 16384,
  "subblock_size": 256,
  "client_local_size": 1073741824,
  "num_replication": 1,
  "main_core_id": 0,
  "poll_core_id": 1,
  "bg_core_id": 2,
  "gc_core_id": 3
}
```

For briefness, we call each memory node as "server `i`" (`i` = 0, 1, ...).

#### 2. Clients configuration

For each compute node, you should provide a configuration file `client_config.json` where you can flexibly configure the client:

```json
{
  "role": "CLIENT",
  "client_id": 0,
  "conn_type": "IB",
  "server_id": 6,
  "udp_port": 2330,
  "memory_num": 6,
  "memory_ips": [
    "10.0.0.1",
    "10.0.0.2",
    "10.0.0.3",
    "10.0.0.4",
    "10.0.0.5",
    "10.0.0.6"
  ],
  "ib_dev_id": 0,
  "ib_port_id": 1,
  "ib_gid_idx": 0,
  "server_base_addr": "0x10000000",
  "server_data_len": 15032385536,
  "allocate_size": 134217728,
  "block_size": 16384,
  "subblock_size": 256,
  "client_local_size": 1073741824,
  "num_replication": 1,
  "num_idx_rep": 1,
  "num_coroutines": 8,
  "miss_rate_threash": 0.1,
  "workload_run_time": 3,
  "micro_workload_num": 10000,
  "key_size": 8,
  "value_size": 32,
  "main_core_id": 0,
  "poll_core_id": 1,
  "encoding_core_id": 2,
  "bg_core_id": 2,
  "gc_core_id": 3,
  "max_stripe": 10000,
  "k_data": 4,
  "m_parity": 2,
  "virtual_mn": 1,
  "all_clients": 11,
  "if_print_log": 1,
  "race_hash_root_size": 5,
  "is_use_cache": 0,
  "cache_size": 256,
  "num_cn": 8,
  "if_load": 1,
  "if_batch": 0,
  "if_req_latency": 0
}
```

For briefness, we call each compute node as "client `i`" (`i` = 0, 1, 2, ...).

It should be noted that, the `server_id` parameter of client `i` should be set to `2+i*8`. For example, the `server_id` of the first three client is 2, 10, 18 respectively.



## Experiments

For each node, execute the following commands to compile the entire program:

```shell
mkdir build && cd build
cmake ..
make -j
```

We test *MetaEC* with **micro-benchmark**, **YCSB benchmarks** and **Twitter-benchmarks** respectively. For each experiments, you should use the following command in memory nodes to set up servers:

```shell
./micro-test/test_server [path-to-server_config.json] [SERVER_NUM]
```

`[SERVER_NUM]` should be the serial number of this memory node, counting from 0.



#### 1. Micro-benchmark

* **Latency**

    To evaluate the latency of each operation, we use a single client to iteratively execute each operation (**INSERT**, **DELETE**, **UPDATE**, and **SEARCH**) for 10,000 times.

    Enter `./build/` and use the following command in client `0`：

    ```shell
    ./micro-test/test_client_cache [PATH_TO_CLIENT_CONFIG] [NUM_TEST_REQUESTS] [OPERATION]
    ```

    Test results will be saved in `./build/results`.

* **Throughput**

    To evaluate the throughput of each operations, each client first iteratively INSERTs 10K different keys. UPDATE and SEARCH operations are then executed on these keys for 10 seconds. Finally, each client executes DELETE for 3 seconds.

    Enter `./build/` and execute the following command on all client nodes at the same time:

    ```shell
    ./micro-test/test_multi_client [PATH_TO_CLIENT_CONFIG] [OPERATION]
    ```
    There is no specific explanation for other parameters. The default setting for the parameter "num_cn" is 8 because 8 compute nodes are used. The parameter "all_clients" indicates the number of clients on each compute node. The parameter "server_id" is set to 6, 6+all_clients * 1, 6+all_cients * 2, ..., 6+all_clients * 7 for the 0th to 7th compute nodes, respectively, which means num_server + compute_node_id * all_clients.

    When starting, start the first compute_node to preheat and load data, and the remaining compute_nodes can be started in sequence. You do not need to guarantee the order of startup, because the system has a synchronization mechanism, and when all clients are ready, they will be tested together.

    Test results will be displayed on each client terminal.


#### 2. YCSB benchmarks 

* **Workload preparation**

    Firstly, download all the testing workloads using `sh download_workload.sh` in directory `./setup` and unpack the workloads you want to `./build/ycsb-test/workloads`.

    Here is the description of the YCSB workloads:

    | Workload | SEARCH | UPDATE | INSERT |
    | -------- | ------ | ------ | ------ |
    | A        | 0.5    | 0.5    | 0      |
    | B        | 0.95   | 0.95   | 0      |
    | C        | 1      | 0      | 0      |
    | D        | 0.95   | 0      | 0.05   |

    Then, you should execute the following command in `./build/ycsb-test` to split the workloads into N parts(N is the total number of client threads):

    ```shell
    python split-workload.py [N]
    ```

    Each workload_id needs to be sent to the same folder on the compute_node of the corresponding client_thread
    And then we can  start testing *MetaEC* using YCSB benchmarks.

* **Throughput**

    To show the **scalability** of *MetaEC*，we can test the throughput of *MetaEC* with different number of client nodes. 

    ```shell
    ./micro-test/test_multi_client [PATH_TO_CLIENT_CONFIG] [WORKLOAD-NAME]
    ```

    Execute the command on all the client nodes at the same time. `[WORKLOAD-NAME]` can be chosen from `workloada ~ workloadd`.  The rest of the client_config.json parameters are the same as Micro-benchmark

    Test results will be displayed on each client terminal.

#### 3. Twitter benchmarks 

  To show the effectiveness of MetaEC under real-world workloads with variable-sized KV sizes, we
  adopt Twitter cache traces to evaluate the throughput of all schemes. We select three representative
  workloads, i.e. Twitter-Storage, Twitter-Computation, and Twitter-Transient, from storage clusters,
  compute clusters, and transient caching clusters, respectively.

  Here is the description of the Twitter workloads:

  |  Workload  | SEARCH | UPDATE | INSERT |
  |  --------  | ------ | ------ | ------ |
  |  Transient | 0.99   | 0      | 0.01   |
  |  Storage   | 0.50   | 0      | 0.50   |
  |  Compute   | 0.71   | 0.29   | 0      |

  The rest of the steps are exactly the same as the ycsb test method.
  The relevant test folder is in twitter-test/.

#### 4. Baseline Test

  We implemented the baseline: **FragEC** and **RACE** integration tests based on the current code framework.

* **FragEC**

  Start Server

  ```shell
  ./micro-test/test_serverfr [path-to-server_config.json] [SERVER_NUM]
  ```

  Start Client

  ```shell
  ./micro-test/test_clientfr_latency [PATH_TO_CLIENT_CONFIG] [NUM_TEST_REQUESTS] [OPERATION]
  ```

* **RACE**

  Start Server

  ```shell
  ./micro-test/test_server [path-to-server_config.json] [SERVER_NUM]
  ```

  Start Client

  ```shell
  ./micro-test/test_race [PATH_TO_CLIENT_CONFIG] [NUM_TEST_REQUESTS] [OPERATION]
  ```
