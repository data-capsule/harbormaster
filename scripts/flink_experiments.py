from collections import defaultdict
from copy import deepcopy
import json
from experiments import Experiment, copy_file_from_remote_public_ip
from deployment import Deployment
import os
import subprocess
from typing import List, Tuple, Dict
from pathlib import Path
from time import sleep

from ssh_utils import run_remote_public_ip, copy_remote_public_ip


class FlinkExperiment(Experiment):

    def extract_service_hosts_public_ip(self):
        nn_host = None
        node1_host = None
        for k, v in self.binary_mapping.items():
            for service_name in v:
                if "hdfs" in service_name:
                    nn_host = k.public_ip
                elif "node1" in service_name:
                    node1_host = k.public_ip
      
        assert nn_host is not None and node1_host is not None, f"nn_host: {nn_host}, node1_host: {node1_host}"
        return nn_host, node1_host


    def extract_flink_leader_private_ip(self):
        for k, v in self.binary_mapping.items():
            for service_name in v:
                if "flink_leader" in service_name:
                    return k.private_ip
        return None

    def extract_flink_hosts_private_ip(self):
        flink_hosts = []
        for k, v in self.binary_mapping.items():
            for service_name in v:
                if "flink_worker" in service_name:
                    flink_hosts.append(k.private_ip)
        return flink_hosts

    def extract_service_hosts_private_ip(self):
        nn_host = None
        node1_host = None
        for k, v in self.binary_mapping.items():
            for service_name in v:
                if "hdfs" in service_name:
                    nn_host = k.private_ip
                elif "node1" in service_name:
                    node1_host = k.private_ip
        assert nn_host is not None and node1_host is not None, f"nn_host: {nn_host}, node1_host: {node1_host}"
        return nn_host, node1_host

    def remote_build_psl(self):
        # If the local build dir is not empty, we assume the build has already been done
        # if len(os.listdir(os.path.join(self.local_workdir, "build"))) > 0:
        #     print("Skipping build for experiment", self.name)
        #     return
        
        with open(os.path.join(self.local_workdir, "git_hash.txt"), "r") as f:
            git_hash = f.read().strip()

        remote_repo = f"/home/{self.dev_ssh_user}/repo/psl-cvm"
        cmds = [
            f"git clone https://github.com/data-capsule/psl-cvm.git {remote_repo}"
        ]
        try:
            run_remote_public_ip(cmds, self.dev_ssh_user, self.dev_ssh_key, self.dev_vm)
        except Exception as e:
            print("Failed to clone repo. It may already exist. Continuing...")
        print("Ran cmds:")
        print(cmds)

        cmds = [
            f"cd {remote_repo} && git checkout psl",       # Move out of DETACHED HEAD state
            f"cd {remote_repo} && git fetch --all --recurse-submodules && git pull --all --recurse-submodules",
        ]
        try:
            run_remote_public_ip(cmds, self.dev_ssh_user, self.dev_ssh_key, self.dev_vm)
        except Exception as e:
            print("Failed to pull repo. Continuing...")
        print("after pull repo")


        # Copy the diff patch to the remote
        copy_remote_public_ip(os.path.join(self.local_workdir, "diff.patch"), f"{remote_repo}/diff.patch", self.dev_ssh_user, self.dev_ssh_key, self.dev_vm)

        # Setup git env
        cmd = []

        # Checkout the git hash and apply the diff 
        cmds = [
            f"cd {remote_repo} && git reset --hard",
            f"cd {remote_repo} && git checkout {git_hash}",
            f"cd {remote_repo} && git submodule update --init --recursive",
            f"cd {remote_repo} && git apply --allow-empty --reject --whitespace=fix diff.patch",
        ]
        
        # Then build       
        cmds.append(
            f"cd {remote_repo} && {self.build_command}"
        )

        print("Executing build commands ****")
        for cmd in cmds:
            print(cmd)
        try:
            run_remote_public_ip(cmds, self.dev_ssh_user, self.dev_ssh_key, self.dev_vm, hide=False)
        except Exception as e:
            print("Failed to build:", e)
            print("\033[91mTry committing the changes and pushing to the remote repo\033[0m")
            exit(1)
        sleep(0.5)

        self.copy_back_build_files()
    

    def remote_build(self):
        remote_repo = f"/home/{self.dev_ssh_user}/repo"
        print("remote_build ****")
        self.remote_build_psl()

        cmds = [
            f"git clone https://github.com/alexthomasv/flink-psl.git {remote_repo}/flink-psl",
            f"mkdir -p {remote_repo}/flink-psl/traces",
            f"cd {remote_repo}/flink-psl/traces && wget https://fiu-trace.s3.us-east-2.amazonaws.com/write-heavy.blkparse",
        ]
       
        print("before flink-psl remote build")
        try:
            res = run_remote_public_ip(cmds, self.dev_ssh_user, self.dev_ssh_key, self.dev_vm)
            print("*************")
            for line in res:
                print(line)
            print("*************")
        except Exception as e:
            assert False, f"Failed to clone flink-sql-benchmark: {e}"

        # Default HDFS ports (unchanged)
        NN_RPC_PORT   = 9000
        NN_HTTP_PORT  = 9870

        hadoop_conf = Path("/usr/local/hadoop/etc/hadoop")
        nn_host, node1_host = self.extract_service_hosts_private_ip()

        core_site = f"""\
            <configuration>
            <property>
                <name>fs.defaultFS</name>
                <value>hdfs://{nn_host}:{NN_RPC_PORT}</value>
            </property>
            </configuration>
            """

        # ---------- hdfs-site.xml ----------
        hdfs_site = f"""\
            <configuration>
            <property><name>dfs.replication</name><value>1</value></property>
            <property><name>dfs.namenode.name.dir</name><value>file:///var/lib/hadoop/hdfs/namenode</value></property>
            <property><name>dfs.datanode.data.dir</name><value>file:///var/lib/hadoop/hdfs/datanode</value></property>

            <!-- Bind and advertise NN RPC/UI -->
            <property><name>dfs.namenode.rpc-bind-host</name><value>0.0.0.0</value></property>
            <property><name>dfs.namenode.rpc-address</name><value>{nn_host}:{NN_RPC_PORT}</value></property>
            <property><name>dfs.namenode.http-bind-host</name><value>0.0.0.0</value></property>
            <property><name>dfs.namenode.http-address</name><value>0.0.0.0:{NN_HTTP_PORT}</value></property>
            </configuration>
            """

        flink_leader_host = self.extract_flink_leader_private_ip()
        flink_conf = f"""\
            jobmanager.rpc.address: {flink_leader_host}
            jobmanager.rpc.port: 6123
            jobmanager.bind-host: 0.0.0.0
            jobmanager.memory.process.size: 1600m

            taskmanager.bind-host: 0.0.0.0
            taskmanager.memory.process.size: 1728m

            # The number of task slots that each TaskManager offers. Each slot runs one parallel pipeline.

            taskmanager.numberOfTaskSlots: 4
            jobmanager.execution.failover-strategy: region

            # io.tmp.dirs: /tmp
            taskmanager.network.dirs: /home/psladmin/flink-tmp/shuffle
            taskmanager.tmp.dirs: /home/psladmin/flink-tmp/tmp

            # make sure BOTH TaskManagers and JobManager use that tmp dir (not /tmp)
            env.java.opts.taskmanager: "-Djava.io.tmpdir=/home/psladmin/flink-tmp/tmp"
            env.java.opts.jobmanager: "-Djava.io.tmpdir=/home/psladmin/flink-tmp/tmp"

            state.backend: rocksdb
            psl.ssl.cert: {self.remote_workdir}/configs/Pft_root_cert.pem
            psl.ed25519.private-key: {self.remote_workdir}/configs/client1_signing_privkey.pem
            psl.node.host: {node1_host}
            psl.node.port: 3001
            psl.lookup.rate: 0.10
            psl.enabled: true
            """

        flink_hosts = self.extract_flink_hosts_private_ip()
        worker_conf = "\n".join(flink_hosts)

        print(worker_conf)
        print(flink_hosts)

        conf_files_data = [core_site, hdfs_site, flink_conf, worker_conf]
        conf_file_names = ["core-site.xml", "hdfs-site.xml", "flink-conf.yaml", "workers"]
        conf_file_paths = [os.path.join(self.local_workdir, "build", conf_file_name) for conf_file_name in conf_file_names]
        # conf_dst_file_paths = [f"/tmp/core-site.xml", f"/tmp/hdfs-site.xml", f"/tmp/flink-conf.yaml", f"/tmp/workers"]
        # assert len(conf_file_paths) == len(conf_dst_file_paths)
        for i, conf_file_data in enumerate(conf_files_data):
            # Create the file in a temporary directory
            with open(conf_file_paths[i], "w") as f:
                print(f"Writing {conf_file_paths[i]}")
                print(conf_file_data)
                f.write(conf_file_data)

        # for src_file_path, dst_file_path in zip(conf_file_paths, conf_dst_file_paths):
        #     print(f"Copying {src_file_path} to {dst_file_path}")
        #     copy_remote_public_ip(src_file_path, dst_file_path, self.dev_ssh_user, self.dev_ssh_key, self.dev_vm)

    def copy_back_build_files(self):
        remote_repo = f"/home/{self.dev_ssh_user}/repo/psl-cvm"
        TARGET_BINARIES = ["client", "controller", "server", "net-perf"]

        # Copy the target/release to build directory
        for bin in TARGET_BINARIES:
            print(f"Copying {remote_repo}/target/release/{bin} to {os.path.join(self.local_workdir, 'build', bin)}")
            copy_file_from_remote_public_ip(f"{remote_repo}/target/release/{bin}", os.path.join(self.local_workdir, "build", bin), self.dev_ssh_user, self.dev_ssh_key, self.dev_vm)


    def bins_already_exist(self):
        pass
        # TARGET_BINARIES = [self.workload]
        # remote_repo = f"/home/{self.dev_ssh_user}/repo"

        # remote_script_dir = f"{remote_repo}/scripts_v2/loadtest"
        # TARGET_SCRIPTS = ["load.py", "locustfile.py", "docker-compose.yml", "toggle.py", "shamir.py", "zipfian.py"]


        # res1 = run_remote_public_ip([
        #     f"find {remote_script_dir}"
        # ], self.dev_ssh_user, self.dev_ssh_key, self.dev_vm, hide=True)

        # res2 = run_remote_public_ip([
        #     f"ls {remote_repo}/target/release"
        # ], self.dev_ssh_user, self.dev_ssh_key, self.dev_vm, hide=True)

        # return all([bin in res2[0] for bin in TARGET_BINARIES]) and all([script in res1[0] for script in TARGET_SCRIPTS])
    

    def generate_configs(self, deployment: Deployment, config_dir, log_dir):
        # If config_dir is not empty, assume the configs have already been generated
        # if len(os.listdir(config_dir)) > 0:
        #     print("Skipping config generation for experiment", self.name)
        #     return
        # Number of nodes in deployment may be < number of nodes in deployment
        # So we reuse nodes.
        # As a default, each deployed node gets its unique port number
        # So there will be no port clash.

        rr_cnt = 0
        nodelist = []
        nodes = {}
        node_configs = {}
        vms = []
        node_list_for_crypto = {}


        vms = deployment.get_all_client_vms()
        assert len(vms) > 1, "Flink requires at least 2 client VMs"

        flink_vm = vms[0] # this is also the client VM
        vms = vms[1:]
        self.binary_mapping = defaultdict(list)
        self.binary_mapping[flink_vm].append("flink_leader")
        for vm in vms:
            self.binary_mapping[vm].append("flink_worker")
        self.getRequestHosts = []

        # if self.node_distribution == "uniform":
        #     vms = deployment.get_all_node_vms()
        # elif self.node_distribution == "sev_only":
        #     vms = deployment.get_nodes_with_tee("sev")
        # elif self.node_distribution == "tdx_only":
        #     vms = deployment.get_nodes_with_tee("tdx")
        # elif self.node_distribution == "nontee_only":
        #     vms = deployment.get_nodes_with_tee("nontee")
        # else:
        #     vms = deployment.get_wan_setup(self.node_distribution)

        (node_vms, storage_vms, sequencer_vms) = self.get_vms(deployment)
        print("Node vms", node_vms)
        print("Storage vms", storage_vms)
        print("Sequencer vms", sequencer_vms)
        print("Client vms", deployment.get_all_client_vms())

        worker_names = []
        storage_names = []
        sequencer_names = []
        port = deployment.node_port_base

        for node_num in range(1, self.num_nodes+1):
            port += 1
            listen_addr = f"0.0.0.0:{port}"
            name = f"node{node_num}"
            domain = f"{name}.pft.org"

            _vm = node_vms[rr_cnt % len(node_vms)]
            self.binary_mapping[_vm].append(name)
            
            private_ip = _vm.private_ip
            rr_cnt += 1
            connect_addr = f"{private_ip}:{port}"

            nodelist.append(name[:])
            nodes[name] = {
                "addr": connect_addr,
                "domain": domain
            }

            worker_names.append(name)

            node_list_for_crypto[name] = (connect_addr, domain)

            config = deepcopy(self.base_node_config)
            config["net_config"]["name"] = name
            config["net_config"]["addr"] = listen_addr
            data_dir = os.path.join(self.data_dir, f"{name}-db")
            config["consensus_config"]["log_storage_config"]["RocksDB"]["db_path"] = str(data_dir)
            config["worker_config"]["state_storage_config"]["RocksDB"]["db_path"] = str(data_dir)

            node_configs[name] = config

        for node_num in range(1, self.num_storage_nodes+1):
            port += 1
            listen_addr = f"0.0.0.0:{port}"
            name = f"storage{node_num}"
            domain = f"{name}.pft.org"

            _vm = storage_vms[rr_cnt % len(storage_vms)]
            self.binary_mapping[_vm].append(name)

            storage_names.append(name)
            
            private_ip = _vm.private_ip
            rr_cnt += 1
            connect_addr = f"{private_ip}:{port}"
            
            nodelist.append(name[:])
            nodes[name] = {
                "addr": connect_addr,
                "domain": domain
            }

            node_list_for_crypto[name] = (connect_addr, domain)

            config = deepcopy(self.base_node_config)
            config["net_config"]["name"] = name
            config["net_config"]["addr"] = listen_addr
            data_dir = os.path.join(self.data_dir, f"{name}-db")
            config["consensus_config"]["log_storage_config"]["RocksDB"]["db_path"] = str(data_dir)

            node_configs[name] = config

        for node_num in range(1, self.num_sequencer_nodes+1):
            port += 1
            listen_addr = f"0.0.0.0:{port}"
            name = f"sequencer{node_num}"
            domain = f"{name}.pft.org"
            
            _vm = sequencer_vms[rr_cnt % len(sequencer_vms)]
            self.binary_mapping[_vm].append(name)
            
            private_ip = _vm.private_ip
            rr_cnt += 1
            connect_addr = f"{private_ip}:{port}"
            
            nodelist.append(name[:])
            nodes[name] = {
                "addr": connect_addr,
                "domain": domain
            }
            sequencer_names.append(name)

            node_list_for_crypto[name] = (connect_addr, domain)

            config = deepcopy(self.base_node_config)
            config["net_config"]["name"] = name
            config["net_config"]["addr"] = listen_addr
            data_dir = os.path.join(self.data_dir, f"{name}-db")
            config["consensus_config"]["log_storage_config"]["RocksDB"]["db_path"] = str(data_dir)

            node_configs[name] = config

        node_names = ["hdfs-leader"]
        for node_num, name in enumerate(node_names):
            print(f"name: {name}, node_num: {node_num}")
            port += 1
            listen_addr = f"0.0.0.0:{port}"
            domain = f"{name}.pft.org"

            _vm = vms[rr_cnt % len(vms)]
            self.binary_mapping[_vm].append(name)
            
            private_ip = _vm.private_ip
            rr_cnt += 1
            connect_addr = f"{private_ip}:{port}"

            nodelist.append(name[:])
            nodes[name] = {
                "addr": connect_addr,
                "domain": domain
            }
            self.getRequestHosts.append(f"http://{private_ip}:{port + 1000}") # This +1000 is hardcoded in contrib/kms/main.rs

            node_list_for_crypto[name] = (connect_addr, domain)

            config = deepcopy(self.base_node_config)
            config["net_config"]["name"] = name
            config["net_config"]["addr"] = listen_addr
            config["consensus_config"]["log_storage_config"]["RocksDB"]["db_path"] = f"{log_dir}/{name}-db"

            node_configs[name] = config        

        if self.client_region == -1:
            client_vms = deployment.get_all_client_vms()
        else:
            client_vms = deployment.get_all_client_vms_in_region(self.client_region)

        self.client_vms = client_vms[:]

        crypto_info = self.gen_crypto(config_dir, node_list_for_crypto, len(client_vms))
        gossip_downstream_worker_list = self.generate_multicast_tree(worker_names, 2)
        sequencer_watchlist_map = self.generate_watchlists(sequencer_names, worker_names)
        

        for k, v in node_configs.items():
            tls_cert_path, tls_key_path, tls_root_ca_cert_path,\
            allowed_keylist_path, signing_priv_key_path = crypto_info[k]

            v["net_config"]["nodes"] = deepcopy(nodes)

            if k in sequencer_names:
                v["consensus_config"]["node_list"] = storage_names[:]
            else:
                v["consensus_config"]["node_list"] = nodelist[:]

            if k in worker_names:
                v["worker_config"]["gossip_downstream_worker_list"] = gossip_downstream_worker_list[k]
            else:
                v["worker_config"]["gossip_downstream_worker_list"] = []

            # if True or k == storage_names[0]:
            v["consensus_config"]["learner_list"] = sequencer_names[:]

            if k in sequencer_names:
                v["consensus_config"]["watchlist"] = sequencer_watchlist_map.get(k, [])

            v["net_config"]["tls_cert_path"] = tls_cert_path
            v["net_config"]["tls_key_path"] = tls_key_path
            v["net_config"]["tls_root_ca_cert_path"] = tls_root_ca_cert_path
            v["rpc_config"]["allowed_keylist_path"] = allowed_keylist_path
            v["rpc_config"]["signing_priv_key_path"] = signing_priv_key_path
            v["worker_config"]["all_worker_list"] = worker_names[:]
            v["worker_config"]["storage_list"] = storage_names[:] 
            v["worker_config"]["sequencer_list"] = sequencer_names[:]

            # Only simulate Byzantine behavior in node1.
            if "evil_config" in v and v["evil_config"]["simulate_byzantine_behavior"] and k != "node1":
                v["evil_config"]["simulate_byzantine_behavior"] = False
                v["evil_config"]["byzantine_start_block"] = 0

            with open(os.path.join(config_dir, f"{k}_config.json"), "w") as f:
                json.dump(v, f, indent=4)

        self.getDistribution = self.base_client_config.get("getDistribution", 50)
        self.workers_per_client = self.base_client_config.get("workers_per_client", 1)
        self.total_client_vms = len(client_vms)
        self.total_worker_processes = self.total_client_vms * self.workers_per_client
        self.locust_master = self.client_vms[0]
        self.workload = self.base_client_config.get("workload", "kms")

        self.total_machines = self.workers_per_client * self.total_client_vms

        users_per_clients = [self.num_clients // self.total_machines] * self.total_machines
        users_per_clients[-1] += self.num_clients - sum(users_per_clients)
        self.users_per_clients = users_per_clients

        print(f"binary_mapping: {self.binary_mapping}")

    def generate_arbiter_script(self):
        nn_host_public, node1_host_public = self.extract_service_hosts_public_ip()
        nn_host_private, node1_host_private = self.extract_service_hosts_private_ip()

        script_lines = [
            "#!/bin/bash",
            "set -e",
            "set -o xtrace",
            "",
            "# This script is generated by the experiment pipeline. DO NOT EDIT.",
            f'SSH_CMD="ssh -o StrictHostKeyChecking=no -i {self.dev_ssh_key}"',
            f'SCP_CMD="scp -o StrictHostKeyChecking=no -i {self.dev_ssh_key}"',
            ""
        ]

        for vm, bin_list in self.binary_mapping.items():
            print(f"vm: {vm}, bin_list: {bin_list}")
            
            # Boot up the nodes first
            for bin in bin_list:
                if "flink_leader" in bin:
                    cmd_block = []
                elif "flink_worker" in bin:
                    cmd_block = [
                        f"$SSH_CMD {self.dev_ssh_user}@{vm.public_ip} '",
                        f"  sudo sed -i.bak -e \"s/\\blocalhost\\b/{nn_host_private}/g\" /usr/local/hadoop/etc/hadoop/hdfs-site.xml;",
                        f"  sudo sed -i.bak -e \"s/\\blocalhost\\b/{nn_host_private}/g\" /usr/local/hadoop/etc/hadoop/core-site.xml;",
                        f"cp -f {self.local_workdir}/build/core-site.xml /home/psladmin/flink-psl/build-target/conf/core-site.xml;",
                        f"cp -f {self.local_workdir}/build/hdfs-site.xml /home/psladmin/flink-psl/build-target/conf/hdfs-site.xml;",
                        f"cp -f {self.local_workdir}/build/flink-conf.yaml /home/psladmin/flink-psl/build-target/conf/flink-conf.yaml \\",
                        f"  > {self.remote_workdir}/logs/{bin}.log \\",
                        f"  2> {self.remote_workdir}/logs/{bin}.err' &",
                        'PID="$PID $!"',
                        "",
                        "sleep 1",
                    ]
                elif "hdfs" in bin:
                    cmd_block = [
                        f"$SSH_CMD {self.dev_ssh_user}@{vm.public_ip} '",
                        f"  sudo sed -i.bak -e \"s/\\blocalhost\\b/{nn_host_private}/g\" /usr/local/hadoop/etc/hadoop/hdfs-site.xml;",
                        f"  sudo sed -i.bak -e \"s/\\blocalhost\\b/{nn_host_private}/g\" /usr/local/hadoop/etc/hadoop/core-site.xml;",
                        f"  /usr/local/hadoop/bin/hdfs namenode -format -force -nonInteractive; /usr/local/hadoop/sbin/start-dfs.sh \\",
                        f"  > {self.remote_workdir}/logs/{bin}.log \\",
                        f"  2> {self.remote_workdir}/logs/{bin}.err' &",
                        'PID="$PID $!"',
                        "",
                        "sleep 5",
                    ]
                elif "node" in bin:
                    binary_name = "server worker"
                    cmd_block = [
                        f"$SSH_CMD {self.dev_ssh_user}@{vm.public_ip} '",
                        f"  sudo {self.remote_workdir}/build/{binary_name} {self.remote_workdir}/configs/{bin}_config.json > {self.remote_workdir}/logs/{bin}.log 2> {self.remote_workdir}/logs/{bin}.err' &",
                        'PID="$PID $!"',
                        "",
                        "sleep 1",
                    ]
                elif "storage" in bin:
                    binary_name = "server storage"
                    cmd_block = [
                        f"$SSH_CMD {self.dev_ssh_user}@{vm.public_ip} '",
                        f"  sudo {self.remote_workdir}/build/{binary_name} {self.remote_workdir}/configs/{bin}_config.json > {self.remote_workdir}/logs/{bin}.log 2> {self.remote_workdir}/logs/{bin}.err' &",
                        'PID="$PID $!"',
                        "",
                        "sleep 1",
                    ]
                elif "sequencer" in bin:
                    binary_name = "server sequencer"
                    cmd_block = [
                        f"$SSH_CMD {self.dev_ssh_user}@{vm.public_ip} '",
                        f"  sudo {self.remote_workdir}/build/{binary_name} {self.remote_workdir}/configs/{bin}_config.json > {self.remote_workdir}/logs/{bin}.log 2> {self.remote_workdir}/logs/{bin}.err' &",
                        'PID="$PID $!"',
                        "",
                        "sleep 1",
                    ]
                elif "client" in bin:
                    binary_name = "client"
                    cmd_block = [
                        f"$SSH_CMD {self.dev_ssh_user}@{vm.public_ip} '",
                        f"  sudo {self.remote_workdir}/build/{binary_name} {self.remote_workdir}/configs/{bin}_config.json > {self.remote_workdir}/logs/{bin}.log 2> {self.remote_workdir}/logs/{bin}.err' &",
                        'PID="$PID $!"',
                        "",
                        "sleep 1",
                    ]
                else:
                    assert False, f"bin: {bin}"
                script_lines.extend(cmd_block)

        remote_repo = f"/home/{self.dev_ssh_user}/repo"

        script_lines.extend([
            f"cp -f {self.local_workdir}/build/core-site.xml /home/psladmin/flink-psl/build-target/conf/core-site.xml",
            f"cp -f {self.local_workdir}/build/hdfs-site.xml /home/psladmin/flink-psl/build-target/conf/hdfs-site.xml",
            f"cp -f {self.local_workdir}/build/core-site.xml /usr/local/hadoop/etc/hadoop/core-site.xml",
            f"cp -f {self.local_workdir}/build/hdfs-site.xml /usr/local/hadoop/etc/hadoop/hdfs-site.xml",
            f"cp -f {self.local_workdir}/build/flink-conf.yaml /home/psladmin/flink-psl/build-target/conf/flink-conf.yaml",
            f"cp -f {self.local_workdir}/build/workers /home/psladmin/flink-psl/build-target/conf/workers",
        ])

        NUM_LOG_LINES = 500000

        script_lines.extend([
            f"sudo chmod +x /etc/profile.d/bigdata_env.sh;",
            f". /etc/profile.d/bigdata_env.sh;",
            f"/usr/local/hadoop/bin/hdfs dfs -mkdir -p /datasets/fiu",
            f"head -n {NUM_LOG_LINES} {remote_repo}/flink-psl/traces/write-heavy.blkparse > {remote_repo}/flink-psl/traces/write-heavy-truncated.blkparse",
            f"/usr/local/hadoop/bin/hdfs dfs -put {remote_repo}/flink-psl/traces/write-heavy-truncated.blkparse  /datasets/fiu/",
        ])

        script_lines.append("sleep 5")
        script_lines.extend([
            f"FLINK_SSH_OPTS=\"-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i {self.dev_ssh_key}\" /home/psladmin/flink-psl/build-target/bin/stop-cluster.sh",
            f"nohup env FLINK_SSH_OPTS=\"-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i {self.dev_ssh_key}\" /home/psladmin/flink-psl/build-target/bin/start-cluster.sh > /home/psladmin/flink-psl/start-cluster.out 2> /home/psladmin/flink-psl/start-cluster.err < /dev/null &",
        ])
        
        with open(os.path.join(self.local_workdir, f"arbiter_0.sh"), "w") as f:
            f.write("\n".join(script_lines) + "\n\n")

    def get_vms(self, deployment: Deployment) -> Tuple[List, List, List]:
        node_vms = deployment.get_all_node_vms()
        storage_vms = deployment.get_all_storage_vms()
        sequencer_vms = deployment.get_all_node_vms()
        return node_vms, storage_vms, sequencer_vms
    
    def generate_multicast_tree(self, worker_names: List[str], fanout: int) -> Dict[str, List[str]]:
        """
        Generate a multicast tree for the given worker names with a given fanout.
        Returns the gossip_downstream_worker_list for each worker.
        """

        ret = {}
        i = 0

        # Assume that the list is the flattened complete tree.
        # The children of index i are i * fanout + 1, i * fanout + 2, ..., i * fanout + fanout.
        # The parent of index i is (i - 1) // fanout.
        # Each worker sends to its children and its parent.

        while i < len(worker_names):
            curr = worker_names[i]
            children = [worker_names[i * fanout + j] for j in range(1, fanout + 1) if i * fanout + j < len(worker_names)]
            parent = (i - 1) // fanout
            ret[curr] = children[:]
            if parent >= 0:
                ret[curr].append(worker_names[parent])
            i += 1

        return ret
        # return {k: [] for k in worker_names}
    
    def generate_watchlists(self, sequencer_names: List[str], worker_names: List[str]) -> Dict[str, List[str]]:
        """
        Generate a watchlist for each sequencer.
        If there is only one sequencer, it gets the first 4 workers.
        Otherwise, worker_names split evenly between the sequencers.
        """
        ret = defaultdict(list)
        if len(sequencer_names) == 1:
            ret[sequencer_names[0]] = worker_names[:4]
            return dict(ret)

        curr_sequencer_idx = 0
        for worker_name in worker_names:
            ret[sequencer_names[curr_sequencer_idx]].append(worker_name)
            curr_sequencer_idx = (curr_sequencer_idx + 1) % len(sequencer_names)
        return dict(ret)

           
