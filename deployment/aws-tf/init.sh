#!/bin/bash

# Copyright (c) Shubham Mishra. All rights reserved.
# Licensed under the MIT License.

# Runs as root.
# Change the default username from ubuntu to psladmin
usermod -l psladmin ubuntu
usermod -m -d /home/psladmin psladmin

# Add the psladmin user to the sudoers file
echo "psladmin ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers


# Docker keys and repos
apt-get update
apt-get install ca-certificates curl
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
chmod a+r /etc/apt/keyrings/docker.asc

# Add the repository to Apt sources:
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  tee /etc/apt/sources.list.d/docker.list > /dev/null
apt-get update

# Install Docker
apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# Non-root run
usermod -aG docker psladmin

# Restart on reboot
systemctl enable docker.service
systemctl enable containerd.service

# PSL dependencies
apt-get install -y screen
apt-get install -y build-essential cmake clang llvm pkg-config
apt-get install -y jq
apt-get install -y protobuf-compiler
apt-get install -y linux-tools-common linux-tools-generic linux-tools-`uname -r`
apt-get install -y net-tools
apt-get install -y ca-certificates curl libssl-dev
apt-get install -y librocksdb-dev libprotobuf-dev
apt-get install -y python3-pip python3-virtualenv


# Increase open file limits

echo "*	soft	nofile	50000" >> /etc/security/limits.conf
echo "*	hard	nofile	50000" >> /etc/security/limits.conf


# Mount the EBS SSD.
# AWS + Ubuntu 24.04 => The name for disk is /dev/nvme1n1
# It may not be present (for sevpool and clientpool)

if [ -b /dev/nvme1n1 ]; then
    mkfs.ext4 /dev/nvme1n1
    mkdir /data
    mount /dev/nvme1n1 /data
    chmod -R 777 /data
fi

# Handle the Flink setup
set -euxo pipefail

# --- Seed env: cloud-init may not set HOME/USER ---
TARGET_USER="${SUDO_USER:-psladmin}"
TARGET_HOME="$(getent passwd "$TARGET_USER" | cut -d: -f6 || echo "/home/$TARGET_USER")"
: "${USER:=$TARGET_USER}"
: "${HOME:=$TARGET_HOME}"
TARGET_GROUP="$(id -gn "$TARGET_USER" 2>/dev/null || echo "$TARGET_USER")"

# ------- versions & paths -----------------------------------------------------
HADOOP_VER=${HADOOP_VER:-3.3.3}
HIVE_VER=${HIVE_VER:-3.1.2}

HADOOP_PREFIX=/usr/local
HIVE_PREFIX=/usr/local
HADOOP_HOME=${HADOOP_PREFIX}/hadoop
HIVE_HOME=${HIVE_PREFIX}/hive

# REPO_URL that hosts the Hadoop and Hive tarballs (override if needed)
REPO_URL="${REPO_URL:-https://github.com/alexthomasv/hdfs-3.3.3.git}"
REPO_SUBPATH="${REPO_SUBPATH:-.}"  # where the tars live inside the repo

# Tarball names/paths inside the repo
HADOOP_TGZ="hadoop-${HADOOP_VER}.tar.gz"
HIVE_TGZ="apache-hive-${HIVE_VER}-bin.tar.gz"
HADOOP_TGZ_REPO_PATH="${HADOOP_TGZ_REPO_PATH:-$HADOOP_TGZ}"
HIVE_TGZ_REPO_PATH="${HIVE_TGZ_REPO_PATH:-$HIVE_TGZ}"

HDFS_BASE=/var/lib/hadoop/hdfs
MS_DIR=${MS_DIR:-$TARGET_HOME/hive-metastore}
MS_PORT=${MS_PORT:-9083}
HS2_PORT=${HS2_PORT:-10000}

# ------- base deps + Java + git-lfs ------------------------------------------
sudo apt-get update -y
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y \
  curl wget tar gzip bzip2 unzip rsync openssh-client openssh-server \
  net-tools iproute2 procps ca-certificates gnupg lsb-release \
  libsnappy1v5 libzstd1 liblz4-1 libbz2-1.0 git git-lfs

# Prefer JDK 11; fall back to JDK 8 if missing
if ! sudo apt-get install -y openjdk-11-jdk; then
  sudo apt-get install -y openjdk-8-jdk
fi
sudo systemctl enable --now ssh || true

# Detect JAVA_HOME
JAVA_BIN="$(readlink -f "$(command -v java)")"
JAVA_HOME="$(dirname "$(dirname "$JAVA_BIN")")"

# ------- env (one file for both Hadoop & Hive) --------------------------------
sudo bash -c "cat > /etc/profile.d/bigdata_env.sh" <<EOF
export JAVA_HOME="$JAVA_HOME"
export HADOOP_HOME="$HADOOP_HOME"
export HADOOP_CONF_DIR="\$HADOOP_HOME/etc/hadoop"
export HIVE_HOME="$HIVE_HOME"
export HIVE_CONF_DIR="\$HIVE_HOME/conf"
export PATH="\$JAVA_HOME/bin:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin:\$HIVE_HOME/bin:\$PATH"
EOF
# shellcheck disable=SC1091
source /etc/profile.d/bigdata_env.sh

# ------- Clone your repo once (Git LFS) ---------------------------------------
# Use a user-writable dir (avoid /tmp space issues)
TMP_CLONE="${TMP_CLONE:-$TARGET_HOME/tmpclone-hdfs}"
rm -rf "$TMP_CLONE"
mkdir -p "$TMP_CLONE"

# Install LFS config at system scope (avoids needing $HOME during cloud-init)
git lfs install --system --skip-repo || true

# Clone without LFS smudge, then fetch only the files we need
GIT_LFS_SKIP_SMUDGE=1 git clone --depth=1 "$REPO_URL" "$TMP_CLONE"
(
  cd "$TMP_CLONE/$REPO_SUBPATH"
  git lfs fetch --include="$HADOOP_TGZ_REPO_PATH,$HIVE_TGZ_REPO_PATH" --exclude=""
  git lfs checkout -- "$HADOOP_TGZ_REPO_PATH" "$HIVE_TGZ_REPO_PATH"
)

# Helper to assert a real tar (not an LFS pointer)
ensure_real_tar () {
  local path="$1"
  if [ ! -f "$path" ]; then
    echo "ERROR: missing tar: $path" >&2; exit 1
  fi
  if head -c 64 "$path" | grep -q 'git-lfs.github.com/spec/v1'; then
    echo "ERROR: $path is an LFS pointer, not the real tarball. Did 'git lfs fetch/checkout' run?" >&2
    exit 1
  fi
}

# ------- Hadoop install from repo tar -----------------------------------------
sudo mkdir -p "$HADOOP_PREFIX"
H_TAR="$TMP_CLONE/$REPO_SUBPATH/$HADOOP_TGZ_REPO_PATH"
ensure_real_tar "$H_TAR"
sudo tar -xzf "$H_TAR" -C "$HADOOP_PREFIX"
sudo ln -sfn "hadoop-${HADOOP_VER}" "$HADOOP_HOME"
sudo chown -R "$TARGET_USER:$TARGET_GROUP" "$HADOOP_PREFIX/hadoop-${HADOOP_VER}" "$HADOOP_HOME"
mkdir -p "$HADOOP_HOME/logs"

# Minimal Hadoop configs (single-node / localhost)
CONF_DIR="$HADOOP_HOME/etc/hadoop"
sudo sed -i "s|^#\s*export JAVA_HOME=.*|export JAVA_HOME=$JAVA_HOME|" "$CONF_DIR/hadoop-env.sh"

sudo bash -c "cat > '$CONF_DIR/core-site.xml'" <<'XML'
<configuration>
  <property><name>fs.defaultFS</name><value>hdfs://localhost:9000</value></property>
</configuration>
XML

sudo bash -c "cat > '$CONF_DIR/hdfs-site.xml'" <<XML
<configuration>
  <property><name>dfs.replication</name><value>1</value></property>
  <property><name>dfs.namenode.name.dir</name><value>file://${HDFS_BASE}/namenode</value></property>
  <property><name>dfs.datanode.data.dir</name><value>file://${HDFS_BASE}/datanode</value></property>
</configuration>
XML

sudo bash -c "cat > '$CONF_DIR/mapred-site.xml'" <<'XML'
<configuration>
  <property><name>mapreduce.framework.name</name><value>yarn</value></property>
</configuration>
XML

sudo bash -c "cat > '$CONF_DIR/yarn-site.xml'" <<'XML'
<configuration>
  <property><name>yarn.resourcemanager.hostname</name><value>localhost</value></property>
  <property><name>yarn.nodemanager.aux-services</name><value>mapreduce_shuffle</value></property>
</configuration>
XML

echo "localhost" | sudo tee "$CONF_DIR/workers" >/dev/null

# Create local NN/DN dirs & make writable; set log dirs
sudo mkdir -p "${HDFS_BASE}/namenode" "${HDFS_BASE}/datanode"
sudo chown -R "$TARGET_USER:$TARGET_GROUP" "$HDFS_BASE" || true
echo 'export HADOOP_LOG_DIR=$HADOOP_HOME/logs' | sudo tee -a "$CONF_DIR/hadoop-env.sh" >/dev/null
echo 'export YARN_LOG_DIR=$HADOOP_HOME/logs'   | sudo tee -a "$CONF_DIR/yarn-env.sh"   >/dev/null

# ------- Hive install from repo tar (via LFS) ---------------------------------
sudo mkdir -p "$HIVE_PREFIX"
HI_TAR="$TMP_CLONE/$REPO_SUBPATH/$HIVE_TGZ_REPO_PATH"
ensure_real_tar "$HI_TAR"
sudo tar -xzf "$HI_TAR" -C "$HIVE_PREFIX"
sudo ln -sfn "apache-hive-${HIVE_VER}-bin" "$HIVE_HOME"
sudo chown -R "$TARGET_USER:$TARGET_GROUP" "$HIVE_PREFIX/apache-hive-${HIVE_VER}-bin" "$HIVE_HOME"
mkdir -p "$HIVE_HOME/logs" "$HIVE_HOME/conf" "$MS_DIR"

# hive-site.xml (Derby + thrift metastore + HS2 bind)
cat > "$HIVE_HOME/conf/hive-site.xml" <<XML
<configuration>
  <property><name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:derby:${MS_DIR}/metastore_db;create=true</value></property>
  <property><name>javax.jdo.option.ConnectionDriverName</name>
    <value>org.apache.derby.jdbc.EmbeddedDriver</value></property>
  <property><name>hive.metastore.uris</name>
    <value>thrift://localhost:${MS_PORT}</value></property>
  <property><name>hive.metastore.warehouse.dir</name>
    <value>/user/hive/warehouse</value></property>
  <property><name>hive.metastore.schema.verification</name><value>true</value></property>
  <property><name>datanucleus.schema.autoCreateAll</name><value>false</value></property>
  <property><name>hive.server2.thrift.bind.host</name><value>0.0.0.0</value></property>
  <property><name>hive.server2.thrift.port</name><value>${HS2_PORT}</value></property>
</configuration>
XML

# Initialize metastore schema (idempotent for Derby)
schematool -dbType derby -initSchema --verbose || true

# Cleanup clone
rm -rf "$TMP_CLONE" || true

# Turns out AWS lets you login to the instance before this script ends executing.
# We will have a flag file to check for finishing.

echo "VM Ready" > /home/psladmin/ready.txt

