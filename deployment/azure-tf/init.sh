#!/bin/bash

# Copyright (c) Shubham Mishra. All rights reserved.
# Licensed under the MIT License.


# Docker keys and repos
sudo apt-get update
sudo apt-get install ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc

# Add the repository to Apt sources:
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update

# Install Docker
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# Non-root run
sudo usermod -aG docker pftadmin

# Restart on reboot
sudo systemctl enable docker.service
sudo systemctl enable containerd.service

# Pirateship dependencies
sudo apt-get install -y screen
sudo apt-get install -y build-essential cmake clang llvm pkg-config
sudo apt-get install -y jq
sudo apt-get install -y protobuf-compiler
sudo apt-get install -y linux-tools-common linux-tools-generic linux-tools-`uname -r`
sudo apt-get install -y net-tools
sudo apt-get install -y ca-certificates curl libssl-dev
sudo apt-get install -y librocksdb-dev libprotobuf-dev
sudo apt-get install -y python3-pip python3-virtualenv


# Increase open file limits

echo "*	soft	nofile	50000" >> /etc/security/limits.conf
echo "*	hard	nofile	50000" >> /etc/security/limits.conf


# Mount the data disk


sudo mkfs.ext4 /dev/sdc
sudo mkdir /data
sudo mount /dev/sdc /data
sudo chmod -R 777 /data

# Handle the Flink setup
set -euxo pipefail

# --- Seed env: cloud-init may not set HOME/USER ---
# CHANGED: Default fallback user changed from psladmin to pftadmin
TARGET_USER="${SUDO_USER:-pftadmin}"
TARGET_HOME="$(getent passwd "$TARGET_USER" | cut -d: -f6 || echo "/home/$TARGET_USER")"
: "${USER:=$TARGET_USER}"
# FORCE HOME to target home to ensure SSH keys go to the right place if running as root
HOME="$TARGET_HOME"
TARGET_GROUP="$(id -gn "$TARGET_USER" 2>/dev/null || echo "$TARGET_USER")"

# ------- versions & paths -----------------------------------------------------
HADOOP_VER=${HADOOP_VER:-3.3.3}
HIVE_VER=${HIVE_VER:-3.1.2}

HADOOP_PREFIX=/usr/local
HIVE_PREFIX=/usr/local
HADOOP_HOME=${HADOOP_PREFIX}/hadoop
HIVE_HOME=${HIVE_PREFIX}/hive

HDFS_BASE=/var/lib/hadoop/hdfs
MS_DIR=${MS_DIR:-$TARGET_HOME/hive-metastore}
MS_PORT=${MS_PORT:-9083}
HS2_PORT=${HS2_PORT:-10000}

# ------- base deps + Java -----------------------------------------------------
sudo apt-get update -y
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y \
  curl wget tar gzip bzip2 unzip rsync openssh-client openssh-server \
  net-tools iproute2 procps ca-certificates gnupg lsb-release \
  libsnappy1v5 libzstd1 liblz4-1 libbz2-1.0 git bison flex build-essential byacc

# Prefer JDK 11; fall back to JDK 8 if missing
# if ! sudo apt-get install -y openjdk-11-jdk; then
sudo apt-get install -y openjdk-8-jdk
# fi
sudo systemctl enable --now ssh || true

# Detect JAVA_HOME
JAVA_BIN="$(readlink -f "$(command -v java)")"
JAVA_HOME="$(dirname "$(dirname "$JAVA_BIN")")"
JAVA8_HOME=/usr/lib/jvm/java-8-openjdk-amd64

# Set the default java to java 8
if [ -x "$JAVA8_HOME/bin/java" ]; then
  update-alternatives --install /usr/bin/java  java  "$JAVA8_HOME/bin/java"  10800
  update-alternatives --install /usr/bin/javac javac "$JAVA8_HOME/bin/javac" 10800
  # (optional, nice to have)
  update-alternatives --install /usr/bin/jar   jar   "$JAVA8_HOME/bin/jar"   10800
  update-alternatives --install /usr/bin/jps   jps   "$JAVA8_HOME/bin/jps"   10800

  update-alternatives --set java  "$JAVA8_HOME/bin/java"
  update-alternatives --set javac "$JAVA8_HOME/bin/javac"
  # (optional)
  update-alternatives --set jar   "$JAVA8_HOME/bin/jar"   || true
  update-alternatives --set jps   "$JAVA8_HOME/bin/jps"   || true
else
  echo "ERROR: JAVA8_HOME not valid: $JAVA8_HOME" >&2
fi

# ------- env (one file for both Hadoop & Hive) --------------------------------
# CHANGED: Updated Flink paths to point to pftadmin home
sudo bash -c "cat > /etc/profile.d/bigdata_env.sh" <<EOF
export JAVA_HOME="$JAVA8_HOME"
# Hadoop / Hive paths (keep your existing values)
export HADOOP_HOME="$HADOOP_HOME"
export HADOOP_CONF_DIR="\$HADOOP_HOME/etc/hadoop"
export HIVE_HOME="$HIVE_HOME"
export HIVE_CONF_DIR="\$HIVE_HOME/conf"
export YARN_CONF_DIR="\$HADOOP_HOME/etc/hadoop"
export FLINK_HOME="/home/pftadmin/flink-psl/build-target"
export FLINK_CONF_DIR="/home/pftadmin/flink-psl/build-target/conf"
export FLINK_SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"

# PATH and Hadoop client classpath
export PATH="\$JAVA_HOME/bin:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin:\$HIVE_HOME/bin:\$PATH"
export HADOOP_CLASSPATH="/usr/local/hadoop/etc/hadoop:/usr/local/hadoop/share/hadoop/common/lib/*:/usr/local/hadoop/share/hadoop/common/*:/usr/local/hadoop/share/hadoop/hdfs:/usr/local/hadoop/share/hadoop/hdfs/lib/*:/usr/local/hadoop/share/hadoop/hdfs/*:/usr/local/hadoop/share/hadoop/mapreduce/*:/usr/local/hadoop/share/hadoop/yarn:/usr/local/hadoop/share/hadoop/yarn/lib/*:/usr/local/hadoop/share/hadoop/yarn/*"
EOF
# shellcheck disable=SC1091
source /etc/profile.d/bigdata_env.sh

# ------- Download tarballs from GitHub Releases -------------------------------
RELEASE_BASE="${RELEASE_BASE:-https://github.com/alexthomasv/hdfs-3.3.3/releases/download/flink}"
HADOOP_URL="${HADOOP_URL:-$RELEASE_BASE/hadoop-${HADOOP_VER}.tar.gz}"
HIVE_URL="${HIVE_URL:-$RELEASE_BASE/apache-hive-${HIVE_VER}-bin.tar.gz}"

TMP_DL_DIR="${TMP_DL_DIR:-$TARGET_HOME/tmp-hdfs-dl}"
rm -rf "$TMP_DL_DIR"; mkdir -p "$TMP_DL_DIR"

download() {
  # $1=url $2=outpath
  curl -fL --retry 5 --retry-delay 2 --continue-at - -o "$2" "$1"
}

ensure_tar_gz() {
  # $1=path
  [ -f "$1" ] && tar -tzf "$1" >/dev/null
}

HADOOP_TGZ_PATH="$TMP_DL_DIR/hadoop-${HADOOP_VER}.tar.gz"
HIVE_TGZ_PATH="$TMP_DL_DIR/apache-hive-${HIVE_VER}-bin.tar.gz"

download "$HADOOP_URL" "$HADOOP_TGZ_PATH"
download "$HIVE_URL"   "$HIVE_TGZ_PATH"

ensure_tar_gz "$HADOOP_TGZ_PATH"
ensure_tar_gz "$HIVE_TGZ_PATH"

# ------- Hadoop install from release tar --------------------------------------
sudo mkdir -p "$HADOOP_PREFIX"
sudo tar -xzf "$HADOOP_TGZ_PATH" -C "$HADOOP_PREFIX"
sudo ln -sfn "hadoop-${HADOOP_VER}" "$HADOOP_HOME"
sudo chown -R "$TARGET_USER:$TARGET_GROUP" "$HADOOP_PREFIX/hadoop-${HADOOP_VER}" "$HADOOP_HOME"
mkdir -p "$HADOOP_HOME/logs"

# Minimal Hadoop configs (single-node / localhost)
CONF_DIR="$HADOOP_HOME/etc/hadoop"
sudo sed -i "s|^#\s*export JAVA_HOME=.*|export JAVA_HOME=$JAVA_HOME|" "$CONF_DIR/hadoop-env.sh"

# CHANGED: Updated proxyuser from psladmin to pftadmin
sudo bash -c "cat > '$CONF_DIR/core-site.xml'" <<XML
<configuration>
  <property><name>fs.defaultFS</name><value>hdfs://localhost:9000</value></property>
  
  <property>
    <name>hadoop.proxyuser.pftadmin.hosts</name>
    <value>*</value>
  </property>
  <property>
    <name>hadoop.proxyuser.pftadmin.groups</name>
    <value>*</value>
  </property>
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

sudo bash -c "cat > '$HADOOP_HOME/etc/hadoop/mapred-site.xml'" <<'XML'
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>

  <property>
    <name>yarn.app.mapreduce.am.env</name>
    <value>HADOOP_MAPRED_HOME=/usr/local/hadoop</value>
  </property>
  <property>
    <name>mapreduce.map.env</name>
    <value>HADOOP_MAPRED_HOME=/usr/local/hadoop</value>
  </property>
  <property>
    <name>mapreduce.reduce.env</name>
    <value>HADOOP_MAPRED_HOME=/usr/local/hadoop</value>
  </property>

  <property>
    <name>mapreduce.application.classpath</name>
    <value>
      /usr/local/hadoop/share/hadoop/mapreduce/*,
      /usr/local/hadoop/share/hadoop/mapreduce/lib/*,
      /usr/local/hadoop/share/hadoop/common/*,
      /usr/local/hadoop/share/hadoop/common/lib/*,
      /usr/local/hadoop/share/hadoop/hdfs/*,
      /usr/local/hadoop/share/hadoop/hdfs/lib/*,
      /usr/local/hadoop/share/hadoop/yarn/*,
      /usr/local/hadoop/share/hadoop/yarn/lib/*
    </value>
  </property>
</configuration>
XML

echo "localhost" | sudo tee "$CONF_DIR/workers" >/dev/null

# Create local NN/DN dirs & make writable; set log dirs
sudo mkdir -p "${HDFS_BASE}/namenode" "${HDFS_BASE}/datanode"
sudo chown -R "$TARGET_USER:$TARGET_GROUP" "$HDFS_BASE" || true
echo 'export HADOOP_LOG_DIR=$HADOOP_HOME/logs' | sudo tee -a "$CONF_DIR/hadoop-env.sh" >/dev/null
echo 'export YARN_LOG_DIR=$HADOOP_HOME/logs'   | sudo tee -a "$CONF_DIR/yarn-env.sh"   >/dev/null

# Make the logs directory writable
# CHANGED: Calculated group dynamically for pftadmin (or target user)
GRP="$(id -gn $TARGET_USER)"
sudo mkdir -p /usr/local/hadoop-3.3.3/logs
# CHANGED: chown psladmin -> $TARGET_USER
sudo chown -R $TARGET_USER:"$GRP" /usr/local/hadoop-3.3.3 /usr/local/hadoop

sudo mkdir -p /var/lib/hadoop/hdfs/namenode /var/lib/hadoop/hdfs/namenode/current /var/lib/hadoop/hdfs/datanode
# CHANGED: chown psladmin -> $TARGET_USER
sudo chown -R $TARGET_USER:"$GRP" /var/lib/hadoop/hdfs

# Cleanup downloads
rm -rf "$TMP_DL_DIR" || true

# Config
ZIP_URL="https://fiustorage12345.blob.core.windows.net/fiu-traces/build-target.zip"
# CHANGED: psladmin -> pftadmin
DEST_DIR="/home/pftadmin/flink-psl"

sudo mkdir -p "$DEST_DIR"
TMPZIP="$(mktemp)"
wget -q -O "$TMPZIP" "$ZIP_URL"

# Unzip (overwrite if re-running)
sudo unzip -o "$TMPZIP" -d "$DEST_DIR"
rm -f "$TMPZIP"

# Give ownership to pftadmin:$GRP
# CHANGED: chown psladmin -> $TARGET_USER
sudo chown -R $TARGET_USER:"$GRP" "$DEST_DIR"

# (Optional) make sure log dir exists and is writable by pftadmin
sudo mkdir -p "$DEST_DIR/build-target/log"
# CHANGED: chown psladmin -> $TARGET_USER
sudo chown -R $TARGET_USER:"$GRP" "$DEST_DIR/build-target/log"
sudo find "$DEST_DIR/build-target/log" -type d -exec chmod 755 {} \;
sudo find "$DEST_DIR/build-target/log" -type f -exec chmod 644 {} \;

# Flink tmp dirs (java.io.tmpdir + shuffle)
GRP="${GRP:-$(id -gn $TARGET_USER)}"
# CHANGED: paths /home/psladmin -> /home/pftadmin
sudo mkdir -p /home/pftadmin/flink-tmp/{tmp,shuffle}
# CHANGED: chown psladmin -> $TARGET_USER
sudo chown -R $TARGET_USER:"$GRP" /home/pftadmin/flink-tmp
sudo find /home/pftadmin/flink-tmp -type d -exec chmod 755 {} \;
sudo find /home/pftadmin/flink-tmp -type f -exec chmod 644 {} \;

BASE_FIFO_DIR=/tmp/psl_fifo
MODE=660                 # rw for user+group
NUM_FIFO_QUEUES=${NUM_FIFO_QUEUES:-4}   # creates 1..N

sudo mkdir -p "$BASE_FIFO_DIR"
# CHANGED: chown pftadmin -> $TARGET_USER
sudo chown $TARGET_USER:"$GRP" "$BASE_FIFO_DIR"
sudo chmod 755 "$BASE_FIFO_DIR"

for i in $(seq 1 "$NUM_FIFO_QUEUES"); do
  sudo mkfifo -m "$MODE" "$BASE_FIFO_DIR/psl_fifo_in$i"
  sudo mkfifo -m "$MODE" "$BASE_FIFO_DIR/psl_fifo_out$i"
  # CHANGED: chown pftadmin -> $TARGET_USER
  sudo chown $TARGET_USER:"$GRP" "$BASE_FIFO_DIR/psl_fifo_in$i" "$BASE_FIFO_DIR/psl_fifo_out$i"
done

# ------- Passwordless self-SSH for Hadoop scripts (required by start-dfs.sh) --
# sshd should already be enabled above, but this is idempotent
sudo systemctl enable --now ssh || true

# Create an SSH key for the current user if missing
# Note: $HOME matches $TARGET_HOME due to override at top of script
mkdir -p "$HOME/.ssh" && chmod 700 "$HOME/.ssh"
if [ ! -f "$HOME/.ssh/id_ed25519" ]; then
  ssh-keygen -q -t ed25519 -N "" -f "$HOME/.ssh/id_ed25519"
fi

# Authorize our own public key (self-SSH)
cat "$HOME/.ssh/id_ed25519.pub" >> "$HOME/.ssh/authorized_keys"
sort -u "$HOME/.ssh/authorized_keys" -o "$HOME/.ssh/authorized_keys"
chmod 600 "$HOME/.ssh/authorized_keys"
chown -R "$TARGET_USER:$TARGET_GROUP" "$HOME/.ssh"

# Pre-accept local host keys to avoid prompts
ssh-keyscan -H localhost 127.0.0.1 "$(hostname -f)" 2>/dev/null >> "$HOME/.ssh/known_hosts" || true
chmod 644 "$HOME/.ssh/known_hosts"

# Tell Hadoop which SSH options/key to use
echo 'export HADOOP_SSH_OPTS="-o StrictHostKeyChecking=no -i $HOME/.ssh/id_ed25519"' \
  | sudo tee -a "$HADOOP_CONF_DIR/hadoop-env.sh" >/dev/null

# For a single-node setup, ensure masters/workers target localhost
echo localhost | sudo tee "$HADOOP_CONF_DIR/masters"  >/dev/null
echo localhost | sudo tee "$HADOOP_CONF_DIR/workers"  >/dev/null

# Sanity (does not prompt for password)
ssh -o StrictHostKeyChecking=no localhost true || true
