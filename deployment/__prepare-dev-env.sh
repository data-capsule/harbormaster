#!/bin/bash

# Copyright (c) Shubham Mishra. All rights reserved.
# Licensed under the MIT License.

curl https://sh.rustup.rs -sSf | sh -s -- -y

export DEBIAN_FRONTEND=noninteractive

# Function to wait for apt locks to be released
wait_for_apt() {
    echo "Checking for apt locks..."
    while sudo fuser /var/lib/dpkg/lock >/dev/null 2>&1 \
       || sudo fuser /var/lib/dpkg/lock-frontend >/dev/null 2>&1 \
       || sudo fuser /var/lib/apt/lists/lock >/dev/null 2>&1 \
       || pgrep -f "apt-get" >/dev/null \
       || pgrep -f "dpkg" >/dev/null \
       || pgrep -f "unattended-upgr" >/dev/null; do
        echo "Waiting for other software managers to finish..."
        sleep 5
    done
}

sudo systemctl stop unattended-upgrades.service 2>/dev/null || true

# Wait for locks to clear
wait_for_apt

# Fix interrupted installs if previous runs crashed
sudo dpkg --configure -a || true

sudo apt-get update
sudo apt-get install -y git

# By default bashrc is not read when using ssh 'command' mode
# So we need to remove/comment out those lines.

cp ideal_bashrc $HOME/.bashrc
