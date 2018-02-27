#!/bin/bash
# Install Pip3 and update
if [ $EUID != 0 ]; then
    sudo "$0" "$@"
    exit $?
fi
sudo apt -y install python3-pip
sudo -H pip3 install --upgrade pip
sudo -H pip3 install elasticsearch
