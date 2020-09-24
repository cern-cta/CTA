#!/bin/bash

PUBLIC=true
if [[ $1 == "cern" ]]; then
  PUBLIC=false
  echo Going to install from internal CERN repositories
fi

echo Installing mhvtl
if [[ "$PUBLIC" == false ]] ; then
  sudo yum install -y mhvtl-utils kmod-mhvtl --enablerepo=castor
else
  sudo yum install -y make gcc zlib-devel lzo-devel kernel kernel-tools kernel-devel kernel-headers
  git clone --depth 1 -b 1.6-3_release https://github.com/markh794/mhvtl.git
  cd mhvtl
  make
  sudo make install
  cd kernel
  make
  sudo make install 

  sudo make_vtl_media -C /etc/mhvtl
  sudo systemctl start mhvtl.target
  echo "Please check the result of 'make install'. If it has failed, reboot and rerun this script"  
fi

echo "mhvtl bootstrap finished"
