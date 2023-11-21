#!/bin/bash

wget https://download.oracle.com/otn_software/linux/instantclient/1919000/oracle-instantclient19.19-basic-19.19.0.0.0-1.el9.x86_64.rpm
wget https://download.oracle.com/otn_software/linux/instantclient/1919000/oracle-instantclient19.19-devel-19.19.0.0.0-1.el9.x86_64.rpm
wget https://cernbox.cern.ch/remote.php/dav/public-files/KpgLqb7gURQp6Qd/libocci_gcc53.so.19.1
yum install -y oracle-instantclient19.19-basic-19.19.0.0.0-1.el9.x86_64.rpm
yum install -y oracle-instantclient19.19-devel-19.19.0.0.0-1.el9.x86_64.rpm
yum install -y oracle-instantclient-tnsnames.ora

occi_folder=$(find /usr/ | grep libocci.so.19.1 | xargs dirname)
cp libocci_gcc53.so.19.1 $occi_folder
ls -l $occi_folder

git apply continuousintegration/docker/ctafrontend/alma9/code.patch
