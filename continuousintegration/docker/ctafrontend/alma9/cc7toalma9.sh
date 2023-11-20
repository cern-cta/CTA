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

sed -i 's/protobuf3/protobuf/g' cta.spec.in
sed -i 's/instantclient19.3/instantclient19.19/g' cta.spec.in
sed -i 's/radosVersion 2:15.2.15/radosVersion 2:16.2.4/g' cta.spec.in
sed -i 's/, grpc-static//' cta.spec.in
sed -i 's/protobuf3/protobuf/g' cmake/FindProtobuf3.cmake
sed -i 's/lib64\/protobuf/lib64/g' cmake/FindProtobuf3.cmake
sed -i 's/protoc3/protoc/g' cmake/FindProtobuf3.cmake
sed -i 's/include\/protobuf/include/g' cmake/FindProtobuf3.cmake
sed -i 's/19.3/19.19/g' cmake/Findoracle-instantclient.cmake
sed -i 's/libocci.so/libocci_gcc53.so/g' cmake/Findoracle-instantclient.cmake
sed -i 's/EosCtaGrpc gpr/EosCtaGrpc gpr absl_synchronization/g' eos_cta/CMakeLists.txt
