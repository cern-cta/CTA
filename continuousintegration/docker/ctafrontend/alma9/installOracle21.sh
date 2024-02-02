#!/bin/bash

wget https://download.oracle.com/otn_software/linux/instantclient/2112000/el9/oracle-instantclient-basic-21.12.0.0.0-1.el9.x86_64.rpm
wget https://download.oracle.com/otn_software/linux/instantclient/2112000/el9/oracle-instantclient-devel-21.12.0.0.0-1.el9.x86_64.rpm
yum install -y oracle-instantclient-basic-21.12.0.0.0-1.el9.x86_64.rpm
yum install -y oracle-instantclient-devel-21.12.0.0.0-1.el9.x86_64.rpm
rm oracle-instantclient-basic-21.12.0.0.0-1.el9.x86_64.rpm oracle-instantclient-devel-21.12.0.0.0-1.el9.x86_64.rpm