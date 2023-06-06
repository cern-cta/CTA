## Install a CentOS VM on CentOS

*Source: https://www.linuxtechi.com/install-kvm-hypervisor-on-centos-7-and-rhel-7/*

1. Download the CERN Centos here : http://linuxsoft.cern.ch/cern/centos/7/os/x86_64/images/boot.iso

2. Install Virtual Machine Manager and KVM / QEMU related libs :
```bash
sudo yum install qemu-kvm qemu-img virt-manager libvirt libvirt-python libvirt-client virt-install virt-viewer bridge-utils
```

3. Start and enable the libvirtd service :
```bash
sudo systemctl start libvirtd
sudo systemctl enable libvirtd
```

4. Launch virt-manager by typing :
```bash
virt-manager
```  
5. Create a new virtual machine by clicking on the corresponding button
- Choose Local install media
- Use ISO image, browse
- Set the Memory (RAM) you want (I suggest using 8192 Mib)
- Set the number of CPU you want (I suggest 2)
- Create a disk image for the virtual machine (choose the size you allocate for the disk)
- Name the machine and click Finish

6. Install the CERN CentOS following the instructions here : http://linux.web.cern.ch/linux/centos7/docs/install.shtml

## Setting up CTA environment in your VM
1. Clone the project from the CTA's gitlab page : https://gitlab.cern.ch/cta/CTA
2. go to the CTA folder and init the xrootd-ssi-profobuf-interface submodule :
```bash
git submodule init
git submodule update
```

3. Go to CTA/continuousintegration/buildtree_runner/vmBootstrap and type the following commands :
```bash
cp gitScripts/.git-* ~
cat gitScripts/bash_profile.hook >> ~/.bash_profile
source ~/.bash_profile
cp tigConf/tigrc ~/.tigrc
yum install -y centos-release-scl-rh
yum install -y git cmake rpm-build devtoolset-11 vim gdb cgdb strace ltrace screen tig lsscsi mt-st mtx sg3_utils jq psmisc yum-plugin-versionlock
```
4. Bootstrap CTA :
```bash
cat > ~/path_to_CTA_folder/.git/hooks/post-checkout << EOFGitHook
#!/bin/sh
cd `git rev-parse --show-toplevel`
git submodule update --init --recursive
EOFGitHook

chmod +x ~/path_to_CTA_folder/.git/hooks/post-checkout

cp ~/path_to_CTA_folder/.git/hooks/post-checkout ~/path_to_CTA_folder/.git/hooks/post-merge
```
5. Build CTA's srpm
Go to the CTA parent's folder and type
```bash
mkdir CTA-build-srpm
cd CTA-build-srpm
source /opt/rh/devtoolset-11/enable
cmake -DPackageOnly:Bool=true ../CTA
make cta_srpm
```

6. Install repositories necessary for CTA's compilation works
```bash
cd ~/path_to_CTA_folder/continuousintegration/buildtree_runner/vmBootstrap
sudo yum-config-manager --add-repo=../../docker/ctafrontend/cc7/etc/yum.repos.d/eos-citrine-depend.repo
sudo yum-config-manager --add-repo=../../docker/ctafrontend/cc7/etc/yum.repos.d/cta-ci.repo
sudo yum-config-manager --add-repo=../../docker/ctafrontend/cc7/etc/yum.repos.d/castor.repo
sudo yum-config-manager --add-repo=ceph-internal.repo
```
7. Install yum-plugin-priorities
```bash
sudo yum install -y yum-plugin-priorities
```

8. Add versionlock for xrootd
```bash
sudo cp ../../docker/ctafrontend/cc7/etc/yum/pluginconf.d/versionlock.list  /etc/yum/pluginconf.d/versionlock.list
```

9. Install build dependencies
10.Verify you have the CentOS-CERN.repo, epel.repo and elrepo.repo in /etc/yum.repos.d/
Then, go to the CTA-build-srpm directory and execute :
```bash
sudo yum-builddep -y ./RPM/SRPMS/cta-0-1.src.rpm --nogpgcheck
```
10. Install mhvtl
```bash
sudo yum install -y mhvtl-utils kmod-mhvtl mhvtl-utils --enablerepo=castor
```
11. Build CTA
Create a directory near the CTA one called CTA-build, go into it and type
```bash
source /opt/rh/devtoolset-11/enable
cmake ../CTA
make -j 4
```
12. Run the unit tests of CTA located in CTA-build/tests
```bash
./cta-unitTests
```

If tests don't run, edit the /etc/resolv.conf file and add a line with
```
search cern.ch
```

## Setting an Oracle database for the CTA catalogue
1. Install the Oracle client
 ```bash
 sudo yum install -y oracle-instantclient12.2-basic.x86_64
```
2. Install sqlplus that will allow you to query the CTA catalogue
```bash
#if necessary, enable all the repos
sudo yum install oracle-instantclient12.2-sqlplus.x86_64
```
You can test that the sqlplus runs via the command *sqlplus64*.

3. Install **oracle tns name** that will allow you to get all the informations about the database connection by just providing its name.
```bash
sudo yum install -y oracle-instantclient-tnsnames.ora.noarch
```
4. Update the **tns name** database by typing
```bash
sudo /etc/cron.hourly/tnsnames-update.cron
```
5. Install **rlwrap** tool that will make your life easier while dealing with sqlplus (allows to have an history of the typed commands for example)
```bash
sudo yum install rlwrap
```
6. Ask for your database credentials : *username*, *database*, *password*.
7. Try to connect to the database and execute a simple sql query.
```bash
rlwrap sqlplus64 username@database
#Then type your password
```
```sql
select table_name from user_tables;
```
