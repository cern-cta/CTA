#!/bin/bash -e

set -x

echo Preparing CTA sources...
cat > ~/CTA/.git/hooks/post-checkout << EOFGitHook
#!/bin/sh
cd `git rev-parse --show-toplevel`
git submodule update --init --recursive
EOFGitHook

chmod +x ~/CTA/.git/hooks/post-checkout
cp ~/CTA/.git/hooks/post-checkout ~/CTA/.git/hooks/post-merge

echo Creating source rpm
rm -rf ~/CTA-build-srpm
mkdir -p ~/CTA-build-srpm
(cd ~/CTA-build-srpm && cmake -DPackageOnly:Bool=true ../CTA; make cta_srpm)

echo Installing repos
for r in `ls -1 ~/CTA/continuousintegration/docker/ctafrontend/cc7/etc/yum.repos.d/*.repo` ; do
  sudo yum-config-manager --add-repo=$r
done
sudo yum install -y yum-plugin-priorities

sudo wget https://public-yum.oracle.com/RPM-GPG-KEY-oracle-ol7 -O /etc/pki/rpm-gpg/RPM-GPG-KEY-oracle
sudo tee /etc/yum.repos.d/oracle-instant-client.repo > /dev/null << 'EOF'
[oracle-instant-client]
name=Oracle instant client
baseurl=https://yum.oracle.com/repo/OracleLinux/OL7/oracle/instantclient/x86_64/
gpgkey=file:///etc/pki/rpm-gpg/RPM-GPG-KEY-oracle
gpgcheck=1
enabled=0
EOF

echo Adding versionlock for xrootd:
sudo cp ~/CTA/continuousintegration/docker/ctafrontend/cc7/etc/yum/pluginconf.d/versionlock.list /etc/yum/pluginconf.d/versionlock.list

echo Installing build dependencies
sudo yum-builddep -y ~/CTA-build-srpm/RPM/SRPMS/cta-0-1.src.rpm --nogpgcheck --enablerepo=oracle-instant-client

echo Installing mhvtl
sudo yum install -y mhvtl-utils kmod-mhvtl mhvtl-utils --enablerepo=castor

echo Building CTA
mkdir -p ~/CTA-build
(cd ~/CTA-build && cmake ../CTA; make -j 4)

echo Tuning the system for EOS
sudo bash -c "echo net.ipv4.tcp_tw_reuse = 1 >> /etc/sysctl.d/00-eos.conf"
sudo sysctl -p

echo CTA setup finished successfully
