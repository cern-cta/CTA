#!/bin/bash -e

set -x

PUBLIC=true
if [[ $1 == "cern" ]]; then
  PUBLIC=false
  echo Going to install from internal CERN repositories
fi

echo Preparing CTA sources...
cd ~/CTA
git submodule update --init --recursive

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

if [[ "$PUBLIC" == false ]] ; then
  for r in `ls -1 ~/CTA/continuousintegration/docker/ctafrontend/cc7/etc/yum.repos.d/*.repo` ; do
    sudo yum-config-manager --add-repo=$r
  done
else 
  sudo wget https://public-yum.oracle.com/RPM-GPG-KEY-oracle-ol7 -O /etc/pki/rpm-gpg/RPM-GPG-KEY-oracle
  sudo tee /etc/yum.repos.d/oracle-instant-client.repo > /dev/null << 'EOF'
[oracle-instant-client]
name=Oracle instant client
baseurl=https://yum.oracle.com/repo/OracleLinux/OL7/oracle/instantclient/x86_64/
gpgkey=file:///etc/pki/rpm-gpg/RPM-GPG-KEY-oracle
gpgcheck=1
enabled=1
EOF

  sudo wget https://git.ceph.com/release.asc -O /etc/pki/rpm-gpg/RPM-ASC-KEY-ceph
  sudo tee /etc/yum.repos.d/ceph.repo > /dev/null << 'EOF'
[ceph]
name=Ceph
baseurl=https://download.ceph.com/rpm-nautilus/el7/x86_64/
gpgkey=file:///etc/pki/rpm-gpg/RPM-ASC-KEY-ceph
gpgcheck=1
enabled=1
EOF

  sudo yum-config-manager --add-repo=`ls -1 ~/CTA/continuousintegration/docker/ctafrontend/cc7/etc/yum.repos.d/eos-*.repo`
fi

sudo yum install -y yum-plugin-priorities
echo Adding versionlock for xrootd:
sudo cp ~/CTA/continuousintegration/docker/ctafrontend/cc7/etc/yum/pluginconf.d/versionlock.list /etc/yum/pluginconf.d/versionlock.list

echo Installing build dependencies
sudo yum-builddep -y ~/CTA-build-srpm/RPM/SRPMS/cta-0-1.src.rpm

echo Building CTA
mkdir -p ~/CTA-build
(cd ~/CTA-build && cmake ../CTA; make -j 4)

echo CTA setup finished successfully
