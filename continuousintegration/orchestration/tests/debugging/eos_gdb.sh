#!/bin/bash

EOS_COMMIT=4e6e89bb
#EOS_COMMIT=5.3.16

# Move to root
cd /

# Clone EOS project (to cover for missing debuginfo)
git clone https://gitlab.cern.ch/dss/eos.git
(cd eos; git checkout ${EOS_COMMIT})

# Install debuginfo for other dependencies
dnf -y debuginfo-install eos-xrootd-5.8.3-3.el9.x86_64
dnf -y debuginfo-install activemq-cpp-3.9.5-1.el9.x86_64 apr-1.7.0-12.el9_3.x86_64 bzip2-libs-1.0.8-10.el9_5.x86_64 cyrus-sasl-lib-2.1.27-21.el9.x86_64 eos-grpc-1.56.1-4.el9.x86_64 expat-2.5.0-5.el9_6.x86_64 fmt-8.1.1-5.el9.x86_64 glibc-2.34-168.el9_6.20.x86_64 jemalloc-5.2.1-2.el9.x86_64 json-c-0.14-11.el9.x86_64 jsoncpp-1.9.5-1.el9.x86_64 keyutils-libs-1.6.3-1.el9.x86_64 krb5-libs-1.21.1-8.el9_6.x86_64 libbrotli-1.0.9-7.el9_5.x86_64 libcap-2.48-9.el9_2.x86_64 libcom_err-1.46.5-7.el9.x86_64 libcurl-7.76.1-31.el9.x86_64 libevent-2.1.12-8.el9_4.x86_64 libffi-3.4.2-8.el9.x86_64 libidn2-2.3.0-7.el9.x86_64 libmacaroons-0.3.0-9.el9.x86_64 libnghttp2-1.43.0-6.el9.x86_64 libpsl-0.21.1-5.el9.x86_64 libselinux-3.6-3.el9.x86_64 libsodium-1.0.18-8.el9.x86_64 libssh-0.10.4-13.el9.x86_64 libstdc++-11.5.0-5.el9_5.alma.1.x86_64 libunistring-0.9.10-15.el9.x86_64 libuuid-2.37.4-21.el9.x86_64 libxcrypt-4.4.18-3.el9.x86_64 libxml2-2.9.13-9.el9_6.x86_64 libzstd-1.5.5-1.el9.x86_64 lz4-libs-1.9.3-5.el9.x86_64 ncurses-libs-6.2-10.20210508.el9.x86_64 openldap-2.6.8-4.el9.x86_64 openpgm-5.2.122-28.el9.x86_64 openssl-libs-3.2.2-6.el9_5.1.x86_64 p11-kit-0.25.3-3.el9_5.x86_64 pcre2-10.40-6.el9.x86_64 scitokens-cpp-1.1.3-2.el9.x86_64 snappy-1.1.8-8.el9.x86_64 sqlite-libs-3.34.1-7.el9_3.x86_64 sssd-client-2.9.6-4.el9_6.2.x86_64 voms-2.1.2-1.el9.x86_64 xz-libs-5.2.5-8.el9_0.x86_64 zeromq-4.3.4-2.el9.x86_64 zlib-1.2.11-40.el9.x86_64
dnf -y debuginfo-install libunwind-1.6.2-1.el9.x86_64 readline-8.1-4.el9.x86_64 systemd-libs-252-51.el9_6.1.alma.1.x86_64

# Create gdb config file
gdb_eos_config=/gdb_eos_config
cat > $gdb_eos_config <<EOF
set debuginfod enabled off
set substitute-path /root/rpmbuild/BUILD/eos-5.3.16-1 /eos
directory /eos
EOF

# Finally, start GDB
gdb -x $gdb_eos_config -p 1
