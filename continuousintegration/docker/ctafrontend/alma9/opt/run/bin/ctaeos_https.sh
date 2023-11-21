#!/bin/bash

# Move latest mkcert binary to where mkcert-ssl.sh expects it
cp /opt/run/bin/external/mkcert-v*-linux-amd64 /usr/bin/mkcert
chmod +x /usr/bin/mkcert

# run vanilla mkcert-ssl.sh
bash /opt/run/bin/external/mkcert-ssl.sh
# Make sure that CAROOT has proper rights
chown -R daemon:daemon /etc/grid-security/certificates/*

if [ $? -eq 0 ]; then
  echo "Host certificate created"
else
  echo "ERROR: Host certificate creation failed!!!"
fi
