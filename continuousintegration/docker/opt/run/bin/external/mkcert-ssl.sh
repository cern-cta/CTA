#!/bin/bash

################################################################
# Set up a ssl-compliant dev environment for eos tests
################################################################


usage() { echo '''Usage: mkcert-ssl.sh [-s|--setup-rootCA][-h|--help]
                     Setup /etc/grid-security/{daemon/certificates} directories with issued credentials
                     --setup-rootCA  is useful to generate a common root CA certificates that may be common to all the images generated
'''; }

onlyRootCA=0

# One loop, nothing more.
if [ "$#" != 0 ]; then
  EOL=$(printf '\1\3\3\7')
  set -- "$@" "$EOL"
  while [ "$1" != "$EOL" ]; do
    opt="$1"; shift
    case "$opt" in

      # Your options go here.
      -s|--setup-rootCA)  onlyRootCA=1;;
      -h|--help) usage; exit 0;;

      # Arguments processing. You may remove any unneeded line after the 1st.
      -|''|[!-]*) set -- "$@" "$opt";;                                          # positional argument, rotate to the end
      --*=*)      set -- "${opt%%=*}" "${opt#*=}" "$@";;                        # convert '--name=arg' to '--name' 'arg'
      -[!-]?*)    set -- "$(echo "${opt#-}" | sed 's    /g')" "$@";;     # convert '-abc' to '-a' '-b' '-c'
      --)         while [ "$1" != "$EOL" ]; do set -- "$@" "$1"; shift; done;;  # process remaining arguments as positional
      -*)         usage_error "unknown option: '$opt'";;                        # catch misspelled options
      *)          usage_error "this should NEVER happen ($opt)";;               # sanity test for previous patterns

    esac
  done
  shift  # $EOL
fi

if ! [[ -f /usr/bin/mkcert ]]; then
  curl https://github.com/FiloSottile/mkcert/releases/download/v1.4.3/mkcert-v1.4.3-linux-amd64 -L --output /usr/bin/mkcert
  chmod +x /usr/bin/mkcert
fi


# CA and host
if [[ $onlyRootCA -eq 1 ]]; then
	CAROOT=/etc/grid-security/certificates/ mkcert -install
        yum install -y wget openssl-perl
        c_rehash /etc/grid-security/certificates/
	# Make sure root CA certificates are owned by daemon
	chown -R daemon:daemon /etc/grid-security/certificates/*
else
	mkdir -p /etc/grid-security/daemon/ /etc/grid-security/certificates/
	# In case /etc/grid-security/certificates/ exists, all the host certs have to use the same rootCA
	CAROOT=/etc/grid-security/certificates/ mkcert -install -cert-file /etc/grid-security/daemon/hostcert.pem -key-file /etc/grid-security/daemon/hostkey.pem $(hostname -f)
	
	chown daemon:daemon /etc/grid-security/daemon/hostcert.pem
	chown daemon:daemon /etc/grid-security/daemon/hostkey.pem
	
	# user
	mkdir -p ~/.globus
	CAROOT=/etc/grid-security/certificates/ mkcert -client -cert-file ~/.globus/usercert.pem -key-file ~/.globus/userkey.pem eos-user
	
	# grid-mapfile
	echo '"/O=mkcert development certificate/OU=root@eos-mgm1" eos-user' > /etc/grid-security/grid-mapfile
	
	# Note:
	# - To try this out, you need a properly configured /etc/xrd.cf.mgm, with:
	#     - sec.protobind gsi enabled
	#     - sec.protocol gsi -cert:<> -key:<> -gridmap:<> pointing to the right path. The defaults should do
	# - If the mgm has started with a wrong config, fix it and restart it, i.e., (typically)
	#     cd tmp && source /etc/sysconfig/eos && /opt/eos/xrootd//bin/xrootd -n mgm -c /etc/xrd.cf.mgm -m -l /var/log/eos/xrdlog.mgm -b -Rdaemon
	# - If everything is right, the following will succeed:
	#     XrdSecDEBUG=1 XrdSecPROTOCOL=gsi XRD_LOGLEVEL=Dump xrdfs root://eos-mgm1 stat /eos
	#
fi
