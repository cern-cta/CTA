#!/bin/bash

echo "Setting up DNS..."

dns_clusterip='10.254.199.254'
dns_domainname=`hostname -d`
my_ipaddress=`hostname -i`
upper_level_dns=`perl -e 'while (<>) { if ($_=~ /nameserver (.*)/) { print $1."\n"; exit }}' < /etc/resolv.conf`

dnspoddir=$(mktemp -d)
cp dns-svc.yaml pod-dns.yaml ${dnspoddir}

perl -p -i -e 's/<%= \@dns_clusterip -%>/'"${dns_clusterip}"'/' ${dnspoddir}/dns-svc.yaml

perl -p -i -e 's/<%= \@dns_domainname -%>/'"${dns_domainname}"'/' ${dnspoddir}/pod-dns.yaml
perl -p -i -e 's/<%= \@my_ipaddress -%>/'"${my_ipaddress}"'/' ${dnspoddir}/pod-dns.yaml
perl -p -i -e 's/<%= \@upper_level_dns -%>/'"${upper_level_dns}"'/' ${dnspoddir}/pod-dns.yaml


kubectl create -f ./kube-system_ns.yaml

kubectl create -f ${dnspoddir}/dns-svc.yaml

kubectl create -f ${dnspoddir}/pod-dns.yaml

echo "DONE"
