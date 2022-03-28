# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2022 CERN
# @license      This program is free software, distributed under the terms of the GNU General Public
#               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
#               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
#               option) any later version.
#
#               This program is distributed in the hope that it will be useful, but WITHOUT ANY
#               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
#               PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
#               In applying this licence, CERN does not waive the privileges and immunities
#               granted to it by virtue of its status as an Intergovernmental Organization or
#               submit itself to any jurisdiction.


###
# Helper functions for tests running on client pod.
#
# to use in your tests symply source this file:
# . /root/client_helper.sh on the client pod
#
# admin_kinit: kinit for CTAADMIN_USER
# admin_klist: klist for CTAADMIN_USER
# admin_kdestroy: kdestroy for CTAADMIN_USER
# admin_cta: runs a cta command as CTAADMIN_USER

EOSPOWER_USER="poweruser1"
CTAADMIN_USER="ctaadmin2"
EOSADMIN_USER="eosadmin1"
USER="user1"

die() {
  echo "$@" 1>&2
  exit 1
}

user_kinit() {
  kinit -kt /root/${USER}.keytab ${USER}@TEST.CTA
  klist
}

admin_cta() {
  KRB5CCNAME=/tmp/${CTAADMIN_USER}/krb5cc_0 cta-admin "$@" 2>/dev/null
}

admin_klist() {
  KRB5CCNAME=/tmp/${CTAADMIN_USER}/krb5cc_0 klist
}

admin_kinit() {
  KRB5CCNAME=/tmp/${CTAADMIN_USER}/krb5cc_0 kinit -kt /root/${CTAADMIN_USER}.keytab ${CTAADMIN_USER}@TEST.CTA
  admin_klist
}

admin_kdestroy() {
  KRB5CCNAME=/tmp/${CTAADMIN_USER}/krb5cc_0 kdestroy
  admin_klist
}

eospower_eos() {
  XrdSecPROTOCOL=krb5 KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 eos $@
}

eospower_klist() {
  KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 klist
}

eospower_kinit() {
  KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 kinit -kt /root/${EOSPOWER_USER}.keytab ${EOSPOWER_USER}@TEST.CTA
  eospower_klist
}

eospower_kdestroy() {
  KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 kdestroy
  eospower_klist
}

eosadmin_eos() {
  XrdSecPROTOCOL=krb5 KRB5CCNAME=/tmp/${EOSADMIN_USER}/krb5cc_0 eos -r 0 0 $@
}

eosadmin_klist() {
  KRB5CCNAME=/tmp/${EOSADMIN_USER}/krb5cc_0 klist
}

eosadmin_kinit() {
  KRB5CCNAME=/tmp/${EOSADMIN_USER}/krb5cc_0 kinit -kt /root/${EOSADMIN_USER}.keytab ${EOSADMIN_USER}@TEST.CTA
  eosadmin_klist
}

eosadmin_kdestroy() {
  KRB5CCNAME=/tmp/${EOSADMIN_USER}/krb5cc_0 kdestroy
  eosadmin_klist
}
