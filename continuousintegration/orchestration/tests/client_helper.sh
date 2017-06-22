CTAADMIN_USER="ctaadmin2"
USER="user1"

die() {
  echo "$@" 1>&2
  exit 1
}

admin_cta() {
  KRB5CCNAME=/tmp/${CTAADMIN_USER}/krb5cc_0 cta $@
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
