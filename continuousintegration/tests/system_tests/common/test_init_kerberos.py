def test_kinit_clients(env):
    # TODO: this together with other options should go into a config file somewhere
    krb5_realm="TEST.CTA"
    env.ctacli[0].exec(f"kinit -kt /root/ctaadmin1.keytab ctaadmin1@{krb5_realm}")
    env.client[0].exec(f"kinit -kt /root/user1.keytab user1@{krb5_realm}")
