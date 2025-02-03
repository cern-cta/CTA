def setup_tests(env):
    # TODO: make sure tests fail if this fails
    # TODO: at some point we can migrate some of these functions to the frontend host class
    env.ctafrontend.exec("cta-catalogue-schema-verify /etc/cta/cta-catalogue.conf")
    env.ctafrontend.exec("cta-catalogue-admin-user-create /etc/cta/cta-catalogue.conf --username ctaadmin1 --comment \"Admin 1\"")
    env.ctacli.exec("cta-admin version")
    env.eosmgm.exec("eos version")


    env.client.exec("mkdir -p /tmp/ctaadmin2")
    env.client.exec("mkdir -p /tmp/poweruser1")
    env.client.exec("mkdir -p /tmp/eosadmin1")

    # TODO: we leave the cleanup of previous runs to later, as this doesn't work properly anyway (better to just call reset catalogue and remove eos stuff)
