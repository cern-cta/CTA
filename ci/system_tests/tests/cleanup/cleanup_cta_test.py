# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from concurrent.futures import ThreadPoolExecutor

#####################################################################################################################
# CTA Teardown
#####################################################################################################################


def test_cleanup_catalogue(cta_frontend):
    schema_version = cta_frontend.get_schema_version()
    cta_frontend.exec("echo yes | cta-catalogue-schema-drop /etc/cta/cta-catalogue.conf")
    cta_frontend.exec(f"cta-catalogue-schema-create -v {schema_version} /etc/cta/cta-catalogue.conf")


def test_restart_cta(env):
    assert len(env.cta_maintd) > 0
    # CTA can't really survive a catalogue wipe, so restart all the pods. This also wipes all the test scripts and temp files so it ensures a clean slate
    hosts = env.cta_taped + env.cta_frontend + env.cta_maintd + env.cta_rmcd
    # Trigger concurrently as the initial restart commands takes a while (even without waiting for the pod to come up again). This way we ensure all pods are restarted simultaneously
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(host.restart, wait_for_restart=True) for host in hosts]

        for future in futures:
            future.result()
