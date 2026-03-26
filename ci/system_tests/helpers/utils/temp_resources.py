# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# RAII structures that create a temporary entry in the catalogue and clean up after themselves


class TempDiskInstanceSpace:
    def __init__(self, cta_cli, dis_name, di_name):
        self.cta_cli = cta_cli
        self.dis_name = dis_name
        self.di_name = di_name

    def __enter__(self):
        self.ls_before = self.cta_cli.execWithOutput("cta-admin --json ds ls")
        self.cta_cli.exec(
            f"cta-admin dis add -n {self.dis_name} --di {self.di_name} -i 10 -u eosSpace:default -m 'Add temp disk instance system'"
        )
        return self

    def __exit__(self, exc_type, exc, tb):
        self.cta_cli.exec(f"cta-admin dis rm -n {self.dis_name} --di {self.di_name}")
        assert self.ls_before == self.cta_cli.execWithOutput("cta-admin --json ds ls")
        return False


class TempLogicalLibrary:
    def __init__(self, cta_cli, ll_name, pl_name):
        self.cta_cli = cta_cli
        self.ll_name = ll_name
        self.pl_name = pl_name

    def __enter__(self):
        self.ls_before = self.cta_cli.execWithOutput("cta-admin --json ll ls")
        self.cta_cli.exec(
            f"cta-admin ll add --name {self.ll_name} --pl {self.pl_name} --comment 'Add temp logical library'"
        )
        return self

    def __exit__(self, exc_type, exc, tb):
        self.cta_cli.exec(f"cta-admin ll rm --name {self.ll_name}")
        assert self.ls_before == self.cta_cli.execWithOutput("cta-admin --json ll ls")
        return False


class TempPhysicalLibrary:
    def __init__(self, cta_cli, pl_name):
        self.cta_cli = cta_cli
        self.pl_name = pl_name

    def __enter__(self):
        self.ls_before = self.cta_cli.execWithOutput("cta-admin --json pl ls")
        self.cta_cli.exec(f"cta-admin pl add --name {self.pl_name} --ma man --mo mod --npcs 3 --npds 4")
        return self

    def __exit__(self, exc_type, exc, tb):
        self.cta_cli.exec(f"cta-admin pl rm --name {self.pl_name}")
        assert self.ls_before == self.cta_cli.execWithOutput("cta-admin --json pl ls")
        return False


class TempMountPolicy:
    def __init__(self, cta_cli, mp_name):
        self.cta_cli = cta_cli
        self.mp_name = mp_name

    def __enter__(self):
        self.ls_before = self.cta_cli.execWithOutput("cta-admin --json mp ls")
        self.cta_cli.exec(f"cta-admin mp add -n {self.mp_name} --ap 2 --aa 2 --rp 2 --ra 1 -m 'Add temp mount policy'")
        return self

    def __exit__(self, exc_type, exc, tb):
        self.cta_cli.exec(f"cta-admin mp rm --name {self.mp_name}")
        assert self.ls_before == self.cta_cli.execWithOutput("cta-admin --json mp ls")
        return False


class TempVirtualOrganization:
    def __init__(self, cta_cli, vo_name, di_name, extra_flags=""):
        self.cta_cli = cta_cli
        self.vo_name = vo_name
        self.di_name = di_name
        self.extra_flags = extra_flags

    def __enter__(self):
        self.ls_before = self.cta_cli.execWithOutput("cta-admin --json vo ls")
        self.cta_cli.exec(
            f"cta-admin vo add --vo '{self.vo_name}' --rmd 1 --wmd 1 --di '{self.di_name}' -m 'Add temp virtual organization' {self.extra_flags}"
        )
        return self

    def __exit__(self, exc_type, exc, tb):
        self.cta_cli.exec(f"cta-admin vo rm --vo {self.vo_name}")
        assert self.ls_before == self.cta_cli.execWithOutput("cta-admin --json vo ls")
        return False


class TempStorageClass:
    def __init__(self, cta_cli, sc_name, vo_name):
        self.cta_cli = cta_cli
        self.sc_name = sc_name
        self.vo_name = vo_name

    def __enter__(self):
        self.ls_before = self.cta_cli.execWithOutput("cta-admin --json sc ls")
        self.cta_cli.exec(f"cta-admin sc add -n {self.sc_name} -c 1 --vo {self.vo_name} -m 'Add temp storage class'")
        return self

    def __exit__(self, exc_type, exc, tb):
        self.cta_cli.exec(f"cta-admin sc rm --name {self.sc_name}")
        assert self.ls_before == self.cta_cli.execWithOutput("cta-admin --json sc ls")
        return False


class TempTapePool:
    def __init__(self, cta_cli, tp_name, vo_name):
        self.cta_cli = cta_cli
        self.tp_name = tp_name
        self.vo_name = vo_name

    def __enter__(self):
        self.ls_before = self.cta_cli.execWithOutput("cta-admin --json tp ls")
        self.cta_cli.exec(f"cta-admin tp add -n '{self.tp_name}' --vo {self.vo_name} -p 0 -m 'Add temp tape pool'")
        return self

    def __exit__(self, exc_type, exc, tb):
        self.cta_cli.exec(f"cta-admin tp rm --name {self.tp_name}")
        assert self.ls_before == self.cta_cli.execWithOutput("cta-admin --json tp ls")
        return False


class TempTape:
    def __init__(self, cta_cli, vid, ll_name, tp_name):
        self.cta_cli = cta_cli
        self.vid = vid
        self.ll_name = ll_name
        self.tp_name = tp_name

    def __enter__(self):
        self.ls_before = self.cta_cli.execWithOutput("cta-admin --json ta ls --all")
        self.cta_cli.exec(
            f"cta-admin ta add -v {self.vid} --mt LTO9 --ve tempvendor -l {self.ll_name} -t {self.tp_name} -f false --purchaseorder temporder -m 'Add temp tape'"
        )
        return self

    def __exit__(self, exc_type, exc, tb):
        self.cta_cli.exec(f"cta-admin ta rm -v {self.vid}")
        assert self.ls_before == self.cta_cli.execWithOutput("cta-admin --json ta ls --all")
        return False
