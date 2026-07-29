# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from types import TracebackType
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from ..hosts import CtaCliHost

# RAII structures that create a temporary entry in the catalogue and clean up after themselves


class TempDiskInstanceSpace:
    def __init__(self, cta_cli: "CtaCliHost", dis_name: str, di_name: str) -> None:
        self.cta_cli = cta_cli
        self.dis_name = dis_name
        self.di_name = di_name

    def __enter__(self) -> "TempDiskInstanceSpace":
        self.ls_before = self.cta_cli.exec_with_output("cta-admin --json ds ls")
        self.cta_cli.exec(
            f"cta-admin dis add -n {self.dis_name} --di {self.di_name} -i 10 -u eosSpace:default -m "
            f"'Add temp disk instance system'"
        )
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> bool:
        self.cta_cli.exec(f"cta-admin dis rm -n {self.dis_name} --di {self.di_name}")
        assert self.ls_before == self.cta_cli.exec_with_output("cta-admin --json ds ls")
        return False


class TempLogicalLibrary:
    def __init__(self, cta_cli: "CtaCliHost", ll_name: str, pl_name: str) -> None:
        self.cta_cli = cta_cli
        self.ll_name = ll_name
        self.pl_name = pl_name

    def __enter__(self) -> "TempLogicalLibrary":
        self.ls_before = self.cta_cli.exec_with_output("cta-admin --json ll ls")
        self.cta_cli.exec(
            f"cta-admin ll add --name {self.ll_name} --pl {self.pl_name} --comment 'Add temp logical library'"
        )
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> bool:
        self.cta_cli.exec(f"cta-admin ll rm --name {self.ll_name}")
        assert self.ls_before == self.cta_cli.exec_with_output("cta-admin --json ll ls")
        return False


class TempPhysicalLibrary:
    def __init__(self, cta_cli: "CtaCliHost", pl_name: str) -> None:
        self.cta_cli = cta_cli
        self.pl_name = pl_name

    def __enter__(self) -> "TempPhysicalLibrary":
        self.ls_before = self.cta_cli.exec_with_output("cta-admin --json pl ls")
        self.cta_cli.exec(f"cta-admin pl add --name {self.pl_name} --ma man --mo mod --npcs 3 --npds 4")
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> bool:
        self.cta_cli.exec(f"cta-admin pl rm --name {self.pl_name}")
        assert self.ls_before == self.cta_cli.exec_with_output("cta-admin --json pl ls")
        return False


class TempMountPolicy:
    def __init__(self, cta_cli: "CtaCliHost", mp_name: str) -> None:
        self.cta_cli = cta_cli
        self.mp_name = mp_name

    def __enter__(self) -> "TempMountPolicy":
        self.ls_before = self.cta_cli.exec_with_output("cta-admin --json mp ls")
        self.cta_cli.exec(f"cta-admin mp add -n {self.mp_name} --ap 2 --aa 2 --rp 2 --ra 1 -m 'Add temp mount policy'")
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> bool:
        self.cta_cli.exec(f"cta-admin mp rm --name {self.mp_name}")
        assert self.ls_before == self.cta_cli.exec_with_output("cta-admin --json mp ls")
        return False


class TempVirtualOrganization:
    def __init__(self, cta_cli: "CtaCliHost", vo_name: str, di_name: str, extra_flags: str = "") -> None:
        self.cta_cli = cta_cli
        self.vo_name = vo_name
        self.di_name = di_name
        self.extra_flags = extra_flags

    def __enter__(self) -> "TempVirtualOrganization":
        self.ls_before = self.cta_cli.exec_with_output("cta-admin --json vo ls")
        self.cta_cli.exec(
            f"cta-admin vo add --vo '{self.vo_name}' --rmd 1 --wmd 1 --di '{self.di_name}' -m 'Add "
            f"temp virtual organization' {self.extra_flags}"
        )
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> bool:
        self.cta_cli.exec(f"cta-admin vo rm --vo {self.vo_name}")
        assert self.ls_before == self.cta_cli.exec_with_output("cta-admin --json vo ls")
        return False


class TempStorageClass:
    def __init__(self, cta_cli: "CtaCliHost", sc_name: str, vo_name: str, copies: int = 1) -> None:
        self.cta_cli = cta_cli
        self.sc_name = sc_name
        self.vo_name = vo_name
        self.copies = copies

    def __enter__(self) -> "TempStorageClass":
        self.ls_before = self.cta_cli.exec_with_output("cta-admin --json sc ls")
        self.cta_cli.exec(
            f"cta-admin sc add -n {self.sc_name} -c {self.copies} --vo {self.vo_name} -m 'Add temp storage class'"
        )
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> bool:
        self.cta_cli.exec(f"cta-admin sc rm --name {self.sc_name}")
        assert self.ls_before == self.cta_cli.exec_with_output("cta-admin --json sc ls")
        return False


class TempTapePool:
    def __init__(self, cta_cli: "CtaCliHost", tp_name: str, vo_name: str) -> None:
        self.cta_cli = cta_cli
        self.tp_name = tp_name
        self.vo_name = vo_name

    def __enter__(self) -> "TempTapePool":
        self.ls_before = self.cta_cli.exec_with_output("cta-admin --json tp ls")
        self.cta_cli.exec(f"cta-admin tp add -n '{self.tp_name}' --vo {self.vo_name} -p 0 -m 'Add temp tape pool'")
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> bool:
        self.cta_cli.exec(f"cta-admin tp rm --name {self.tp_name}")
        assert self.ls_before == self.cta_cli.exec_with_output("cta-admin --json tp ls")
        return False


class TempArchiveRoute:
    def __init__(self, cta_cli: "CtaCliHost", sc_name: str, tp_name: str, copynb: int) -> None:
        self.cta_cli = cta_cli
        self.tp_name = tp_name
        self.sc_name = sc_name
        self.copynb = copynb

    def __enter__(self) -> "TempArchiveRoute":
        self.ls_before = self.cta_cli.exec_with_output("cta-admin --json archiveroute ls")
        self.cta_cli.exec(
            f"cta-admin archiveroute add --storageclass '{self.sc_name}' --tapepool {self.tp_name} "
            f"--copynb {self.copynb} -m 'Add temp archive route'"
        )
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> bool:
        self.cta_cli.exec(
            f"cta-admin archiveroute rm --storageclass '{self.sc_name}' --copynb {self.copynb} "
            f"--archiveroutetype DEFAULT"
        )
        assert self.ls_before == self.cta_cli.exec_with_output("cta-admin --json archiveroute ls")
        return False


class TempTape:
    def __init__(self, cta_cli: "CtaCliHost", vid: str, ll_name: str, tp_name: str) -> None:
        self.cta_cli = cta_cli
        self.vid = vid
        self.ll_name = ll_name
        self.tp_name = tp_name

    def __enter__(self) -> "TempTape":
        self.ls_before = self.cta_cli.exec_with_output("cta-admin --json ta ls --all")
        self.cta_cli.exec(
            f"cta-admin ta add -v {self.vid} --mt LTO9 --ve tempvendor -l {self.ll_name} -t "
            f"{self.tp_name} -f false --purchaseorder temporder -m 'Add temp tape'"
        )
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> bool:
        self.cta_cli.exec(f"cta-admin ta rm -v {self.vid}")
        assert self.ls_before == self.cta_cli.exec_with_output("cta-admin --json ta ls --all")
        return False
