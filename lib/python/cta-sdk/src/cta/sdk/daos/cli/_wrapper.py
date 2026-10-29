# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import json
import os
import subprocess
from logging import getLogger
from typing import Any, Generator, List, Optional, Union

from cta.sdk.exceptions import BackendError, ResourceAlreadyExistsError, ResourceConflictError, ResourceNotFoundError

LOGGER = getLogger("cta.sdk")
FLAG = object()


class CtaCliWrapper:
    def __init__(
        self,
        exec_path: str = "/usr/bin/cta-admin",
        timeout: int = 60,
        sss_keytab_file: Optional[str] = None,
        cta_conf_file: Optional[str] = None,
    ):
        self.base_command = [exec_path, "--jsonl"] + (["--config", cta_conf_file] if cta_conf_file is not None else [])
        self.timeout = timeout
        self.sss_keytab_file = sss_keytab_file
        LOGGER.debug(
            "CTA Cli Wrapper initialized with command: %s, timeout: %d",
            self.base_command,
            self.timeout,
        )

    def _build_env(self) -> dict[str, str]:
        kt = os.environ.get("CTAXrdSecSSSKT") or self.sss_keytab_file
        extra = {"XrdSecPROTOCOL": "sss", "XrdSecSSSKT": kt} if kt is not None and os.access(kt, os.R_OK) else {}
        return os.environ | extra

    def _run_command(
        self,
        cmd: list[str],
    ) -> subprocess.CompletedProcess[str]:
        LOGGER.debug("Executing shell command: %s", cmd)

        try:
            cmd_call: subprocess.CompletedProcess[str] = subprocess.run(
                cmd,
                encoding="utf-8",
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=self.timeout,
                env=self._build_env(),
            )
        except subprocess.TimeoutExpired as e:
            raise BackendError((e.stderr or b"").decode("utf-8"), 1, cmd) from e

        if cmd_call.returncode != 0:
            raise BackendError(cmd_call.stderr, cmd_call.returncode, cmd_call.args)

        return cmd_call

    def _run_streaming_command(
        self,
        cmd: list[str],
    ) -> Generator[str, None, None]:
        LOGGER.debug("Executing shell command (streaming): %s", cmd)

        with subprocess.Popen(
            cmd,
            encoding="utf-8",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=1,
            env=self._build_env(),
        ) as proc:
            try:
                if proc.stdout is None:
                    raise StopIteration()
                yield from proc.stdout
                _, stderr = proc.communicate(timeout=5)

            except subprocess.TimeoutExpired as e:
                raise BackendError("Process did not terminate properly after streaming output", 1, cmd) from e

            except Exception as e:
                raise BackendError(f"Error while streaming output: {e}", 1, cmd) from e

            else:
                if proc.returncode != 0:
                    raise BackendError(f"{stderr}", proc.returncode, cmd)

    @property
    def cta_version(self) -> dict[str, Any]:
        cmd_call = self._run_command(self.base_command + ["version"])
        return json.loads(cmd_call.stdout)

    def create(
        self,
        resource_type: str,
        resource_id: Union[dict[str, Any], str],
        data: Optional[dict[str, Any]] = None,
    ) -> None:
        payload = [resource_id] if isinstance(resource_id, str) else CtaCliWrapper.build_options(resource_id)
        payload += CtaCliWrapper.build_options(data)
        try:
            self._run_command(self.base_command + [resource_type, "add"] + payload)
        except BackendError as e:
            if e.return_code == 2:
                raise ResourceAlreadyExistsError(resource_type, resource_id) from e
            raise

    def update(self, resource_type: str, resource_id: Union[dict[str, Any], str], data: dict[str, Any]) -> None:
        payload = [resource_id] if isinstance(resource_id, str) else CtaCliWrapper.build_options(resource_id)
        payload += CtaCliWrapper.build_options(data)

        try:
            self._run_command(self.base_command + [resource_type, "ch"] + payload)
        except BackendError as e:
            if e.return_code == 2:
                raise ResourceNotFoundError(resource_type, resource_id) from e
            raise

    def delete(
        self, resource_type: str, resource_id: Union[dict[str, Any], str], data: Optional[dict[str, Any]] = None
    ) -> None:
        payload = [resource_id] if isinstance(resource_id, str) else CtaCliWrapper.build_options(resource_id)
        payload += CtaCliWrapper.build_options(data)

        try:
            self._run_command(self.base_command + [resource_type, "rm"] + payload)
        except BackendError as e:
            if e.return_code == 2:
                raise ResourceNotFoundError(resource_type, resource_id) from e
            raise

    def get(self, resource_type: str, resource_id: Union[dict[str, Any], str]) -> dict[str, Any]:
        payload = [resource_id] if isinstance(resource_id, str) else CtaCliWrapper.build_options(resource_id)
        try:
            cmd_call = self._run_command(self.base_command + [resource_type, "ls"] + payload)
        except BackendError as e:
            if e.return_code == 2:
                raise ResourceNotFoundError(resource_type, resource_id) from e
            raise
        else:
            if len(lines := cmd_call.stdout.splitlines()) > 1:
                raise ResourceConflictError(resource_type, resource_id, len(lines))
            elif len(lines) == 0:
                raise ResourceNotFoundError(resource_type, resource_id)
            else:
                return json.loads(lines[0])

    def list(self, resource_type: str, filters: Optional[dict[str, Any]] = None) -> list[dict[str, Any]]:
        payload = CtaCliWrapper.build_options(filters)

        cmd_call = self._run_command(self.base_command + [resource_type, "ls"] + payload)
        return [json.loads(line) for line in cmd_call.stdout.splitlines()]

    def iter(
        self, resource_type: str, filters: Optional[dict[str, Any]] = None
    ) -> Generator[dict[str, Any], None, None]:
        payload = CtaCliWrapper.build_options(filters)
        yield from (
            json.loads(line)
            for line in self._run_streaming_command(self.base_command + [resource_type, "ls"] + payload)
        )

    def put_up(
        self, resource_type: str, resource_id: Union[dict[str, Any], str], data: Optional[dict[str, Any]] = None
    ) -> None:
        payload = [resource_id] if isinstance(resource_id, str) else CtaCliWrapper.build_options(resource_id)
        payload += CtaCliWrapper.build_options(data)
        self._run_command(self.base_command + [resource_type, "up"] + payload)

    def put_down(
        self, resource_type: str, resource_id: Union[dict[str, Any], str], data: Optional[dict[str, Any]] = None
    ) -> None:
        payload = [resource_id] if isinstance(resource_id, str) else CtaCliWrapper.build_options(resource_id)
        payload += CtaCliWrapper.build_options(data)
        self._run_command(self.base_command + [resource_type, "down"] + payload)

    def reclaim(
        self, resource_type: str, resource_id: Union[dict[str, Any], str], data: Optional[dict[str, Any]] = None
    ) -> None:
        payload = [resource_id] if isinstance(resource_id, str) else CtaCliWrapper.build_options(resource_id)
        payload += CtaCliWrapper.build_options(data)
        self._run_command(self.base_command + [resource_type, "reclaim"] + payload)

    @staticmethod
    def build_options(data: Optional[dict] = None) -> List[str]:
        if data is None:
            return []

        args = []
        for k, v in data.items():
            if v is FLAG:
                args.append(f"--{k}")
            elif v is not None:
                args.extend([f"--{k}", str(v)])

        return args
