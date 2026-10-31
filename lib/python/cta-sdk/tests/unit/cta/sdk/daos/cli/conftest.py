from collections.abc import Generator
from unittest.mock import MagicMock, patch

import pytest
from cta.sdk.daos.cli._wrapper import CtaCliWrapper


@pytest.fixture
def mock_wrapper(
    request: pytest.FixtureRequest, raw_data: str
) -> Generator[tuple[CtaCliWrapper, tuple[MagicMock, MagicMock]], None, None]:
    """
    Configure overrides for the CLI wrapper.

    You can override with:
    - stdout: str or slice:
        - slice select raw_data elements to return
        - any other string: used as-is
    - stderr: str (default: "")
    - returncode: int (default: 0).
    """
    stdout = ""
    stderr = ""
    returncode = 0

    if hasattr(request, "param"):
        stdout_param = request.param.get("stdout", "")
        stdout = "\n".join(raw_data.splitlines()[stdout_param]) if isinstance(stdout_param, slice) else stdout_param

        stderr = request.param.get("stderr", "")
        returncode = request.param.get("returncode", 0)

    def fake_run_streaming_command(*args: object, **kwargs: object):
        yield from stdout.splitlines()

    with (
        patch.object(
            CtaCliWrapper,
            "_run_command",
            return_value=MagicMock(args="Nothing for now", stdout=stdout, stderr=stderr, returncode=returncode),
        ) as mock_run,
        patch.object(CtaCliWrapper, "run_streaming_command", side_effect=fake_run_streaming_command) as mock_stream_run,
    ):
        yield CtaCliWrapper("cta-admin"), (mock_run, mock_stream_run)

    # with patch(
    #     "cta.sdk.daos.cli._wrapper.CtaCliWrapper._run_command",
    #     return_value=MagicMock(args="Nothing for now", stdout=stdout, stderr=stderr, returncode=returncode),
    # ) as mock_run, patch(
    #     "cta.sdk.daos.cli._wrapper.CtaCliWrapper._run_command",
    #     return_value=MagicMock(
    #         args="Nothing for now",
    #     ),
    # ) as mock_stream_run:
    #     wrapper = CtaCliWrapper("cta-admin")
    #     yield wrapper, mock_run, mock_stream_run
