import subprocess
from typing import Optional


def run_shell_command(command: str, env: Optional[dict] = None, timeout: Optional[int] = None):
    """Run a shell command and return CompletedProcess.

    - `command` can be a single string (runs in shell=True) or a list for exec form.
    - Captures stdout and stderr. Caller should inspect `returncode` and raise as needed.
    """
    if isinstance(command, str):
        # run via shell to support legacy scripts; prefer list form when possible
        proc = subprocess.run(command, shell=True, capture_output=True, env=env, timeout=timeout)
    else:
        proc = subprocess.run(command, shell=False, capture_output=True, env=env, timeout=timeout)
    return proc
