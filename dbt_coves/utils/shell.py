from typing import Optional, Union, List
from pathlib import Path

import subprocess
from subprocess import PIPE, Popen, CompletedProcess
from subprocess import run as shell_run


def run(
    cmd, cwd: Optional[Union[str, Path]] = None, write_to_stdout: bool = True
) -> Popen:
    with Popen(cmd, stdout=PIPE, bufsize=1, universal_newlines=True, cwd=cwd) as proc:
        if write_to_stdout:
            for line in proc.stdout:
                print(line, end="")  # process line here
    return proc


def run_and_capture(args_list: List) -> CompletedProcess:
    return shell_run(args_list, capture_output=True, text=True)


def run_and_capture_shell(args_list):
    return shell_run(args_list, shell=True)


def run_and_capture_cwd(args_list, cwd) -> CompletedProcess:
    return shell_run(args_list, cwd=cwd)


def run_dbt_ls(bash_cmd: str, cwd: Optional[Union[str, Path]] = None) -> List[str]:
    process = subprocess.run(
        bash_cmd.split(),
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=True,
    )
    stdout = process.stdout.decode().strip()
    full_stdout = stdout.split("\n") if "\n" in stdout else [stdout]
    return [line for line in full_stdout if "source:" in line]
