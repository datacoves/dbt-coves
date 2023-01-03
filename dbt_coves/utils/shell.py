from pathlib import Path
from subprocess import PIPE, CompletedProcess, Popen
from subprocess import run as shell_run
from typing import List, Optional, Union


def run(cmd, cwd: Optional[Union[str, Path]] = None, write_to_stdout: bool = True) -> Popen:
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
