from pathlib import Path
from subprocess import PIPE, CompletedProcess, Popen
from subprocess import run as shell_run
from typing import List, Optional, Sequence, Union


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


def get_flags(flags: Optional[Sequence[str]] = None) -> List[str]:
    if flags:
        return [flag.replace("+", "-") for flag in flags if flag]
    else:
        return []


def prepare_cmd(task, command: Sequence[str]) -> List[str]:
    command = ["dbt", *command]
    if task.config.args.PROFILES_DIR:
        command.extend(["--profiles-dir", task.config.args.PROFILES_DIR])
    if task.config.args.project_dir:
        command.extend(["--project-dir", task.config.args.project_dir])
    cmd_flags = get_flags(task.get_config_value("cmd_flags"))
    command.extend(cmd_flags)
    return command
