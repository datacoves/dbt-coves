from subprocess import PIPE, Popen, run as shell_run
import subprocess


def run(cmd, cwd=None):
    with Popen(cmd, stdout=PIPE, bufsize=1, universal_newlines=True, cwd=cwd) as p:
        for line in p.stdout:
            print(line, end="")  # process line here
    return p


def run_and_capture(args_list):
    return shell_run(args_list, capture_output=True, text=True)


def run_dbt_ls(bash_cmd, cwd=None):
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
