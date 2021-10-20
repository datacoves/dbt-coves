from subprocess import PIPE, Popen, run as shell_run


def run(cmd, cwd=None):
    with Popen(cmd, stdout=PIPE, bufsize=1, universal_newlines=True, cwd=cwd) as p:
        for line in p.stdout:
            print(line, end="")  # process line here
    return p


def run_and_capture(args_list):
    return shell_run(args_list, capture_output=True, text=True)
