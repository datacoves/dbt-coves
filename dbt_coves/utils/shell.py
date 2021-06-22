from subprocess import PIPE, CalledProcessError, Popen


def execute(cmd):
    with Popen(cmd, stdout=PIPE, bufsize=1, universal_newlines=True) as p:
        for line in p.stdout:
            print(line, end="")  # process line here
    return p
