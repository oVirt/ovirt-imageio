import subprocess

def to_bool(string):
    first = str(string).lower()[:1]
    if first in ('t', 'y', '1'):
        return True
    elif first in ('f', 'n', '0'):
        return False
    else:
        raise ValueError("Invalid value for boolean: {0}".format(string))


def shell_exec(cmd, stdin=None, exc_on_failure=False):
    """
    Execute a command with the given input and return the result.

    :param cmd: command/argument list
    :param stdin: string of input for command's stdin, or None
    :param exc_on_failure: optional, throw CalledProcessError if returncode
                           is non-zero
    :return: tuple of (returncode, stdout, stderr)
    """
    p = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE if stdin is not None else None,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        close_fds=True)
    (out, err) = p.communicate(input=stdin)

    if (p.returncode != 0 and exc_on_failure):
        raise subprocess.CalledProcessError(
            p.returncode, cmd[0], err
        )
    else:
        return (p.returncode, out, err)
