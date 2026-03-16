#!/usr/bin/python3
# /// script
# requires-python = '>=3.10'
# dependencies = [
#   'jeepney'
# ]
# ///
# Author: Kwok "Molten Armor" Guy
import os, os.path, sys, signal, pwd
from jeepney.io.blocking import open_dbus_connection, Proxy
from jeepney import (
    message_bus,
    new_method_return,
    new_error,
    MessageType,
    HeaderFields,
)
import logging
from typing import Any

logging.basicConfig(level=logging.INFO)

SERVICE = "org.freedesktop.systemd1"
INTERFACE = "org.freedesktop.systemd1.Manager"
PATH = "/org/freedesktop/systemd1"


def sigchld_handler(*_):
    while True:
        try:
            pid, _ = os.waitpid(-1, os.WNOHANG)
            if pid == 0:
                break
            logging.info(f"Reaped child: {pid}")

        except ChildProcessError:
            break


def start_transient_unit(properties: dict[str, Any]) -> int:
    # D-Bus variants are translated to Python tuple
    # [0]: Signature
    # [1]: Value
    # [1][0]: See https://www.freedesktop.org/software/systemd/man/latest/org.freedesktop.systemd1.html#:~:text=ExecStartPre%2C%20ExecStart
    # We only need path and argv
    path, argv, _ = properties["ExecStart"][1][0]

    # Prepare for redirection
    stdin, stdout, stderr = (
        (variant := properties.get("StandardInputFileDescriptor")) and variant[1],
        (variant := properties.get("StandardOutputFileDescriptor")) and variant[1],
        (variant := properties.get("StandardErrorFileDescriptor")) and variant[1],
    )

    user = (variant := properties.get("User")) and variant[1]

    workdir = (variant := properties.get("WorkingDirectory")) and variant[1]

    env = (variant := properties.get("Environment")) and variant[1]

    if not os.path.isfile(path):
        raise FileNotFoundError(f"{path} is not a file!")

    if not os.access(path, os.X_OK):
        raise PermissionError(f"{path} is not executable!")

    if user:
        try:
            pwd.getpwnam(user)
        except KeyError:
            raise SystemError(f"User {user} not found")

    if workdir:
        if workdir != "-~" and not os.path.isdir(workdir):
            raise FileNotFoundError(f"{workdir} is not a valid directory!")

    pid = os.fork()

    # Child process
    if pid == 0:
        try:
            os.setsid()

            if env:
                if user:
                    userdb = pwd.getpwnam(user)
                else:
                    userdb = pwd.getpwnam("root")

                os.environ.update(
                    {
                        "USER": userdb.pw_name,
                        "HOME": userdb.pw_dir,
                        "SHELL": userdb.pw_shell,
                    }
                )

                try:
                    with open("/etc/default/locale") as f:
                        os.environ.update(
                            dict(
                                [
                                    e.strip().split("=", 1)
                                    for e in f.readlines()
                                    if e.strip() and not e.startswith("#")
                                ]
                            )
                        )
                except FileNotFoundError:
                    os.environ.update({"LANG": "POSIX"})

                os.environ.update(dict([e.split("=", 1) for e in env]))

            if user:
                userdb = pwd.getpwnam(user)
                os.initgroups(user, userdb.pw_gid)
                os.setgid(userdb.pw_gid)
                os.setuid(userdb.pw_uid)

            if workdir:
                if workdir == "-~" and user:
                    os.chdir(pwd.getpwnam(user).pw_dir)
                else:
                    os.chdir(workdir)

            # Redirect all fd
            if stdin:
                os.dup2(stdin.fileno(), 0)
            if stdout:
                os.dup2(stdout.fileno(), 1)
            if stderr:
                os.dup2(stderr.fileno(), 2)

            for fd in (_ := os.listdir("/proc/self/fd")):
                if fd not in ("0", "1", "2"):
                    try:
                        os.close(int(fd))
                    except OSError:
                        continue

            os.execv(path, argv)

        # If execv fails
        finally:
            sys.exit(1)

    if stdin:
        stdin.close()
    if stdout:
        stdout.close()
    if stderr:
        stderr.close()

    logging.info(f"Forked child: {pid}")

    return pid


def main():
    signal.signal(signal.SIGCHLD, sigchld_handler)

    with open_dbus_connection(bus="SYSTEM", enable_fds=True) as conn:
        logging.info("Connected to system bus.")

        if Proxy(message_bus, conn).RequestName(SERVICE) != (1,):
            logging.error("Failed to request service! Exiting...")
            sys.exit(1)

        logging.info("Successfully requested service name.")

        while True:
            msg = conn.receive()

            if (
                msg.header.message_type == MessageType.method_call
                and msg.header.fields.get(HeaderFields.path) == PATH
                and msg.header.fields.get(HeaderFields.interface) == INTERFACE
                and msg.header.fields.get(HeaderFields.member) == "StartTransientUnit"
            ):
                try:
                    # Reference: https://www.freedesktop.org/software/systemd/man/latest/org.freedesktop.systemd1.html#:~:text=in%20%20s%20name)%3B-,StartTransientUnit
                    # [2]: Properties
                    # For convenience, we convert properties to dict
                    properties = dict(msg.body[2])

                    logging.info(f"Received properties: {properties}")

                    pid = start_transient_unit(properties)

                    # See: https://www.freedesktop.org/software/systemd/man/latest/org.freedesktop.systemd1.html#:~:text=in%20%20s%20name)%3B-,StartTransientUnit
                    conn.send(
                        new_method_return(
                            msg, "o", (f"/org/freedesktop/systemd1/job/{pid}",)
                        )
                    )

                except Exception as e:
                    logging.error(f"Error: {e}")
                    conn.send(
                        new_error(
                            msg, "org.freedesktop.DBus.Error.Failed", "s", (str(e),)
                        )
                    )


if __name__ == "__main__":
    main()
