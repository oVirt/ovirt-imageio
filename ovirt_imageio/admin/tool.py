# ovirt-imageio
# Copyright (C) 2021 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

"""
Manage ovirt-imageio service
"""

import argparse
import json
import sys

from ovirt_imageio import admin


def main():
    parser = argparse.ArgumentParser(
        description="Control the ovirt-imageio service")

    commands = parser.add_subparsers(title="commands")

    add_cmd = add_command(
        commands,
        name="add-ticket",
        help="Add a ticket.",
        command=add_ticket)

    add_cmd.add_argument(
        "filename",
        help="Ticket filename.")

    show_cmd = add_command(
        commands,
        name="show-ticket",
        help="Show a ticket status.",
        command=show_ticket)

    show_cmd.add_argument(
        "ticket_id",
        help="Ticket id.")

    mod_cmd = add_command(
        commands,
        name="mod-ticket",
        help="Modify a ticket.",
        command=mod_ticket)

    mod_cmd.add_argument(
        "ticket_id",
        help="Ticket id.")

    mod_cmd.add_argument(
        "--timeout",
        type=int,
        help="New timeout in seconds.")

    del_cmd = add_command(
        commands,
        name="del-ticket",
        help="Delete a ticket.",
        command=del_ticket)

    del_cmd.add_argument(
        "ticket_id",
        help="Ticket id.")

    add_command(
        commands,
        name="start-profile",
        help="Start server profiling",
        command=start_profile)

    add_command(
        commands,
        name="stop-profile",
        help="Stop server profiling",
        command=stop_profile)

    args = parser.parse_args()
    try:
        args.command(args)
    except admin.Error as e:
        # Expected errror, log a clean error.
        sys.stderr.write(f"ovirt-imageioctl: {e}\n")
        sys.exit(1)


def add_command(commands, name, help, command):
    cmd = commands.add_parser(name, help=help)
    cmd.set_defaults(command=command)
    cmd.add_argument(
        "-c", "--conf-dir",
        default=admin.DEFAULT_CONF_DIR,
        help=f"Configuration directory (default {admin.DEFAULT_CONF_DIR}).")
    return cmd


def add_ticket(args):
    with open(args.filename) as f:
        data = f.read()
    ticket = json.loads(data)
    cfg = admin.load_config(args.conf_dir)
    with admin.Client(cfg) as c:
        c.add_ticket(ticket)


def show_ticket(args):
    cfg = admin.load_config(args.conf_dir)
    with admin.Client(cfg) as c:
        info = c.get_ticket(args.ticket_id)
    print(json.dumps(info, indent=2))


def mod_ticket(args):
    cfg = admin.load_config(args.conf_dir)
    with admin.Client(cfg) as c:
        c.mod_ticket(args.ticket_id, {"timeout": args.timeout})


def del_ticket(args):
    cfg = admin.load_config(args.conf_dir)
    with admin.Client(cfg) as c:
        c.del_ticket(args.ticket_id)


def start_profile(args):
    cfg = admin.load_config(args.conf_dir)
    with admin.Client(cfg) as c:
        c.start_profile()


def stop_profile(args):
    cfg = admin.load_config(args.conf_dir)
    with admin.Client(cfg) as c:
        c.stop_profile()
