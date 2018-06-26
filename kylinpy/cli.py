#!/usr/bin/env python

from __future__ import division
from __future__ import absolute_import

import sys
import json
import logging
import click

from kylinpy import Kylinpy


def exception_handler(exception_type, exception, traceback):
    print("%s: %s" % (exception_type.__name__, exception))


sys.excepthook = exception_handler  # noqa


@click.group()
@click.option('-h', '--host', required=True, help='kylin/kap host name')
@click.option('-P', '--port', default=7070, help='kylin/kap port, default: 7070')
@click.option('-u', '--username', required=True, help='kylin/kap username')
@click.option('-p', '--password', required=False, help='kylin/kap password')
@click.option('-s', '--session', required=False, help='session id')
@click.option('--project', required=True, help='kylin/kap project')
@click.option('--prefix', default='/kylin/api', help='kylin/kap RESTful prefix of url, default: /kylin/api')
@click.option('--debug/--no-debug', default=False, help='show debug infomation')
@click.option('--api2/--api1', default=False, help='API version; default is api1; --api1 used by KYLIN; --api2 used by KAP')
@click.pass_context
def main(ctx, host, port, username, password, session, project, prefix, debug, api2):
    if debug:
        logging.basicConfig(level=logging.DEBUG)

    _version = 'v2' if api2 else 'v1'
    ctx.obj = Kylinpy(host, username, port, project, **{
        'version': _version,
        'prefix': prefix,
        'is_debug': debug,
        'password': password,
        'session': session
    })


@main.command(help='list all projects')
@click.pass_context
def projects(ctx):
    print(json.dumps(ctx.obj.projects(), indent=4, sort_keys=True))


@main.command(help='sql query')
@click.option('--sql', required=True, help='ANSI-SQL select statement')
@click.pass_context
def query(ctx, sql):
    print(json.dumps(ctx.obj.query(sql), indent=4, sort_keys=True))


@main.command(help='list all users, need admin role, KAP only')
@click.pass_context
def users(ctx):
    print(json.dumps(ctx.obj.users(), indent=4, sort_keys=True))


@main.command(help='get user auth info')
@click.pass_context
def auth(ctx):
    print(json.dumps(ctx.obj.authentication(), indent=4, sort_keys=True))

@main.command(help='show cubes')
@click.pass_context
def cubes(ctx):
    print(json.dumps(ctx.obj.cubes(), indent=4, sort_keys=True))

@main.command(help='get sample sql of cube, KAP only')
@click.option('--name', required=True, help='cube name')
@click.pass_context
def cube_sql(ctx, name):
    print(json.dumps(ctx.obj.cube_sql(name), indent=4, sort_keys=True))


@main.command(help='show cube description')
@click.option('--name', required=True, help='cube name')
@click.pass_context
def cube_desc(ctx, name):
    print(json.dumps(ctx.obj.cube_desc(name), indent=4, sort_keys=True))


@main.command(help='list cube names')
@click.pass_context
def cube_names(ctx):
    print(json.dumps(ctx.obj.get_cube_names(), indent=4, sort_keys=True))


@main.command(help='list cube columns')
@click.option('--name', required=True, help='cube name')
@click.pass_context
def cube_columns(ctx, name):
    print(json.dumps(ctx.obj.get_cube_columns(name), indent=4, sort_keys=True))


@main.command(help='list cube metrics')
@click.option('--name', required=True, help='cube name')
@click.pass_context
def cube_measures(ctx, name):
    print(json.dumps(ctx.obj.get_cube_measures(name), indent=4, sort_keys=True))


@main.command(help='list all table names')
@click.pass_context
def table_names(ctx):
    print(json.dumps(ctx.obj.get_table_names(), indent=4, sort_keys=True))


@main.command(help='list table columns')
@click.option('--name', required=True, help='cube name')
@click.pass_context
def table_columns(ctx, name):
    print(json.dumps(ctx.obj.get_table_columns(name), indent=4, sort_keys=True))


@main.command(help='show model description')
@click.option('--name', required=True, help='model name')
@click.pass_context
def model_desc(ctx, name):
    print(json.dumps(ctx.obj.model_desc(name), indent=4, sort_keys=True))


@main.command(help='list models')
@click.pass_context
def model_list(ctx):
    print(json.dumps(ctx.obj.list_models(), indent=4, sort_keys=True))