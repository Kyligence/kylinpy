#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import click

from .kylinpy import dsn_proxy

# click.disable_unicode_literals_warning = True
#
#
# def exception_handler(exception_type, exception, traceback):
#     print('%s: %s' % (exception_type.__name__, exception))
#
#
# sys.excepthook = exception_handler  # noqa


@click.group()
@click.option('-d', '--dsn', required=True, help='Kylin DSN')
@click.option('--debug/--no-debug', default=False, help='Show debug infomation')
@click.pass_context
def main(ctx, dsn, debug):
    # if debug:
    #     logging.basicConfig(level=logging.DEBUG)
    ctx.obj = dsn_proxy(dsn)


@main.command(help='Kylin query')
@click.option('-s', '--sql', required=True, help='ANSI-SQL select statement')
@click.pass_context
def query(ctx, sql):
    print(ctx.obj.query(sql).to_object)


@main.command(help='Get cube_desc')
@click.option('-c', '--cube', required=True, help='ANSI-SQL select statement')
@click.pass_context
def cube_desc(ctx, cube):
    cube = ctx.obj.get_datasource(cube)
    print(cube.cube_desc)
