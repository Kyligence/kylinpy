#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import json
import logging

import click

from .kylinpy import dsn_proxy


@click.group()
@click.option('-d', '--dsn', required=True, help='Kylin DSN')
@click.option('-a', '--args', help='connect arguments, JSON string')
@click.option('--debug/--no-debug', default=True, help='Show debug infomation')
@click.pass_context
def main(ctx, dsn, args, debug):
    if debug:
        logging.basicConfig(level=logging.DEBUG)
    args = json.loads(args) if args else {}
    ctx.obj = dsn_proxy(dsn, args)


@main.command(help='Kylin query')
@click.option('-s', '--sql', required=True, help='ANSI-SQL select statement')
@click.pass_context
def query(ctx, sql):
    print(ctx.obj.query(sql).to_object)


@main.command(help='dumps cube_desc/model_desc/tables_and_columns')
@click.option('-c', '--cube', required=True, help='ANSI-SQL select statement')
@click.pass_context
def cube_dumps(ctx, cube):
    cube = ctx.obj.get_datasource(cube)
    print(cube.cube_desc)
    print()
    print(cube.model_desc)
    print()
    print(cube.tables_and_columns)
