# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from collections import deque

import sqlparse


def walk_tokens(token):
    queue = deque([token])
    while queue:
        token = queue.popleft()
        if isinstance(token, sqlparse.sql.TokenList):
            queue.extend(token)
        yield token


def add_quotes_to_name_token(statement):
    _statement = sqlparse.parse(statement)[0]

    for token in walk_tokens(_statement):
        if token.ttype == sqlparse.tokens.Name:
            if isinstance(token.parent.parent, sqlparse.sql.Function):
                continue
            token.value = '"{}"'.format(token.value)
    return str(_statement)
