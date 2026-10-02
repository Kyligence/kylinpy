# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import subprocess
import sys


class TestReservedWords(object):
    def test_importing_dialect_does_not_mutate_shared_base_reserved_words(self):
        """KylinIdentifierPreparer must not leak its reserved-word set onto
        sqlalchemy.sql.compiler.IdentifierPreparer.reserved_words, the shared
        default every other SQLAlchemy dialect without its own reserved_words
        falls back on. Run in a subprocess so the check reflects the state
        right after import, regardless of what else this test session has
        already imported.
        """
        script = (
            "from sqlalchemy.sql import compiler\n"
            "before = set(compiler.IdentifierPreparer.reserved_words)\n"
            "import kylinpy.sqla_dialect\n"
            "after = compiler.IdentifierPreparer.reserved_words\n"
            "assert after == before, "
            "'importing kylinpy.sqla_dialect mutated the shared '"
            "'compiler.IdentifierPreparer.reserved_words default'\n"
            "assert 'name' not in after\n"
            "print('OK')\n"
        )
        result = subprocess.run(
            [sys.executable, "-c", script],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        assert result.returncode == 0, result.stdout.decode()
        assert b"OK" in result.stdout

    def test_kylin_identifier_preparer_keeps_its_own_reserved_words(self):
        from sqlalchemy.sql import compiler

        import kylinpy.sqla_dialect as m

        assert m.KylinIdentifierPreparer.reserved_words is not (
            compiler.IdentifierPreparer.reserved_words
        )
        assert "name" in m.KylinIdentifierPreparer.reserved_words
        assert "__timestamp" in m.KylinIdentifierPreparer.reserved_words
