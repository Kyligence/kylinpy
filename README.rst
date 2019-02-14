.. image:: https://img.shields.io/pypi/v/kylinpy.svg
   :target: https://pypi.python.org/pypi/kylinpy

Apache Kylin Python Client Library
==================================
Apache Kylin Python Client Library is a python-based Apache Kylin client.

Any application that uses SQLAlchemy can now query Apache Kylin with this Apache Kylin dialect installed.


Installation
------------

The easiest way to install Apache Kylin Python Client Library is to use pip::

    pip install --upgrade kylinpy

Alternatiely, you may install this library from local project path,
You are welcomed to also commit to this library::

    git clone https://github.com/Kyligence/kylinpy.git
    pip install -e kylinpy

Apache Kylin dialect for SQLAlchemy
-----------------------------------
Any application that uses SQLAlchemy can now query Apache Kylin with this Apache Kylin dialect installed. It is part of the Apache Kylin Python Client Library, so if you already installed this library in the previous step, you are ready to use. 

You may use below template to build DSN to connect Apache Kylin::

    kylin://<username>:<password>@<hostname>:<port>/<project>

============================= ============================================
DSN Field                         Default Value
============================= ============================================
username
----------------------------- --------------------------------------------
password
----------------------------- --------------------------------------------
hostname
----------------------------- --------------------------------------------
port                               7070
----------------------------- --------------------------------------------
project
============================= ============================================


SQLAlchemy **create_engine** takes an argument **connect_args** which is an additional dictionary that will be passed to connect().


============================= ============================================
key                              Default Value
============================= ============================================
is_ssl                           False
----------------------------- --------------------------------------------
prefix                           kylin/api
----------------------------- --------------------------------------------
timeout(unit: seconds)           30
----------------------------- --------------------------------------------
unverified                       True
----------------------------- --------------------------------------------
version                          v1
----------------------------- --------------------------------------------
is_pushdown                      False
============================= ============================================


From SQLAlchemy access Apache Kylin
--------------------------------------
::

    $ python
    >>> import sqlalchemy as sa
    >>> kylin_engine = sa.create_engine('kylin://ADMIN:KYLIN@sandbox/learn_kylin', connect_args={'is_ssl': True, 'timeout': 60})
    >>> results = kylin_engine.execute('SELECT count(*) FROM KYLIN_SALES')
    >>> [e for e in results]
    [(4953,)]
    >>> kylin_engine.table_names()
    [u'KYLIN_ACCOUNT',
     u'KYLIN_CAL_DT',
     u'KYLIN_CATEGORY_GROUPINGS',
     u'KYLIN_COUNTRY',
     u'KYLIN_SALES',
     u'KYLIN_STREAMING_TABLE']

From Pandas access Apache Kylin
------------------------------------
::

   $ python
    >>> import sqlalchemy as sa
    >>> import pandas as pd
    >>> kylin_engine = sa.create_engine('kylin://ADMIN:KYLIN@sandbox/learn_kylin', connect_args={'is_ssl': True, 'timeout': 60})
    >>> sql = 'select * from kylin_sales limit 10'
    >>> pd.read_sql(sql, kylin_engine)


From Superset access Apache Kylin
-------------------------------------

Now you can configure the DSN in your application to establish the connection with Apache Kylin.

For example, you may install Apache Kylin Python Client Library in your Superset environment and configure connection to Apache Kylin in Superset

.. image:: https://raw.githubusercontent.com/Kyligence/kylinpy/master/docs/picture/superset1.png

then you may be able to query Apache Kylin one table at a time from Superset

.. image:: https://raw.githubusercontent.com/Kyligence/kylinpy/master/docs/picture/superset2.png

you may also be able to query detail data

.. image:: https://raw.githubusercontent.com/Kyligence/kylinpy/master/docs/picture/superset3.png

Alternatively, you may also be able to query multiple tables from Apache Kylin by using SQL Lab in Superset.

.. image:: https://raw.githubusercontent.com/Kyligence/kylinpy/master/docs/picture/superset4.png

