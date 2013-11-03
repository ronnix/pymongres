# coding: utf-8

# Copyright 2013 Ronan Amicel
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import absolute_import

from psycopg2 import connect
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def setup_package():
    _run_postgres_cmd('DROP DATABASE IF EXISTS pymongres_test')
    _run_postgres_cmd('CREATE DATABASE pymongres_test')


def teardown_package():
    _run_postgres_cmd('DROP DATABASE pymongres_test')


def _run_postgres_cmd(command):
    with connect(database='postgres') as connection:
        connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        with connection.cursor() as cursor:
            cursor.execute(command)
