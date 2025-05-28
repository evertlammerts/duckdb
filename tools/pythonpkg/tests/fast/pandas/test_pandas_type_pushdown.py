import duckdb
import pandas as pd
import pytest


class TestPandasTypePushDown(object):

    def test_regular_dict_to_map_conversion(self):
        con = duckdb.connect()
        con.execute("CREATE TABLE my_table (id INT, data MAP(VARCHAR, INTEGER))")
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'data': [{'a': 1}, {'b': 2}, {'c': 3}]
        })
        con.execute("INSERT INTO my_table SELECT * FROM df")
        result = con.execute("SELECT * FROM my_table ORDER BY id").df()
        print(con.sql("SELECT * FROM my_table ORDER BY id").explain())
        assert len(result) == 3
        assert result['data'][0] == {'a': 1}
        assert result['data'][1] == {'b': 2}
        assert result['data'][2] == {'c': 3}

    def test_fetch_and_insert(self):
        con = duckdb.connect()
        con.execute("CREATE TABLE my_table (data MAP(VARCHAR, INTEGER))")
        con.execute("INSERT INTO my_table VALUES(MAP {'duckdb': 130})")
        df = con.execute('SELECT * FROM my_table').df()
        con.execute("INSERT INTO my_table SELECT * FROM df")
        result = con.execute('SELECT * FROM my_table').df()
        assert len(result) == 2
        assert result['data'][0] == {'duckdb': 130}
        assert result['data'][1] == result['data'][0]

    def test_empty(self):
        con = duckdb.connect()
        con.execute("CREATE TABLE my_table (data MAP(VARCHAR, INTEGER))")
        df = pd.DataFrame({'data': []})
        con.execute("INSERT INTO my_table SELECT * FROM df")
        result = con.execute('SELECT * FROM my_table').df()
        assert len(result) == 0

    def test_pandas_dict_format(self):
        con = duckdb.connect()
        con.execute("CREATE TABLE my_table (data MAP(VARCHAR[], INTEGER))")
        df = pd.DataFrame({'data': [{'key': [['list'], ['lost']], 'value': [4, 2]}]})
        con.execute("INSERT INTO my_table SELECT * FROM df")
        result = con.execute('SELECT * FROM my_table').df()
        assert len(result) == 1
        assert result['data'][0] == {'key': [['list'], ['lost']], 'value': [4, 2]}

    def test_type_upgrade(self):
        con = duckdb.connect()
        con.execute("CREATE TABLE my_table (data MAP(FLOAT, VARCHAR))")
        df = pd.DataFrame({'data': [{1: 'int', 2.5: 'float', '3': 'string'}]})
        con.execute("INSERT INTO my_table SELECT * FROM df")
        result = con.execute('SELECT * FROM my_table').df()
        assert len(result) == 1
        assert result['data'][0] == {1.0: 'int', 2.5: 'float', 3.0: 'string'}

    def test_fetch_and_insert_list_keys(self):
        con = duckdb.connect()
        con.execute("CREATE TABLE my_table (data MAP(INTEGER[], VARCHAR))")
        con.execute("INSERT INTO my_table VALUES(MAP {[1,2,3]: 'array'})")
        df = con.execute('SELECT * FROM my_table').df()
        con.execute("INSERT INTO my_table SELECT * FROM df")
        result = con.execute('SELECT * FROM my_table').df()
        assert len(result) == 2
        assert result['data'][0] == {'key': [[1, 2, 3]], 'value': ['array']}
        assert result['data'][1] == result['data'][0]

    def test_fetch_and_insert_struct_keys(self):
        con = duckdb.connect()
        con.execute("CREATE TABLE my_table (data MAP(STRUCT(bank VARCHAR, pin INTEGER), VARCHAR))")
        con.execute("""
                    INSERT INTO my_table VALUES(MAP {
                      {'bank': 'abn', 'pin': 1111}: 'no money',
                      {'bank': 'ing', 'pin': 1234}: 'empty'
                    })
        """)
        df = con.execute('SELECT * FROM my_table').df()
        con.execute("INSERT INTO my_table SELECT * FROM df")
        result = con.execute('SELECT * FROM my_table').df()
        assert len(result) == 2
        assert result['data'][0] == {'key': [{'bank': 'abn', 'pin': 1111}, {'bank': 'ing', 'pin': 1234}], 'value': ['no money', 'empty']}
        assert result['data'][1] == result['data'][0]

    def test_cast_fails(self):
        df = pd.DataFrame({'data': [{'duckdb': 130}]})
        with pytest.raises(duckdb.ConversionException, match=r"Unimplemented type for cast \(STRUCT\(.*\) -> MAP\(.*\)\)"):
            # doesn't work: the binder visits the tableref before the select so lacks a pushed down
            # type and infers a STRUCT
            duckdb.sql("SELECT data::MAP(VARCHAR, INTEGER) FROM df").df()
