import domo_query as dq
from configparser import ConfigParser

def test_version():
    assert dq.__version__ == '0.1.0'

def test_config():
    config = ConfigParser()
    config.read("tests/config.ini")
    assert "domo.com auth" in config
    auth = config["domo.com auth"]
    assert "client_id" in auth
    assert "secret" in auth
    return auth

def test_login():
    auth = test_config()
    conn = dq.Connection(auth["client_id"], auth["secret"])
    login = conn.login
    assert isinstance(login, dict)
    assert "Authorization" in login
    return conn

def test_tables():
    conn = test_login()
    tables = conn.tables
    first_table = tables[0]
    assert "id" in first_table
    assert "name" in first_table
    assert "rows" in first_table
    assert "columns" in first_table
    return conn

def test_find_table():
    conn = test_tables()
    id = conn.tables[0]["id"]
    name = conn.tables[0]["name"]
    assert conn.find_table(id) == conn._last_id_or_name
    assert conn.find_table(name) == conn._last_id_or_name
    return conn

def test_query():
    conn = test_find_table()
    for table in conn.tables:
        if table["rows"] < 100:
            selected = table
            break
    id_provided = conn.query(id_or_name=selected["id"])
    assert isinstance(id_provided, list)
    assert isinstance(id_provided[0], dict)
    rows = min(selected["rows"] - 1, 10)
    sql = f"select * from table limit {rows}"
    sql_provided = conn.query(sql)
    assert len(sql_provided) == rows