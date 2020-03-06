"""Provides a Connection class with a tables property providing metadata for datasets
where the user credentials has share permissions or is marked as the owner. Also provides
query method used to pull/export data from DOMO.
"""
__version__ = '0.1.0'

# Standard library
import dataclasses as dc
import typing as tp

# 3rd party
import requests

@dc.dataclass
class Connection:
    """Interface for the domo_query package. The Connection object is lazy meaning
    that properties are populated on their first use.

    This library requires a 'Client' to be created at developer.domo.com.
    1. Go to developer.domo.com.
    2. Sign into your domain.
    3. Under My Account select New Client.
    4. Scope must be data.

    The 'Client ID' and 'Secret' are required parameters when instantiating the Connection class.
    Once the class is instantiated, properties are calculated as they are used.

    conn = Connection(YOUR_CLIENT_ID, YOUR_SECRET)
    conn.tables returns a list of dict objects where each dict contains an 'id' and 'name' key.
    The 'id' or 'name' can be used to run queries.

    The sql paramter of the 'query' method will always be selected from the 'table' table.

    For example, 'select * from table' is the default query provided if sql=''.
    Results can be limited by passing 'select * from table limit 10'.
    Columns and rows can be specified using normal sql select and where clauses.
    """
    client_id: str
    secret: str
    auth_url: str=dc.field(init=False, default="https://api.domo.com/oauth/token?grant_type=client_credentials&scope=data")
    query_url: str=dc.field(init=False, default="https://api.domo.com/v1/datasets")
    _login: dict=dc.field(init=False, default_factory=dict)
    _tables: tp.List[tp.Dict]=dc.field(init=False, default_factory=list)
    _last_id_or_name: str=dc.field(init=False, default="")

    @property
    def login(self) -> tp.Dict:
        """Returns the Authorization header specified in the DOMO API."""
        if self._login:
            return self._login

        auth = requests.auth.HTTPBasicAuth(self.client_id, self.secret)
        # authenticate
        response = requests.get(self.auth_url, auth=auth)

        # get token and return basic header
        token = response.json()["access_token"]
        header = dict(Authorization = f"bearer {token}")
        self._login = header
        return self._login

    @property
    def tables(self) -> tp.List[tp.Dict]:
        """Return a list of datasets in the domain. Each item in the list contains
        metadata about the dataset such as name, owner, rows, columns, etc. The login
        used to create the 'Client ID' and 'Secret' must have share permissions or be the owner
        of the datasets returned.
        """
        if self._tables:
            return self._tables

        header = self.login # authenticate
        limit = 50 # barch size to fetch each loop
        offset = 0 # set state variable
        datasets = [] # holds metadata for datasets

        # requests datasets in groups of limit
        while True:
            # set url
            url = f"{self.query_url}?offset={offset}&limit={limit}"

            # get next group of datasets
            chunk = requests.get(url, headers=header).json()
            if not chunk: break

            # append chunk to master list
            datasets.extend(chunk)

            # increment state
            offset += limit

        self._tables = datasets
        return self._tables

    def find_table(self, id_or_name: str) -> str:
        """Takes the name or id of the DOMO dataset and returns metadata for the
        matching dataset. This method is used to obtain the id_or_name parameter in the
        'query' method.
        """
        for table in self.tables:
            if table["name"] == id_or_name:
                self._last_id_or_name = table
                return self._last_id_or_name
            elif table["id"] == id_or_name:
                self._last_id_or_name = table
                return self._last_id_or_name

    def query(self, sql: str="", id_or_name: str="") -> tp.List[dict]:
        """Returns the results of a query on a DOMO dataset. The results are
        formatted as a list of dict objects where each record is a dict where
        keys are columns.

        1. If id_or_name is not provided, the last id_or_name value is used.
        2. If the query method has never been useed, the id_or_name must be passed.
           This value can be
           obtained using the find_table method.
        3. If 'sql' is not provided the default will be 'select * from table'.
           All 'sql' statements must select from the 'table' table.
        """
        # authenticate
        header = self.login

        # add json type
        header['Accept'] = 'application/json'

        # create params
        sql = sql or "select * from table"

        if id_or_name and self._last_id_or_name:
            id = self._last_id_or_name["id"]
        elif not id_or_name and not self._last_id_or_name:
            raise ValueError("Must provide Dataset ID or Name.")
        else:
            self.find_table(id_or_name)
            id = self._last_id_or_name["id"]

        query = {"sql":sql}
        url = f"{self.query_url}/query/execute/{id}?includeHeaders=true"

        # get query results
        response = requests.post(
            url,
            headers=header,
            json=query
        ).json()

        # format result as a list of dicts
        columns = response["columns"]
        rows = response["rows"]

        data = [
            dict(zip(columns, row))
            for row in rows
        ]
        return data