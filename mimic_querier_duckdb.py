#TODO: wrap this in try/except
import duckdb
import sqlglot
import re
from mimic_querier import *

class MIMIC_Querier_DuckDB(MIMIC_Querier):
    def __init__(
        self,
        exclusion_criteria_template_vars={},
        query_args={},
        schema_name='main,mimiciii'
    ):
        """ A class to facilitate repeated Queries to a MIMIC DuckDB database """
        super().__init__(exclusion_criteria_template_vars, query_args, schema_name)
        self.exclusion_criteria_template_vars = {}
        self.query_args  = query_args
        self.schema_name = schema_name
        self.connected   = False
        self.connection, self.cursor = None, None
        self._no_namespace_re = re.compile("SET\s+SEARCH_PATH\s+TO\s+[^;]+;",flags=re.RegexFlag.IGNORECASE)
        self._no_materialized_re = re.compile("(CREATE\s+|DROP\s+)MATERIALIZED(\s+VIEW\s)",flags=re.RegexFlag.IGNORECASE)

    def close(self):
        if not self.connected: return
        self.connection.close()
        self.connected = False

    def connect(self):
        self.close()
        self.connection = duckdb.connect(self.query_args['database'], read_only=True)
        self.connection.execute(f"USE {self.schema_name};")
        self.connected = True

    def query(self, query_string=None, query_file=None, extra_template_vars={}):
        assert query_string is not None or query_file is not None, "Must pass a query!"
        assert query_string is None or query_file is None, "Must only pass one query!"
        self.connect()

        if query_string is None:
            with open(query_file, mode='r') as f: query_string = f.read()
        template_vars = copy.copy(self.exclusion_criteria_template_vars)
        template_vars.update(extra_template_vars)

        query_string = query_string.format(**template_vars)
        
        query_string = re.sub(self._no_namespace_re, "", query_string) 

        try:
            sql_list = sqlglot.transpile(query_string, read="postgres", write="duckdb", pretty=True)
        except Exception as e:
            print(query_string)
            raise e
        query_string = sql_list[0] # should only ever be one query passed to this function

        #print(query_string)
        
        out = self.connection.execute(query_string).df()
        out.columns = map(str.lower, out.columns)
        #print(out)

        self.close()
        return out
    
    def ensure_view(self, query_file, view_name):
        self.close()
        # not readonly so we won't use .connect()
        self.connection = duckdb.connect(self.query_args['database'])
        self.connected = True
        result = self.connection.execute(f"select table_schema from information_schema.tables where table_name = '{view_name}'").fetchall()
        if len(result) > 0 and result[0][0] == self.schema_name:
            return
        #else...        
        print(f"View {view_name} not found--creating...")
        self.connection.execute(f"USE {self.schema_name};")
        with open(query_file, mode='r') as f: query_string = f.read()
        query_string = re.sub(self._no_namespace_re, "", query_string)
        query_string = re.sub(self._no_materialized_re, r'\g<1>\g<2>', query_string)
        try:
            sql_list = sqlglot.transpile(query_string, read="postgres", write="duckdb", pretty=True)
        except Exception as e:
            print(query_string)
            raise e
        for sql in sql_list:
            self.connection.execute(query_string)
        self.close()






