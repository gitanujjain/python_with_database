import psycopg2
from string import Template
import os
from tantor.logs.t_logging  import logger 

class PostgresConectionManger:
    """
    this class for postgres database.
    """

    def __init__(self, connection_info) -> None:
        """
        initalize value connection details and create the connection with oracle database
        """
        self.host_address = connection_info.get('host_address', '')
        self.port_number = connection_info.get('port_number', '5432')
        self.database_name = connection_info.get('database_name', '')
        self.user_name= connection_info.get('username','')
        self.schema_name = connection_info.get('schema_name', 'public')
        self.password = connection_info.get('password', '')
        self.source_schema = connection_info.get('source_schema', None)
        err, self.connection = self.create_connection()
        if self.connection is None:
            raise Exception(err)


    def create_connection(self):
        """ Connect to the oracle Server database server """
        con = None
        err = None
        try:
            params = {
                "host": self.host_address,
                "database": self.database_name,
                "user": self.user_name,
                "password": self.password,
                "port": self.port_number,  # Default is 5432 for Postgres SQL
                
            }
            con = psycopg2.connect(**params)
        except Exception as err:
            logger.error(f"can not connect to postgres beacuse of {err}")
            return err, con
        return err, con

    def metadata_details(self, table_name):
        meta_data_details = []
        if table_name is None:
            sql_table = f"SELECT table_name FROM information_schema.tables where TABLE_CATALOG ='{self.database_name.lower()}' and table_schema = 'public' and table_type = 'BASE TABLE'"
            logger.info(f'sql query for List of Table: {sql_table}')
            with self.connection.cursor() as cursor:
                cursor.execute(sql_table)
                for result in cursor.fetchall():
                    temp = {'table_schema': 'public', 
                            'table_name': result[0], 
                            'column_detail':self.fetch_table_details(result[0]),
                            'constraint_details':{},
                            'constraint_details':{}, 
                            'index_details':{}, 
                            'partition_json':{}}
                    meta_data_details.append(temp)
        else:
            logger.info(f"fetch the table info for table name is {table_name}")
            temp = { 'table_schema': 'public', 
                    'table_name': table_name, 
                    'column_detail':self.fetch_table_details(table_name),
                    'constraint_details':{},
                    'constraint_details':{},
                    'index_details':{}, 
                    'partition_json':{}}
            meta_data_details.append(temp)
        return meta_data_details

    def fetch_table_details(self, table_name):
        temp_col = []
        with self.connection.cursor() as cursor_tbl:
            with open('/opt/airflow/dags/tantor/metadata_detail/postgres_sql', 'r') as file:
                temp_sql = file.read()
            template = Template(temp_sql)
            sql_col_detail = template.safe_substitute({'schema_name': self.schema_name, 'table_name': table_name})
            cursor_tbl.execute(sql_col_detail)
            column_details_list = cursor_tbl.fetchall()
            for column_details in column_details_list:
                column_details = ['null' if each is None else each for each in column_details]
                temp_col.append({"column_name": column_details[1], "DATA_TYPE": column_details[2],
                                 "is_nullable": column_details[3], "COLUMN_DEFAULT": column_details[4],
                                 "DATA_LENGTH": column_details[5], "DATA_PRECISION": column_details[6],
                                 "DATA_SCALE": column_details[7], "COLUMN_KEY": column_details[8]})

        return temp_col

    def table_count(self, table_name, where_clause=None):
        try:
            records_count = 0
            if table_name is not None:
                sql = f"select /*+ parallel(16)*/ count(1) from  {table_name}"
                if where_clause is not None:
                    sql = sql + f"\n {where_clause}"
            with self.connection.cursor() as cursor:
                cursor.execute(sql)
                data = cursor.fetchone()
                records_count = data[0]
            return records_count
        except Exception as err:
            raise err

    def find_table(self, table_name):
        """it check the table is exits or Not 
        Parameters
        ----------
        table_name : str
            it contain the name Of Table Name 

        Returns
        -------
        result :  bool
            it return the true/false based on table is exits or not. 
        """
        sql = f"SELECT count(1) FROM information_schema.tables where table_name = '{table_name}'"
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
            data = cursor.fetchone()
            table_count = data[0]
        if table_count == 0:
            return True
        else:
            return False

    def create_table(self, sql_query, table_create=False):
        result = None
        print(sql_query)
        with self.connection.cursor() as cursor:
            cursor.execute(sql_query)
            if table_create:
                self.connection.commit()
            else:
                data = cursor.fetchone()
                result = data[0]
        return result

    def detete_table(self, sql_query):
        result = None
        print(sql_query)
        with self.connection.cursor() as cursor:
            cursor.execute(sql_query)
            self.connection.commit()
        return result
    
    def connection_close(self):
        self.connection.close()

    def find_min_max_value(self, table_name, column_name):
        sql_query=f'SELECT min({column_name}) AS min_value, max({column_name}) AS max_value FROM {table_name}'
        result={}
        with self.connection.cursor() as cursor:
            cursor.execute(sql_query)
            temp_result = cursor.fetchall()
            for each in temp_result:
                temp = ['null' if e is None else e for e in each ]
                result={"min_value": f'{temp[0]}', "max_value": f'{temp[1]}'}
        return result
    
    
    def table_space(self, table_name):
        """it used to check the table space in Kbs 
        Parameters
        ----------
        table_name : str
            it contain the name Of Table Name 

        Returns
        -------
        result :  integer
            it return the integer value related to table space in kbs. 
        """
        table_space_kb=0
        sql_query = f"SELECT pg_size_pretty ( pg_total_relation_size ('{table_name}') ) size"
        print("----sql_query-------", sql_query)
        with self.connection.cursor() as cursor:
            cursor.execute(sql_query)
            data = cursor.fetchone()
            table_space_kb = data[0]
        print("table space -------", table_space_kb)    
        return table_space_kb