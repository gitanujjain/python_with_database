import pyodbc


class AzureSqlConectionManger:
    """
    this class for Azure Sql the connection and detail of databse.
    """

    def __init__(self, connection_info) -> None:
        print("conn_info=====",connection_info)
        """
        initalize value connection details and create the connection with mssql database
        """
        self.host_address = connection_info.get('host_address', '')
        self.port_number = connection_info.get('port_number', '1433')
        self.database_name = connection_info.get('database_name', '')
        self.username=connection_info.get('username','')
        self.schema_name = connection_info.get('schema_name', '')
        self.password = connection_info.get('password', '')
        self.source_schema = connection_info.get('source_schema', None)
        err, self.connection = self.create_connection()
        if self.connection is None:
            print("Connection not created ")
            raise Exception(err)

    def create_connection(self):
        """ Connect to the oracle Server database server """
        con = None
        err = None
        try:
            # Establish the connection
            server = self.host_address
            database = self.database_name
            username = self.username
            password = self.password
            driver= '{ODBC Driver 17 for SQL Server}'
            con = pyodbc.connect('DRIVER=' + driver + ';SERVER=' + server + ';PORT=1433;DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
            print(con)
        except Exception as err:
            print(err)
            return err, con
        return err, con

    def connection_close(self):
        self.connection.close()

    def metadata_details(self):
        sql_table = f"SELECT TABLE_NAME FROM information_schema.tables where TABLE_CATALOG ='{self.database_name.upper()}' "
        meta_data_details = []
        with self.connection.cursor() as cursor:
            cursor.execute(sql_table)
            for result in cursor.fetchall():
                temp = {'table_name': result[0], 'column_detail': self.fetch_table_details(result[0])}
                meta_data_details.append(temp)
        return meta_data_details

    def fetch_table_details(self, table_name):
        temp_col = []
        with self.connection.cursor() as cursor_tbl:
            sql_col_detail = f'''
                           SELECT 
                               COLUMN_NAME, DATA_TYPE, IS_NULLABLE, column_default,
                               CASE 
                                   WHEN DATA_TYPE IN ('decimal', 'numeric') THEN NUMERIC_PRECISION
                                   WHEN DATA_TYPE IN ('datetime2', 'time', 'datetimeoffset') THEN DATETIME_PRECISION
                                   ELSE CHARACTER_MAXIMUM_LENGTH
                               END AS Length,
                               NUMERIC_PRECISION AS Precision,
                               NUMERIC_SCALE AS Scale

                           FROM INFORMATION_SCHEMA.COLUMNS
                           WHERE TABLE_NAME = '{table_name}' '''
            cursor_tbl.execute(sql_col_detail)
            column_details_list = cursor_tbl.fetchall()
            for column_details in column_details_list:
                temp_col.append({"column_name": column_details[0], "DATA_TYPE": column_details[1],
                                 "is_nullable": column_details[2], "COLUMN_DEFAULT": column_details[3],
                                 "DATA_LENGTH": column_details[4], "DATA_PRECISION": column_details[5],
                                 "DATA_SCALE": column_details[6]})
        return temp_col

    def find_table(self, table_name):
        sql_table = f"SELECT count(1) FROM information_schema.tables where TABLE_CATALOG ='{self.database_name.upper()}' and TABLE_NAME='{table_name}'"

        with self.connection.cursor() as cursor:
            cursor.execute(sql_table)
            data = cursor.fetchone()
            print(data)
            table_count = data[0]
        if table_count == 0:
            return True
        else:
            return False

    def create_table(self, sql_query, table_create=False):
        result = None
        with self.connection.cursor() as cursor:
            cursor.execute(sql_query)
            if table_create:
                self.connection.commit()
            else:
                data = cursor.fetchone()
                result = data[0]
        return result

    def table_space(self, table_name):
        sql_table = f"SELECT count(1) FROM information_schema.tables where TABLE_CATALOG ='{self.database_name.upper()}' and TABLE_NAME='{table_name}'"

        with self.connection.cursor() as cursor:
            cursor.execute(sql_table)
            data = cursor.fetchone()
            print(data)
            table_count = data[0]
        if table_count == 0:
            return True
        else:
            return False

