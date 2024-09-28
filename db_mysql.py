import mysql.connector


class MySqlConectionManger:
    """
    this class for MySQL the connection and detail of database.
    """
    def __init__(self, connection_info):
        """
        initialize value connection details and create the connection with mssql database
        """
        self.host_address = connection_info.get('host_address', '')
        self.port_number = connection_info.get('port_number', '')
        self.database_name = connection_info.get('database_name', '')
        self.schema_name = connection_info.get('schema_name', '')
        self.username = connection_info.get('username', '')
        self.password = connection_info.get('password', '')
        self.source_schema = connection_info.get('source_schema', None)
        err, self.connection = self.create_connection()
        if self.connection is None:
            print("Connection not created ")
            raise Exception(err)
        else:
            print("Conenction had been created successfully")
            
    def create_connection(self):
        """ Connect to the oracle Server database server """
        con = None
        err = None
        try:
            params = {
                'host': self.host_address,
                'user': self.username,
                'password': self.password,
                'database': self.database_name,
                'port': self.port_number,
                'raise_on_warnings': True
            }
            con = mysql.connector.connect(**params)
        except Exception as err:
            return err, con
        return err, con

    def connection_close(self):
        self.connection.close()
        print("Connection has been successfully closed ")

    def metadata_details(self, table_name=None):
        meta_data_details = []
        if table_name is None:
            sql_table = f"SELECT TABLE_NAME FROM information_schema.tables where TABLE_SCHEMA ='{self.database_name}'"
            with self.connection.cursor() as cursor:
                cursor.execute(sql_table)
                for result in cursor.fetchall():
                    #print("result ---------", result)
                    temp = {'table_schema': self.database_name,'table_name': result[0], 'column_detail': self.fetch_table_details(result[0]),  'constraint_details':{}, 'index_details':{}, 'partition_json':{}}
                    meta_data_details.append(temp)
        else:
            temp = {'table_schema': self.database_name,'table_name': table_name, 'column_detail': self.fetch_table_details(table_name),'constraint_details':{}, 'index_details':{}, 'partition_json':{}}
            meta_data_details.append(temp)

        return meta_data_details

    def fetch_table_details(self, table_name):
        temp_col = []
        with self.connection.cursor() as cursor_tbl:
            sql_col_detail = f"SELECT column_name, data_type,is_nullable,COLUMN_DEFAULT, character_maximum_length, numeric_precision, numeric_scale, COLUMN_KEY FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '{table_name}'"
            #print(sql_col_detail)
            cursor_tbl.execute(sql_col_detail)
            column_details_list = cursor_tbl.fetchall()
            for column_details in column_details_list:
                column_details = ['null' if each is None else each for each in column_details]
                temp_col.append({"column_name": column_details[0], "DATA_TYPE": column_details[1],
                                 "is_nullable": column_details[2], "COLUMN_DEFAULT": column_details[3],
                                 "DATA_LENGTH": column_details[4], "DATA_PRECISION": column_details[5],
                                 "DATA_SCALE": column_details[6], "COLUMN_KEY": column_details[7]})
        return temp_col

    def create_table(self, sql_query, table_create=False):
        result = None
        #print(sql_query)
        with self.connection.cursor() as cursor:
            cursor.execute(sql_query)
            if table_create:
                self.connection.commit()
            else:
                data = cursor.fetchone()
                result = data[0]
        return result

    def delete_table(self, sql_query):
        """Used to delete the table based On query
        Parameters
        ----------
        sql_query : str
            it contain the query to delete the table 

        Returns
        -------
        result :  bool
            it return the true/false based on table is deleted or Not. 
        """
        
        result = True
        with self.connection.cursor() as cursor:
            cursor.execute(sql_query)
            self.connection.commit()
        return result

    def find_table(self,table_name):
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

        sql_query=f"SELECT count(1) FROM information_schema.tables where  TABLE_SCHEMA ='{self.database_name}' and table_name='{table_name}'"
        with self.connection.cursor() as cursor:
            cursor.execute(sql_query)
            data = cursor.fetchone()
            table_count = data[0]
        if table_count == 0:
            return True
        else:
            return False
        
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
            it return the integer value related to table space in kbs."""
        
        table_space_kb=0
        sql_query=f'''SELECT ROUND((DATA_LENGTH + INDEX_LENGTH) / 1024) AS `size_kb` 
                      FROM information_schema.TABLES 
                      WHERE TABLE_SCHEMA = '{self.database_name}' AND TABLE_NAME = '{table_name}'
                      ORDER BY (DATA_LENGTH + INDEX_LENGTH) DESC'''
        print("Sql_query-------------", sql_query)
        with self.connection.cursor() as cursor:
            cursor.execute(sql_query)
            result = cursor.fetchone()
            table_space_kb=result[0]
        return table_space_kb             