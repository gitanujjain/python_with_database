import pymssql
import datetime
class MsSqlConectionManger:
    """
    this class for MsSQL the connection and detail of databse.
    """

    def __init__(self, connection_info) -> None:
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
            print("Connection is failed ")
            raise Exception(err)
        else:
            print("Connection is establish ")

    def create_connection(self):
        """ Connect to the MSSQL server """
        con = None
        err = None
        try:
            params = {
                'server': self.host_address,
                'user': self.username,
                'password': self.password,
                'database': self.database_name,
                'port': self.port_number
            }
            con = pymssql.connect(**params)
        except Exception as err:
            return err, con
        return err, con

    def connection_close(self):
        self.connection.close()
        print("Connection has been successfully closed")

    def metadata_details(self, table_name=None):
        meta_data_details = []
        if table_name is None:
            sql_table = f"SELECT TABLE_NAME FROM information_schema.tables where TABLE_CATALOG ='{self.database_name.upper()}' and TABLE_SCHEMA='DBO' "
            with self.connection.cursor() as cursor:
                cursor.execute(sql_table)
                for result in cursor.fetchall():
                    temp = {'table_schema': 'dbo',
                            'table_name': result[0], 
                            'column_detail': self.fetch_table_details(result[0]),
                            'constraint_details':{},
                            'index_details':{}, 
                            'partition_json':self.fetch_partition_details(result[0])}
                    meta_data_details.append(temp)
        else:
            temp = {'table_schema': 'dbo',
                    'table_name': table_name, 
                    'column_detail': self.fetch_table_details(table_name),
                    'constraint_details':{},
                    'index_details':{}, 
                    'partition_json':self.fetch_partition_details(table_name)}
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
                column_details = [ 'null' if each is None else each for each in column_details ]
                temp_col.append({"column_name": column_details[0], "DATA_TYPE": column_details[1],
                                 "is_nullable": column_details[2], "COLUMN_DEFAULT": column_details[3],
                                 "DATA_LENGTH": column_details[4], "DATA_PRECISION": column_details[5],
                                 "DATA_SCALE": column_details[6]})
        return temp_col

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
        sql_table = f"SELECT count(1) FROM information_schema.tables where TABLE_CATALOG ='{self.database_name.upper()}' and TABLE_NAME='{table_name}'"

        with self.connection.cursor() as cursor:
            cursor.execute(sql_table)
            data = cursor.fetchone()
            table_count = data[0]
        if table_count == 0:
            return True
        else:
            return False

    def fetch_partition_details(self, table_name):
        temp_col = []
        sql_query = f'''WITH DistinctHighValues AS (
                            SELECT
                                t.name AS TableName,
                                ps.name AS PartitionSchemeName,
                                pf.name AS PartitionFunctionName,
                                pf.function_id AS PartitionFunctionID,
                                pf.type_desc AS PartitionFunctionType,
                                p.partition_number AS PartitionNumber,
                                prv.value AS HighValue,
                                c.name AS PartitionColumnName,
                                ty.name AS PartitionColumnType
                            FROM sys.tables t
                            JOIN sys.indexes i ON t.object_id = i.object_id
                            JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id
                            JOIN sys.partition_functions pf ON ps.function_id = pf.function_id
                            JOIN sys.partitions p ON p.object_id = t.object_id AND p.index_id = i.index_id
                            JOIN sys.partition_range_values prv ON pf.function_id = prv.function_id AND prv.boundary_id = p.partition_number - 1
                            JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                            JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                            JOIN sys.types ty ON c.user_type_id = ty.user_type_id
                            WHERE t.name = '{table_name}'
                        )
                        SELECT
                            TableName,
                            PartitionSchemeName,
                            PartitionFunctionName,
                            PartitionFunctionID,
                            PartitionFunctionType,
                            PartitionNumber,
                            CAST(HighValue AS VARCHAR(MAX)) AS HighValue,  -- Casting HighValue to VARCHAR
                            PartitionColumnName,  
                            PartitionColumnType
                        FROM
                            DistinctHighValues
                        ORDER BY
                            PartitionNumber
                        '''

        with self.connection.cursor() as cursor:
            cursor.execute(sql_query)
            partition_details_list = cursor.fetchall()
            for partition_details in partition_details_list:
                partition_details = ['null' if each is None else each for each in partition_details]

                try:
                    high_value = datetime.datetime.strptime(partition_details[6].strip(), '%b %d %Y').strftime('%Y-%m-%d')
                except ValueError:
                    high_value = partition_details[6] 
                
                temp_col.append({
                    "TableName": partition_details[0],
                    "PartitionSchemeName": partition_details[1],
                    'PartitionFunctionName': partition_details[2],
                    'PartitionFunctionID': partition_details[3],
                    'PartitionFunctionType': partition_details[4],
                    'PartitionNumber': partition_details[5],
                    'highValue': high_value,
                    'PartitionColumnName': partition_details[7],
                    'PartitionColumnType': partition_details[8]
                })
            return temp_col
        

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

    def delete_table(self, sql_query):
        result = None
        with self.connection.cursor() as cursor:
            cursor.execute(sql_query)
            self.connection.commit()
        return result
    
    def table_space(self, table_name):
        table_space=0
        sql_table = f'''
                        SELECT 
                            SUM(a.total_pages) TotalSpaceKb
                        FROM 
                            sys.tables t
                        INNER JOIN      
                            sys.indexes i ON t.object_id = i.object_id
                        INNER JOIN 
                            sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
                        INNER JOIN 
                            sys.allocation_units a ON p.partition_id = a.container_id
                        LEFT OUTER JOIN 
                            sys.schemas s ON t.schema_id = s.schema_id
                        WHERE 
                            t.name = '{table_name}'
                            AND t.is_ms_shipped = 0
                            AND i.object_id > 255 
                        GROUP BY 
                            t.name, s.name, p.rows
                     
                        '''
        with self.connection.cursor() as cursor:
            cursor.execute(sql_table)
            data = cursor.fetchone()
            table_space= data[0]
        return table_space

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