import oracledb

class OracleConectionManger:
    """
    this class for oracle database.
    """

    def __init__(self, connection_info) -> None:
        print("=====db======", connection_info)
        """
        initialize value connection details and create the connection with oracle database
        """
        self.host_address = connection_info.get('host_address', None)
        self.port_number = connection_info.get('port_number', None)
        self.database_name = connection_info.get('database_name', None)
        self.schema_name = connection_info.get('schema_name', None)
        self.username = connection_info.get('username', None)
        self.password = connection_info.get('password', None)
        self.source_schema = connection_info.get('source_schema', None)
        err, self.connection = self.create_connection()
        if self.connection is None:
            print(f"Connection creation geting error {err}")
            raise Exception(err)
        else:
            print("Connection has been created")
            
    def create_connection(self):
        """ Connect to the oracle Server database server """
        con = None
        err = None
        try:
            params = {
                'dsn': f'{self.host_address}:{self.port_number}/{self.database_name}',
                'user': self.username,
                'password': self.password}
            con = oracledb.connect(**params)
        except Exception as err:
            return err, con
        return err, con

    def metadata_details(self, table_name=None):
        """Fetches the metadata for a particular connection.
        
        Parameters
        ----------
        table_name : str
            Name of the table.
        
        Returns
        -------
        meta_data_details : list
            List containing all the details related to the table.
        """
        meta_data_details = []
        if table_name is None:
            if self.source_schema:
                sql_table = f"SELECT table_name FROM all_tables WHERE owner ='{self.source_schema.upper()}'"
            else:
                sql_table = "SELECT table_name FROM USER_TABLES"
            with self.connection.cursor() as cursor:
                cursor.execute(sql_table)
                for result in cursor.fetchall():
                    temp = {
                        'table_schema': self.source_schema.lower() if self.source_schema else self.username.lower(),
                        'table_name': result[0],
                        'column_detail': self.fetch_table_details(result[0]),
                        'constraint_details': self.fetch_primary_key_constraint(result[0]),
                        'index_details': self.fetch_index_constraint(result[0]),
                        'partition_json': self.fetch_partition_information(result[0])
                    }
                    meta_data_details.append(temp)
        else:
            temp = {
                'table_schema': self.source_schema.lower() if self.source_schema else self.username.lower(),
                'table_name': table_name,
                'column_detail': self.fetch_table_details(table_name),
                'constraint_details': self.fetch_primary_key_constraint(table_name),
                'index_details': self.fetch_index_constraint(table_name),
                'partition_json': self.fetch_partition_information(table_name)
            }
            meta_data_details.append(temp)
        return meta_data_details

    def fetch_table_details(self, table_name):
        temp_col = []
        with self.connection.cursor() as cursor_tbl:
            if self.source_schema:
                sql_col_detail = f"""
                SELECT column_name, DATA_TYPE, NULLABLE, DATA_DEFAULT, DATA_LENGTH, DATA_PRECISION, DATA_SCALE
                FROM all_tab_columns
                WHERE TABLE_NAME='{table_name}' AND OWNER='{self.source_schema.upper()}'
                """
            else:
                sql_col_detail = f"""
                SELECT column_name, DATA_TYPE, NULLABLE, DATA_DEFAULT, DATA_LENGTH, DATA_PRECISION, DATA_SCALE
                FROM user_tab_columns
                WHERE TABLE_NAME='{table_name}'
                """
            cursor_tbl.execute(sql_col_detail)
            column_details_list = cursor_tbl.fetchall()
            for column_details in column_details_list:
                column_details = ['null' if each is None else each for each in column_details]
                temp_col.append({
                    "column_name": column_details[0],
                    "DATA_TYPE": column_details[1],
                    "is_nullable": column_details[2],
                    "COLUMN_DEFAULT": column_details[3],
                    "DATA_LENGTH": column_details[4],
                    "DATA_PRECISION": column_details[5],
                    "DATA_SCALE": column_details[6]
                })
                # if column_details[1] == 'DATE':
                #     if self.source_schema:
                #         min_max_query = f"""
                #                         SELECT MIN({column_details[0]}), MAX({column_details[0]}) FROM {self.source_schema}.{table_name}
                #                         """
                #     else:
                #         min_max_query = f"""
                #         SELECT MIN({column_details[0]}), MAX({column_details[0]}) FROM {table_name}
                #         """
                #     cursor_tbl.execute(min_max_query)

                #     min_max_result = cursor_tbl.fetchall()
                #     min_date_str = min_max_result[0][0].strftime('%Y-%m-%d %H:%M:%S') if min_max_result[0][0] else None
                #     max_date_str = min_max_result[0][1].strftime('%Y-%m-%d %H:%M:%S') if min_max_result[0][1] else None
                #     temp_col.append({
                #         "extra_info": {
                #             "MIN_DATE": min_date_str,
                #             "MAX_DATE": max_date_str
                #         }
                #     })
        return temp_col

    def fetch_primary_key_constraint(self, table_name):
        temp_col = []
        with self.connection.cursor() as cursor_tbl:
            if self.source_schema:
                sql_col_detail = f'''SELECT cols.table_name,
                                            cols.column_name, 
                                            cols.position, 
                                            cons.status, 
                                            cons.owner, 
                                            cons.CONSTRAINT_NAME , 
                                            cons.CONSTRAINT_TYPE,
                                            CASE WHEN cons.SEARCH_CONDITION IS NULL THEN ''  END AS SEARCH_CONDITION
                                    FROM all_constraints cons, all_cons_columns cols
                                    WHERE  cons.constraint_name = cols.constraint_name
                                        AND cons.owner=cols.owner
                                        AND cons.owner='{self.source_schema.upper()}'
                                        AND cons.table_name='{table_name}'
                                        AND cons.CONSTRAINT_TYPE != 'R'
                                        ORDER BY cols.table_name, cols.position '''
            else:
                sql_col_detail = f'''SELECT cols.table_name,
                                            cols.column_name, 
                                            cols.position, 
                                            cons.status, 
                                            cons.owner, 
                                            cons.CONSTRAINT_NAME , 
                                            cons.CONSTRAINT_TYPE,
                                            CASE WHEN cons.SEARCH_CONDITION IS NULL THEN ''  END AS SEARCH_CONDITION
                                    FROM all_constraints cons, all_cons_columns cols
                                    WHERE  cons.constraint_name = cols.constraint_name
                                        AND cons.owner=cols.owner
                                        AND cons.owner='{self.username.upper()}'
                                        AND cons.table_name='{table_name}'
                                        AND cons.CONSTRAINT_TYPE != 'R'
                                        ORDER BY cols.table_name, cols.position '''
            cursor_tbl.execute(sql_col_detail)
            column_details_list = cursor_tbl.fetchall()
            for column_details in column_details_list:
                column_details = ['null' if each is None else each for each in column_details]
                temp_col.append({"table_name": column_details[0], "column_name": column_details[1],
                                 "position": column_details[2], "status": column_details[3],
                                 "owner": column_details[4], "constraint_name": column_details[5],
                                 "constraint_type": column_details[6], 'search_condition': column_details[7]})

        return temp_col + self.fetch_foreign_key_constraint(table_name)

    def fetch_foreign_key_constraint(self, table_name):
        temp_col = []
        with self.connection.cursor() as cursor_tbl:
            if self.source_schema:
                sql_col_detail = f'''SELECT a.constraint_name,
                                            a.table_name, 
                                            a.column_name,  
                                            c.owner, 
                                            c_pk.table_name r_table_name,  
                                            b.column_name r_column_name,
                                            c_pk.owner r_owner,
                                            c.constraint_type 
                                    FROM user_cons_columns a
                                    JOIN user_constraints c ON a.owner = c.owner
                                        AND a.constraint_name = c.constraint_name
                                    JOIN user_constraints c_pk ON c.r_owner = c_pk.owner
                                        AND c.r_constraint_name = c_pk.constraint_name
                                    JOIN user_cons_columns b ON C_PK.owner = b.owner
                                        AND  C_PK.CONSTRAINT_NAME = b.constraint_name AND b.POSITION = a.POSITION     
                                    WHERE c.constraint_type = 'R' AND  c.owner ='{self.source_schema.upper()}' AND a.table_name='{table_name}' '''
            else:
                sql_col_detail = f'''SELECT a.constraint_name,
                                            a.table_name, 
                                            a.column_name,  
                                            c.owner, 
                                            c_pk.table_name r_table_name,  
                                            b.column_name r_column_name,
                                            c_pk.owner r_owner,
                                            c.constraint_type 
                                    FROM user_cons_columns a
                                    JOIN user_constraints c ON a.owner = c.owner
                                        AND a.constraint_name = c.constraint_name
                                    JOIN user_constraints c_pk ON c.r_owner = c_pk.owner
                                        AND c.r_constraint_name = c_pk.constraint_name
                                    JOIN user_cons_columns b ON C_PK.owner = b.owner
                                        AND  C_PK.CONSTRAINT_NAME = b.constraint_name AND b.POSITION = a.POSITION     
                                    WHERE c.constraint_type = 'R' AND  c.owner ='{self.username.upper()}' AND a.table_name='{table_name}' '''
            # print(sql_col_detail)
            cursor_tbl.execute(sql_col_detail)
            column_details_list = cursor_tbl.fetchall()
            for column_details in column_details_list:
                column_details = ['null' if each is None else each for each in column_details]
                temp_col.append({"constraint_name": column_details[0], "table_name": column_details[1],
                                 "column_name": column_details[2],
                                 "owner": column_details[3], "r_table_name": column_details[4],
                                 "r_column_name": column_details[5], "r_owner": column_details[6],
                                 "constraint_type": column_details[7]})
        return temp_col

    def fetch_index_constraint(self, table_name):
        temp_col = []
        with self.connection.cursor() as cursor_tbl:
            if self.source_schema:
                sql_col_detail = f'''select ind.table_owner, 
                                            ind.table_name,
                                            ind_col.column_name,
                                            ind.index_name,
                                            ind.index_type,
                                            ind.table_type 
                                from sys.all_indexes ind
                                inner join sys.all_ind_columns ind_col
                                        on ind.owner = ind_col.index_owner
                                        and ind.index_name = ind_col.index_name
                                where ind.uniqueness = 'UNIQUE' AND ind.owner ='{self.source_schema.upper()}' AND ind.table_name= '{table_name}' '''
            else:
                sql_col_detail = f'''select ind.table_owner, 
                                            ind.table_name,
                                            ind_col.column_name,
                                            ind.index_name,
                                            ind.index_type,
                                            ind.table_type 
                                from sys.all_indexes ind
                                inner join sys.all_ind_columns ind_col
                                        on ind.owner = ind_col.index_owner
                                        and ind.index_name = ind_col.index_name
                                where ind.uniqueness = 'UNIQUE' AND ind.owner ='{self.username.upper()}' AND ind.table_name= '{table_name}' '''
            # print(sql_col_detail)
            cursor_tbl.execute(sql_col_detail)
            column_details_list = cursor_tbl.fetchall()
            for column_details in column_details_list:
                temp_col.append({"table_owner": column_details[0], "table_name": column_details[1],
                                 "column_name": column_details[2],
                                 "index_name": column_details[3], "index_type": column_details[4]})
        return temp_col

    def fetch_partition_information(self, table_name):
        if self.source_schema:
            sql_query = f'''
                            SELECT
                                p1.table_owner AS table_owner,
                                p1.table_name AS table_name,
                                p1.high_value AS high_value,
                                p1.partition_name AS partition_name,
                                c.column_name AS column_name,
                                p1.tablespace_name AS tablespace_name,
                                t.partitioning_type AS partition_type,
                                (SELECT COUNT(*)
                                FROM all_tab_partitions apt
                                WHERE apt.table_owner = p1.table_owner
                                AND apt.table_name = p1.table_name
                                ) AS partition_count,
                                p2.high_value AS min_value,
                                p1.high_value AS max_value,
                                p1.partition_position AS partition_position
                            FROM
                                all_tab_partitions p1
                            LEFT JOIN
                                all_tab_partitions p2
                            ON p1.PARTITION_position = p2.PARTITION_position + 1
                                AND p1.table_owner = p2.table_owner
                                AND p1.table_name = p2.table_name
                            JOIN
                                all_part_key_columns c
                            ON p1.table_owner = c.owner
                                AND p1.table_name = c.name
                            JOIN
                                all_part_tables t
                            ON p1.table_owner = t.owner
                                AND p1.table_name = t.table_name
                            WHERE
                                p1.table_owner = '{self.source_schema.upper()}'
                                AND p1.table_name = '{table_name}'
                            ORDER BY
                                p1.partition_position
                            '''
        else:
            sql_query = f'''
                           SELECT
                                p1.table_owner AS table_owner,
                                p1.table_name AS table_name,
                                p1.high_value AS high_value,
                                p1.partition_name AS partition_name,
                                c.column_name AS column_name,
                                p1.tablespace_name AS tablespace_name,
                                t.partitioning_type AS partition_type,
                                (SELECT COUNT(*)
                                FROM all_tab_partitions apt
                                WHERE apt.table_owner = p1.table_owner
                                AND apt.table_name = p1.table_name
                                ) AS partition_count,
                                p2.high_value AS min_value,
                                p1.high_value AS max_value,
                                p1.partition_position AS partition_position
                            FROM
                                all_tab_partitions p1
                            LEFT JOIN
                                all_tab_partitions p2
                            ON p1.PARTITION_position = p2.PARTITION_position + 1
                                AND p1.table_owner = p2.table_owner
                                AND p1.table_name = p2.table_name
                            JOIN
                                all_part_key_columns c
                            ON p1.table_owner = c.owner
                                AND p1.table_name = c.name
                            JOIN
                                all_part_tables t
                            ON p1.table_owner = t.owner
                                AND p1.table_name = t.table_name
                            WHERE
                                p1.table_owner = '{self.username.upper()}'
                                AND p1.table_name = '{table_name}'
                            ORDER BY
                                p1.partition_position
                            '''
        
        temp_col = []
        with self.connection.cursor() as cursor:
            cursor.execute(sql_query)
            all_records = cursor.fetchall()
            for record in all_records:
                record = ['null' if each is None else each for each in record]
                temp_col.append({
                    "table_owner": record[0],
                    "table_name": record[1],
                    "high_value": record[2],
                    "partition_name": record[3],
                    "column_name": record[4],
                    "tablespace_name": record[5],
                    "partition_type": record[6],
                    "partition_count": record[7],
                    "min_value":record[8],
                    "max_value":record[9],
                    "partition_position":record[10],
                    "selected":False,
                    "dropped":False
                })
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
            return None

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
        print(f"from oracle databse named '{self.database_name}', user try to find the table '{table_name}'", )
        sql = f"SELECT count(1) FROM USER_TABLES where table_name = '{table_name.upper()}'"
        table_count = 0
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
        print("---------------------Sql Query", sql_query)
        with self.connection.cursor() as cursor:
            cursor.execute(sql_query)
            if table_create:
                self.connection.commit()
            else:
                data = cursor.fetchone()
                result = data[0]

        return result
    
    def calculate_checksum(self, sql_query):
        result = []
        print("---------------------Sql Query", sql_query)
        with self.connection.cursor() as cursor:
            cursor.execute(sql_query)
            checksum_all = cursor.fetchall()
            for each in checksum_all:
                result.append(each[0])
        return result
    
    def find_min_max_value(self, table_name, column_name):
        print("table_name, --------------", table_name, "column_name+++++++",column_name)
        sql_query=f'SELECT min({column_name}) AS min_value, max({column_name}) AS max_value FROM {table_name}'
        result={}
        with self.connection.cursor() as cursor:
            cursor.execute(sql_query)
            temp_result = cursor.fetchall()

            for each in temp_result:
                temp = ['null' if e is None else e for e in each ]
                result={"min_value": f'{temp[0]}', "max_value": f'{temp[1]}'}
                
        return result

    def delete_table(self, sql_query):
        result = None
        print("-----------delete_table----------------", sql_query)
        with self.connection.cursor() as cursor:
            cursor.execute(sql_query)
            self.connection.commit()
        return result
    
    def connection_close(self):
        self.connection.close()

    def table_space(self, table_name):
        try:
            if table_name is not None:
                sql = f"SELECT Nvl(SUM(bytes)/1024,0) FROM user_segments WHERE segment_name= '{table_name}'"
                print(sql)
            with self.connection.cursor() as cursor:
                cursor.execute(sql)
                data = cursor.fetchone()
                print(data)
                records_count = data[0]
                print(records_count)
            return records_count
        except Exception as err:
            raise err
