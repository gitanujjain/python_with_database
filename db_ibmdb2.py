import socket
import ibm_db


class Db2ConnectionManager():
    """A simple class that manages a Db2 server or database connection."""
    def __init__(self,connection_info):
        """Initialize Db2 server or database name, user ID, and password attributes."""

        self.ds_type = connection_info.get('dsType', 'DB')
        self.host_address=connection_info.get('host_address','')
        self.port_number=connection_info.get('port_number',50000)
        self.database_name=connection_info.get('database_name','')
        self.schema_name=connection_info.get('schema_name','')
        self.username=connection_info.get('username','')
        self.password=connection_info.get('password','')
        self.source_schema=connection_info.get('source_schema','')
        err, self.connection = self.create_connection()
        if self.connection is None :
             raise Exception(err)
        
    def create_connection(self):
        """Attempt to establish a Db2 server or database connection."""
        # Define And Initialize The Appropriate Local Variables
        conn_string = "DRIVER={IBM DB2 ODBC DRIVER}"
        connectionID=None
        msg_string=''
        conn_string += ";DATABASE=" + self.database_name  # Only Used To Connect To A Database
        conn_string += ";HOSTNAME=" + str(self.host_address)  # Only Used To Connect To A Server
        conn_string += ";PORT=" + str(self.port_number)  # Only Used To Connect To A Server
        conn_string += ";PROTOCOL=TCPIP"
        conn_string += ";UID=" + self.username
        conn_string += ";PWD=" + self.password
        print(conn_string)
        try:
            connectionID = ibm_db.connect(conn_string, self.username, self.password)
        except Exception:
            pass
        if connectionID is None:
            msg_string = ibm_db.conn_errormsg()
        return msg_string, connectionID
    

    def connection_close(self):
        """Attempt to close a Db2 server or database connection."""
        msg_string = ""
        return_code = True
        if self.connection is not None:
            try:
                return_code = ibm_db.close(self.connection)
            except Exception:
                pass
            if return_code is False:
                msg_string = ibm_db.conn_errormsg(self.connection)
                return_code = False
            else:
                return_code = True
        return return_code, msg_string


    def metadata_details(self, table_name=None):
        meta_data_Details=[]
        if table_name is None:
            sql_table = f"SELECT TABNAME AS table_name FROM SYSCAT.TABLES WHERE TABSCHEMA='{self.username.upper()}'"
            print(sql_table)
            
            res_table=ibm_db.exec_immediate(self.connection, sql_table)
            result = ibm_db.fetch_both(res_table)
            while result:
                temp={'table_schema': self.username.lower(),
                      'table_name': result['TABLE_NAME'], 
                      'column_detail':self.fetch_table_details( result['TABLE_NAME']),
                      'constraint_details':{},
                      'index_details':{},
                      'partition_json':self.fetch_partition_details(result['TABLE_NAME'])
                }
                meta_data_Details.append(temp)
                result= ibm_db.fetch_both(res_table)            
        else:
            temp={'table_schema': self.username.lower(),
                  'table_name': table_name, 
                  'column_detail':self.fetch_table_details(table_name),
                  'constraint_details':{},
                  'index_details':{}, 
                  'partition_json':self.fetch_partition_details(table_name)
                  }
            meta_data_Details.append(temp)
        return meta_data_Details

    def fetch_table_details(self, table_name):
        temp_col = []
        sql_col_detail=f''' SELECT 
                                col.COLNAME AS column_name, 
                                dt.TYPENAME AS data_type,
                                col.LENGTH AS data_length,
                                dt.LENGTH AS DATA_PRECISION, 
                                col.SCALE AS DATA_SCALE 
                            FROM SYSCAT.COLUMNS AS col 
                            JOIN SYSCAT.DATATYPES AS dt 
                            ON col.TYPENAME = dt.TYPENAME  
                            WHERE col.TABNAME = '{table_name}' '''
        res_table_col_det=ibm_db.exec_immediate(self.connection, sql_col_detail)
        result2 = ibm_db.fetch_both(res_table_col_det)
        while result2:
            temp_col.append({"column_name":result2['COLUMN_NAME'],
                             "DATA_TYPE":result2['DATA_TYPE'],
                             "DATA_LENGTH":result2['DATA_LENGTH'],
                             "DATA_PRECISION":result2['DATA_PRECISION'],
                             "DATA_SCALE":result2['DATA_SCALE']})
            result2 = ibm_db.fetch_both(res_table_col_det)
        return temp_col
    
    def fetch_partition_details(self, table_name):
        temp_col = []
        sql_part_details = f'''
                    SELECT
                        dp.DATAPARTITIONNAME AS partiton_name,
                        dp.DATAPARTITIONID,
                        dp.LOWVALUE AS min_value,
                        dp.HIGHVALUE AS max_value,
                        dpe.DATAPARTITIONEXPRESSION AS column_name
                    FROM
                        SYSCAT.DATAPARTITIONS dp
                    JOIN
                        SYSCAT.DATAPARTITIONEXPRESSION dpe
                    ON
                        dp.TABSCHEMA = dpe.TABSCHEMA
                        AND dp.TABNAME = dpe.TABNAME
                    WHERE
                        dp.TABNAME = '{table_name}'
                        AND dp.TABSCHEMA = '{self.username.upper()}'
        '''
        stmt = ibm_db.exec_immediate(self.connection, sql_part_details)
        temp_col = []
        result = ibm_db.fetch_both(stmt)
        while result:
            temp_col.append({
                "partition_name": result[0],
                "partition_id": result[1],
                "min_value": result[2],
                "max_value": result[3],
                "column_name": result[4],
	            "table_name": table_name,
                "partition_type": "RANGE",
                "selected":False,
                "dropped":False
            })
            result = ibm_db.fetch_both(stmt)
        return temp_col
    
    
    def table_count_db2(self, table_name):
    
        new_sql = f"select /*+ parallel(16)*/ count(1) from {table_name}"
        try:
            prepare_statement = ibm_db.prepare(self.connection, new_sql)
        except Exception:
            pass
        if prepare_statement is False:
            print("\nERROR: Unable to prepare the SQL statement specified.")
            return False,0
        else:
            print("Executing the prepared SQL statement ... ", end="")
            try:
                return_code = ibm_db.execute(prepare_statement)
            except Exception:
                pass
            if return_code is False:
                print("\nERROR: Unable to execute the SQL statement specified.")
                return False, 0
            else:
                try:
                    dataRecord = ibm_db.fetch_tuple(prepare_statement)
                except:
                    pass
                if dataRecord is False:
                    return False, 0
                else:
                    return True, dataRecord[0]
     
     
    def find_table(self, table_name):
        sql_query = f"SELECT COUNT(1) FROM SYSCAT.TABLES WHERE TABNAME = '{table_name}' AND TABSCHEMA = '{self.username.upper()}'"
        table_count = 0
        try:
            prepare_statement = ibm_db.prepare(self.connection, sql_query)
            return_code = ibm_db.execute(prepare_statement)
            if not return_code:
                print("\nERROR: Unable to execute the SQL statement specified.")
                return None
            
            dataRecord = ibm_db.fetch_tuple(prepare_statement)
            if dataRecord is not False:
                table_count = dataRecord[0]
        except Exception as e:
            print(f"Error in create table: {e}")
            print("Exception details:", e.__class__.__name__, str(e))
        return table_count == 0

    
    def calculate_checksum(self, sql_query):
        result = []
        print("---------------------Sql Query:", sql_query)        
        try:
            stmt = ibm_db.prepare(self.connection, sql_query)
            return_code = ibm_db.execute(stmt)
            if return_code:
                row = ibm_db.fetch_assoc(stmt)
                while row:
                    result.append(row['SHA1_HASH'])  
                    row = ibm_db.fetch_assoc(stmt)
            else:
                print("Error executing the query")
        
        except Exception as e:
            print(f"An error occurred: {e}")
        
        return result
    

    def create_table(self, sql_query, table_create=False):
        result = None
        print("Connection object:", self.connection)
        print(sql_query)
        try:
            if isinstance(sql_query, str):
                prepare_statement = ibm_db.prepare(self.connection, sql_query)
                return_code = ibm_db.execute(prepare_statement)
                if not return_code:
                    print("\nERROR: Unable to execute the SQL statement specified.")
                    return None
                if not table_create:
                    dataRecord = ibm_db.fetch_tuple(prepare_statement)
                    if dataRecord:
                        result = dataRecord[0]

            elif isinstance(sql_query, list):
                for query in sql_query:
                    try:
                        prepare_statement = ibm_db.prepare(self.connection, query)
                        return_code = ibm_db.execute(prepare_statement)
                        if not return_code:
                            print(f"\nERROR: Unable to execute the SQL statement specified for query: {query.strip()}")
                        print(f"Executed: {query.strip()}")
                    except Exception as e:
                        print(f"An error occurred while executing: {query.strip()}")
                        print(f"Error: {e}")
            return result

        except Exception as e:
            print(f"Error in create table: {e}")
            print("Exception details:", e.__class__.__name__, str(e))
            return result

    def delete_table(self, sql_query):
        result = None
        print("Connection object:", self.connection)  
        print(sql_query)
        try:
            prepare_statement = ibm_db.prepare(self.connection, sql_query)
            return_code = ibm_db.execute(prepare_statement)
            if not return_code:
                print("\nERROR: Unable to execute the SQL statement specified.")
                return None
        except Exception as e:
            print(f"Error in create table: {e}")
            print("Exception details:", e.__class__.__name__, str(e))
        return result
    

    def table_space(self, table_name):
        """
        
        """
        sql_query = f"""
                    SELECT (DATA_OBJECT_P_SIZE + INDEX_OBJECT_P_SIZE + LONG_OBJECT_P_SIZE + LOB_OBJECT_P_SIZE + XML_OBJECT_P_SIZE) AS TOTAL_SIZE_IN_KB 
                    FROM SYSIBMADM.ADMINTABINFO 
                    WHERE tabname='{table_name}';
                    """
        table_space = 0
        try:
            prepare_statement = ibm_db.prepare(self.connection, sql_query)
            return_code = ibm_db.execute(prepare_statement)
            if not return_code:
                print("\nERROR: Unable to execute the SQL statement specified.")
                return None
            
            dataRecord = ibm_db.fetch_tuple(prepare_statement)
            if dataRecord is not False:
                table_space = dataRecord[0]
        except Exception as e:
            print(f"Error in create table: {e}")
            print("Exception details:", e.__class__.__name__, str(e))
        return table_space 

    def find_min_max_value(self, table_name, column_name):
        sql_query=f'SELECT min({column_name}) AS min_value, max({column_name}) AS max_value FROM {table_name}'
        result={}
        try:
            prepare_statement = ibm_db.prepare(self.connection, sql_query)
            return_code = ibm_db.execute(prepare_statement)
            if not return_code:
                print("\nERROR: Unable to execute the SQL statement specified.")
            else:    
                dataRecord = ibm_db.fetch_tuple(prepare_statement)
                if dataRecord is not False:
                    table_space = dataRecord[0]
                    for each in table_space:
                        temp = ['null' if e is None else e for e in each ]
                        result={"min_value": f'{temp[0]}', "max_value": f'{temp[1]}'}

        except Exception as e:
            print(f"Error in create table: {e}")
            print("Exception details:", e.__class__.__name__, str(e))
        return result