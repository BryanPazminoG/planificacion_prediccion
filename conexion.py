import mysql.connector

# Clase de conexion a base de datos
class Conexion:
    cursor = ''
    db = ''

    def __init__(self, host, user, passwd):
        try:
            self.db = mysql.connector.connect(host=host, user=user, passwd=passwd, charset='utf8', use_unicode=True)
            self.cursor = self.db.cursor(dictionary=True)

        except mysql.connector.Error as e:
            print('Error al conectar a la base de datos, "' + host + '"')
            exit(0)

    def runQuery(self, database, sql_query, dictionary, all):
        try:
            # se evalua si la consulta se requiere como un diccionario o una lista de valores simple
            if dictionary:
                self.cursor = self.db.cursor(dictionary=True)
            else:
                self.cursor = self.db.cursor()

            # setear la base a utilizar
            self.cursor.execute('use ' + str(database))

            # Ejecutar el query
            self.cursor.execute(sql_query)

            self.db.commit()
            # si all es true busca todas las coincidencias si no solo devuelve la primera coincidencia
            if all:
                return {
                    'status': 'success',
                    'count': self.cursor.rowcount,
                    'data': self.cursor.fetchall()
                }
            else:
                return {
                    'status': 'success',
                    'count': self.cursor.rowcount,
                    'data': self.cursor.fetchone()
                }

        except mysql.connector.Error as e:
            return {
                'status': 'error',
                'message': e.args
            }

    def createDatabase(self, database_name):
        try:
            # Create a new database
            create_db_query = f"CREATE DATABASE IF NOT EXISTS {database_name} CHARACTER SET utf8 COLLATE utf8_general_ci;"
            self.cursor.execute(create_db_query)
            self.db.commit()

            return {
                'status': 'success',
                'message': f"Database '{database_name}' created successfully."
            }

        except mysql.connector.Error as e:
            return {
                'status': 'error',
                'message': e.args
            }

    def createTable(self, database, table_name, column_data_types):
        try:
            # Cambiar a la base de datos
            self.cursor.execute(f'USE {database}')

            # Eliminar la tabla si ya existe
            drop_table_query = f"DROP TABLE IF EXISTS {table_name};"
            self.cursor.execute(drop_table_query)

            # Crear la definición de las columnas
            columns_definition = ', '.join([f"{column} {data_type}" for column, data_type in column_data_types.items()])
            
            # Crear la consulta CREATE TABLE
            create_table_query = f"CREATE TABLE {table_name} ({columns_definition})"
            
            # Ejecutar la consulta CREATE TABLE
            #self.cursor.execute(create_table_query)
            self.runQuery(database="Planificacion_Academica", 
                        sql_query=create_table_query,
                        dictionary=False,
                        all=False)
            # Confirmar la transacción
            #self.db.commit()

            return {
                'status': 'success',
                'message': f"Table '{table_name}' created successfully in database '{database}'."
            }

        except mysql.connector.Error as e:
            print(f"Error creating table '{table_name}' in database '{database}'.")
            print(str(e))
            return {
                'status': 'error',
                'message': e.args
            }

    def is_connected(self):
        if self.db.is_connected():
            return True
        else:
            return False

    def __del__(self):
        if self.db and self.db.is_connected():  # Verificar si hay una conexión y está abierta
            # Cerrar la conexión con la base de datos
            self.db.close()



