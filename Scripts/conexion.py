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

    def createTable(self, database, table_name, columns):
        try:
            self.cursor.execute(f'USE {database}')

            # Drop the table if it already exists
            drop_table_query = f"DROP TABLE IF EXISTS {table_name};"
            self.cursor.execute(drop_table_query)

            # Create the new table
            create_table_query = f"CREATE TABLE {table_name} ({columns});"
            
            print(create_table_query)
            
            self.cursor.execute(create_table_query)
            self.db.commit()

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
        # cerramos el cursor
        self.cursor.close()
        # cierra la conexion con la base de Datos
        self.db.close()


def main():
    host = '127.0.0.1'
    user = 'admin'
    passwd = '12345'

    conexion = Conexion(host, user, passwd)

    if conexion.is_connected():
        print('Connected to the database.')

        new_database_name = 'predictor'
        result_create_db = conexion.createDatabase(new_database_name)
        print(result_create_db)

        new_table_name = 'predicciones'
        table_columns = 'id INT PRIMARY KEY, name VARCHAR(255), age INT'
        result_create_table = conexion.createTable(new_database_name, new_table_name, table_columns)
        print(result_create_table)

    else:
        print('Failed to connect to the database.')

if __name__ == '__main__':
    main()
