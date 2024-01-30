import os
import pandas as pd
from conexion import Conexion  # Asegúrate de reemplazar "tu_archivo_mysql" con el nombre real de tu archivo.
from sqlalchemy import create_engine

def excel_to_dataframe(file_name):
    try:
        # Lee el archivo Excel y devuelve un DataFrame
        df = pd.read_excel(file_name)
        return df
    except Exception as e:
        print(f"No se pudo leer el archivo {file_name}. Error: {str(e)}")
        return None

def main():
    # Nombre del archivo Excel que deseas convertir en DataFrame
    file_name = "Data\Output\Prediccion\Dataframe_Calculo.xlsx"

    # Convierte el archivo Excel a un DataFrame
    prediccion = excel_to_dataframe(file_name)
    
    # Convierte el segundo archivo Excel a un DataFrame
    combinado = excel_to_dataframe('Data\Output\Dataframe\Dataframe_Combinado.xlsx')

    # Imprime el DataFrame resultante
    if prediccion is not None:
        print("DataFrame resultante:")

        # Agregar una columna de ID incremental (PK)
        prediccion.reset_index(inplace=True)        
        nuevos_nombres_prediccion = {'index': 'id', 'ÁREA DE CONOCIMIENTO': 'AREA_CONOCIMIENTO'}
        prediccion.rename(columns=nuevos_nombres_prediccion, inplace=True)
        
        combinado.reset_index(inplace=True)
        nuevos_nombres_combinado = {'index': 'id', 'ÁREA DE CONOCIMIENTO': 'AREA_CONOCIMIENTO', '# EST': 'NUM_EST'}
        combinado.rename(columns=nuevos_nombres_combinado, inplace=True)

        # Configuración de la conexión a la base de datos MySQL
        host = "127.0.0.1"
        user = "admin"
        passwd = "12345"
        database_name = "Planificacion_Academica"
        table_name = "Predicciones"

        # Crear la conexión
        db_connection = Conexion(host, user, passwd)

        # Crear la base de datos si no existe
        db_connection.createDatabase(database_name)

        # Crear la tabla en la base de datos
        # Obtener tipos de datos y nombres de columnas del DataFrame
        column_data_types = {'id': 'INT AUTO_INCREMENT PRIMARY KEY'}
        custom_data_types = {'CAMPUS': 'VARCHAR(100)', 
                            'DEPARTAMENTO': 'VARCHAR(100)', 
                            'ASIGNATURA': 'VARCHAR(100)', 
                            'AREA_CONOCIMIENTO': 'VARCHAR(100)',
                            'CODIGO_ASIGNATURA': 'VARCHAR(100)',
                            'CODIGO': 'VARCHAR(100)',
                            'ESTUDIANTES_RL': 'FLOAT',
                            'ESTUDIANTES_SE': 'FLOAT',
                            'ESTUDIANTES_AD': 'FLOAT',
                            'NRC_RL': 'FLOAT',
                            'NRC_SE': 'FLOAT',
                            'NRC_AD': 'FLOAT',
                            'HORAS_RL': 'FLOAT',
                            'HORAS_SE': 'FLOAT',
                            'HORAS_AD': 'FLOAT',
                            'OBSERVACION_RL': 'VARCHAR(100)',
                            'OBSERVACION_SE': 'VARCHAR(100)',
                            'OBSERVACION_AD': 'VARCHAR(100)'
                            }
        
        column_data_types_combinado = {'id': 'INT AUTO_INCREMENT PRIMARY KEY'}
        data_combinado = {'id': 'INT AUTO_INCREMENT PRIMARY KEY',
                        'DEPARTAMENTO': 'VARCHAR(100)',
                        'CAMPUS': 'VARCHAR(100)',
                        'AREA_CONOCIMIENTO': 'VARCHAR(100)', 
                        'CODIGO_ASIGNATURA': 'VARCHAR(100)',
                        'NRC': 'VARCHAR(30)',
                        'STATUS': 'VARCHAR(30)',
                        'NUM_EST': 'INT',
                        'HI': 'VARCHAR(30)',
                        'HF': 'VARCHAR(30)',
                        'L': 'VARCHAR(3)',
                        'M': 'VARCHAR(3)',
                        'I': 'VARCHAR(3)',
                        'J': 'VARCHAR(3)',
                        'V': 'VARCHAR(3)',
                        'HORA_DIA': 'INT',
                        'NUM_DIAS': 'INT',
                        'HORAS': 'INT',
                        'TIPO': 'VARCHAR(10)',
                        'PERIODO': 'VARCHAR(10)',
                        'OBSERVACION': 'VARCHAR(10)',
                        }

        # Agregar las columnas personalizadas al diccionario
        column_data_types.update({nombre_columna: f"{tipo_dato}" for nombre_columna, tipo_dato in custom_data_types.items()})
        db_connection.createTable(database_name, table_name, column_data_types)
        
        # Agregar las columnas personalizadas al diccionario
        column_data_types_combinado.update({nombre_columna: f"{tipo_dato}" for nombre_columna, tipo_dato in data_combinado.items()})
        db_connection.createTable(database_name, "Combinado", column_data_types_combinado)

        # Crear un motor de SQLAlchemy
        engine = create_engine(f"mysql+mysqldb://{user}:{passwd}@{host}/{database_name}")

        # Insertar los datos del DataFrame en la tabla usando SQLAlchemy
        try:
            prediccion.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
            combinado.to_sql(name="Combinado", con=engine, if_exists='replace', index=False)
            print(f"Datos insertados en la tabla '{table_name}' de la base de datos '{database_name}'.")
        except Exception as e:
            print(f"Error al insertar datos en la tabla '{table_name}' de la base de datos '{database_name}': {str(e)}")

    else:
        print("No se pudo leer el archivo Excel.")

if __name__ == "__main__":
    main()
