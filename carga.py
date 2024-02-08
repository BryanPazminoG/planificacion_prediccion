import os
import pandas as pd
from conexion import Conexion  # Asegúrate de reemplazar "tu_archivo_mysql" con el nombre real de tu archivo.
from sqlalchemy import create_engine
import sqlalchemy
import winreg

def excel_to_dataframe(file_name):
    try:
        # Lee el archivo Excel y devuelve un DataFrame
        df = pd.read_excel(file_name)
        return df
    except Exception as e:
        print(f"No se pudo leer el archivo {file_name}. Error: {str(e)}")
        return None
    
def get_mysql_credentials_from_registry():
    try:
        # Abrir la clave del registro donde se almacenan las credenciales de MySQL
        key = winreg.OpenKey(winreg.HKEY_CURRENT_USER, r"SOFTWARE\Predictor")

        # Leer los valores de host, usuario y contraseña
        host, _ = winreg.QueryValueEx(key, "Host")
        user, _ = winreg.QueryValueEx(key, "User")
        passwd, _ = winreg.QueryValueEx(key, "Password")

        # Cerrar la clave del registro
        winreg.CloseKey(key)

        return host, user, passwd
    except FileNotFoundError:
        # Si la clave del registro no existe, devolver valores predeterminados o None
        return None, None, None
def set_mysql_credentials_to_registry(host, user, passwd):
    try:
        # Crear o abrir la clave del registro donde se almacenarán las credenciales de MySQL
        key = winreg.CreateKey(winreg.HKEY_CURRENT_USER, r"SOFTWARE\Predictor")

        # Establecer los valores de host, usuario y contraseña
        winreg.SetValueEx(key, "Host", 0, winreg.REG_SZ, host)
        winreg.SetValueEx(key, "User", 0, winreg.REG_SZ, user)
        winreg.SetValueEx(key, "Password", 0, winreg.REG_SZ, passwd)

        # Cerrar la clave del registro
        winreg.CloseKey(key)
    except Exception as e:
        print(f"Error al escribir en el Registro: {e}")


    

def main():
    # Nombre del archivo Excel que deseas convertir en DataFrame
    file_name = "Data\Output\Prediccion\Dataframe_Predicciones_Calculo.xlsx"

    # Convierte el archivo Excel a un DataFrame
    prediccion = excel_to_dataframe(file_name)
    
    # Convierte el segundo archivo Excel a un DataFrame
    combinado = excel_to_dataframe('Data\Output\Dataframe\Dataframe_Combinado.xlsx')

    # Imprime el DataFrame resultante
    if prediccion is not None:
        print("DataFrame resultante:")

        # Agregar una columna de ID incremental (PK)
        prediccion.reset_index(inplace=True)        
        nuevos_nombres_prediccion = {'ÁREA DE CONOCIMIENTO': 'AREA_CONOCIMIENTO'}
        prediccion.rename(columns=nuevos_nombres_prediccion, inplace=True)
        
        combinado.reset_index(inplace=True)
        nuevos_nombres_combinado = {'ÁREA DE CONOCIMIENTO': 'AREA_CONOCIMIENTO', '# EST': 'NUM_EST'}
        combinado.rename(columns=nuevos_nombres_combinado, inplace=True)

        host, user, passwd = get_mysql_credentials_from_registry()

        # Si las credenciales no existen, solicitarlas al usuario y guardarlas en el Registro
        if host is None or user is None or passwd is None:
            host = input("Introduce el host de la base de datos MySQL: ")
            user = input("Introduce el nombre de usuario de la base de datos MySQL: ")
            passwd = input("Introduce la contraseña de la base de datos MySQL: ")
            set_mysql_credentials_to_registry(host, user, passwd)


        database_name = "Planificacion_Academica"
        table_name = "Predicciones"

        # Crear la conexión
        db_connection = Conexion(host, user, passwd)

        # Crear la base de datos si no existe
        db_connection.createDatabase(database_name)
        
        engine = create_engine(f"mysql+mysqldb://{user}:{passwd}@{host}/{database_name}")

        custom_data_types = {
            'ID': sqlalchemy.types.Integer().with_variant(sqlalchemy.types.Integer(), "sqlite"),
            'CAMPUS': sqlalchemy.String(100), 
            'DEPARTAMENTO': sqlalchemy.String(100), 
            'ASIGNATURA': sqlalchemy.String(100), 
            'AREA_CONOCIMIENTO': sqlalchemy.String(100),
            'CODIGO_ASIGNATURA': sqlalchemy.String(100),
            'CODIGO': sqlalchemy.String(100),
            'ESTUDIANTES_RL': sqlalchemy.types.FLOAT(),
            'ESTUDIANTES_SE': sqlalchemy.types.FLOAT(),
            'ESTUDIANTES_AD': sqlalchemy.types.FLOAT(),
            'NRC_RL': sqlalchemy.types.FLOAT(),
            'NRC_SE': sqlalchemy.types.FLOAT(),
            'NRC_AD': sqlalchemy.types.FLOAT(),
            'HORAS_RL': sqlalchemy.types.FLOAT(),
            'HORAS_SE': sqlalchemy.types.FLOAT(),
            'HORAS_AD': sqlalchemy.types.FLOAT(),
            'OBSERVACION_RL': sqlalchemy.String(100),
            'OBSERVACION_SE': sqlalchemy.String(100),
            'OBSERVACION_AD': sqlalchemy.String(100)
        }

        data_combinado = {
                        
                        'ID': sqlalchemy.types.Integer().with_variant(sqlalchemy.types.Integer(), "sqlite"),
                        'DEPARTAMENTO': sqlalchemy.String(100),
                        'CAMPUS': sqlalchemy.String(100),
                        'AREA_CONOCIMIENTO': sqlalchemy.String(100), 
                        'CODIGO_ASIGNATURA': sqlalchemy.String(100),
                        'NRC': sqlalchemy.String(10),
                        'STATUS': sqlalchemy.String(10),
                        'NUM_EST': sqlalchemy.types.Integer(),
                        'HI': sqlalchemy.String(10),
                        'HF': sqlalchemy.String(10),
                        'L': sqlalchemy.String(10),
                        'M': sqlalchemy.String(10),
                        'I': sqlalchemy.String(10),
                        'J': sqlalchemy.String(10),
                        'V': sqlalchemy.String(10),
                        'HORA_DIA': sqlalchemy.types.Integer(),
                        'NUM_DIAS': sqlalchemy.types.Integer(),
                        'HORAS': sqlalchemy.types.Integer(),
                        'TIPO': sqlalchemy.String(10),
                        'PERIODO': sqlalchemy.String(10),
                        'OBSERVACION': sqlalchemy.String(10),
                        }

        # Agregar las columnas personalizadas al diccionario
        column_data_types = {}
        column_data_types.update({nombre_columna: tipo_dato for nombre_columna, tipo_dato in custom_data_types.items()})
        db_connection.createTable(database_name, table_name, column_data_types)

        # Agregar las columnas personalizadas al diccionario
        column_data_types_combinado = {}
        column_data_types_combinado.update({nombre_columna: tipo_dato for nombre_columna, tipo_dato in data_combinado.items()})
        db_connection.createTable(database_name, "Combinado", column_data_types_combinado)

        try:
            prediccion.to_sql(name=table_name, con=engine, if_exists='replace', index=False, dtype=custom_data_types)
            combinado.to_sql(name="Combinado", con=engine, if_exists='replace', index=False, dtype=column_data_types_combinado)
            print(f"Datos insertados en la tabla '{table_name}' y en la tabla Combinado de la base de datos '{database_name}'.")
        except Exception as e:
            print(f"Error al insertar datos en la tabla '{table_name}' de la base de datos '{database_name}': {str(e)}")

    else:
        print("No se pudo leer el archivo Excel.")

if __name__ == "__main__":
    main()
