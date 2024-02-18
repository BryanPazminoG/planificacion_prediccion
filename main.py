import subprocess
import time
import os

from modelos_proyeccion_matriculas import main as modelos_proyeccion_matriculas
from modelos_proyeccion_nrc import main as modelos_proyeccion_nrc
from extraccion import main as extraccion_main
from transformacion import main as transformacion_main
from carga import main as carga_main
import configparser


def validar_directorios():
    directorios = [
        "Data",
        "Data/Catalogo",
        "Data/Input",
        "Data/Output",
        "Data/Output/Catalogo",
        "Data/Output/Dataframe",
        "Data/Output/Dataframe Actual",
        "Data/Output/Prediccion",
        "Data/Reporte Actual"
    ]
    directorios_creados = False  # Variable para registrar si se creó al menos un directorio
    for directorio in directorios:
        if not os.path.exists(directorio):
            os.makedirs(directorio)
            print(f"Directorio {directorio} creado.")
            directorios_creados = True  # Marcamos que se creó al menos un directorio

    # Si se creó al menos un directorio, salimos de la ejecución
    if directorios_creados:
        print("Se crearon nuevos directorios.")
        exit()

    print("Todos los directorios ya existían. Continuando con la ejecución...")



            
def ejecutar_funcion(funcion):
    try:
        inicio = time.time()
        funcion()
        fin = time.time()
        tiempo_ejecucion = fin - inicio
        print(f"La función {funcion.__name__} se ejecutó correctamente en {tiempo_ejecucion:.2f} segundos.")
        
    except Exception as error:
        print(f"Error al ejecutar la función {funcion.__name__}: {error}")

def crear_config_ini():
    if not os.path.exists("config.ini"):
        try:
            config = configparser.ConfigParser()
            config['DEFAULT'] = {
                'host': '127.0.0.1',
                'user': 'root',
                'pwd': ''
            }
            with open("config.ini", 'w') as configfile:
                config.write(configfile)
            exit()  # Detener la ejecución en caso de error
        except Exception as e:
            print(f"Error al crear el archivo config.ini: {e}")
            
    else:
        print("El archivo config.ini ya existe.")

if __name__ == "__main__":
    
    crear_config_ini() 
    validar_directorios()
    ejecutar_funcion(extraccion_main)
    ejecutar_funcion(modelos_proyeccion_matriculas)
    ejecutar_funcion(modelos_proyeccion_nrc)
    ejecutar_funcion(transformacion_main)
    ejecutar_funcion(carga_main)
    input("Presiona Enter para salir...")
    