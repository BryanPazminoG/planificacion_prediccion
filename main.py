import subprocess
import time
import os

from modelos_proyeccion_matriculas import main as modelos_proyeccion_matriculas
from modelos_proyeccion_nrc import main as modelos_proyeccion_nrc
from extraccion import main as extraccion_main
from transformacion import main as transformacion_main
from carga import main as carga_main



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
    for directorio in directorios:
        if not os.path.exists(directorio):
            os.makedirs(directorio)
            print(f"Directorio {directorio} creado.")
            if "Output" in directorio:
                print("Recuerda subir la información al directorio 'Data/Output'.")
            return False  # Devuelve False si algún directorio no existe
    return True  # Devuelve True si todos los directorios existen

            
def ejecutar_funcion(funcion):
    try:
        inicio = time.time()
        funcion()
        fin = time.time()
        tiempo_ejecucion = fin - inicio
        print(f"La función {funcion.__name__} se ejecutó correctamente en {tiempo_ejecucion:.2f} segundos.")
        
    except Exception as error:
        print(f"Error al ejecutar la función {funcion.__name__}: {error}")

if __name__ == "__main__":
    if not validar_directorios():
        exit
    ejecutar_funcion(extraccion_main)
    ejecutar_funcion(modelos_proyeccion_matriculas)
    ejecutar_funcion(modelos_proyeccion_nrc)
    ejecutar_funcion(transformacion_main)
    ejecutar_funcion(carga_main)
    input("Presiona Enter para salir...")
    