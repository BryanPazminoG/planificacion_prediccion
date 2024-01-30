import subprocess
import time
import os

# Función para ejecutar un script y capturar su tiempo de ejecución
def ejecutar_script(nombre_script):
    try:
        inicio = time.time()
        proceso = subprocess.Popen(['python', nombre_script])
        proceso.wait()  # Esperar a que el proceso termine
        fin = time.time()
        tiempo_ejecucion = fin - inicio
        print(f"El script {nombre_script} se ejecutó correctamente en {tiempo_ejecucion:.2f} segundos.")
        
    except subprocess.CalledProcessError as error:
        print(f"Error al ejecutar el script {nombre_script}: {error}")

# Obtener la ruta absoluta al directorio actual
script_directory = os.getcwd()

# Obtener las rutas absolutas de los archivos etl.py y modelos_proyeccion.py
etl_script_path = os.path.join(script_directory, 'Scripts', 'etl.py')
proyeccion_matriculas_script_path = os.path.join(script_directory, 'Scripts', 'modelos_proyeccion.py')
proyeccion_nrc_script_path = os.path.join(script_directory, 'Scripts', 'modelos_proyeccion_nrc.py')
post_etl_script_path = os.path.join(script_directory, 'Scripts', 'post_etl.py')
dataframe_bd_script_path = os.path.join(script_directory, 'Scripts', 'dataframe_bd.py')

# Ejecutar etl.py
ejecutar_script(etl_script_path)
# Ejecutar modelos_proyeccion.py
ejecutar_script(proyeccion_matriculas_script_path)
# Ejecutar modelos_proyeccion_nrc.py
ejecutar_script(proyeccion_nrc_script_path)
# Ejecutar post_etl.py
ejecutar_script(post_etl_script_path)
# Ejecutar dataframe_bd.py
#ejecutar_script(dataframe_bd_script_path)

print("Procesos completados.")
input("Presiona Enter para salir...")
