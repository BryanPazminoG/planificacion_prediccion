import pandas as pd
import os
import glob
from etl import TransformProcess, LimpiezaCampus
from fuzzywuzzy import fuzz

class DataMergerCalculator:
    def __init__(self):
        pass

    def merge_files(self, estudiantes_path, nrc_path, output_path='Data/Output/Prediccion/Combinado_Predicciones.xlsx'):
        # Leer los archivos Excel
        archivo_estudiantes = pd.read_excel(estudiantes_path)
        archivo_nrc = pd.read_excel(nrc_path)

        # Fusionar los datos según ASIGNATURA, CAMPUS, DEPARTAMENTO, CODIGO
        merged_df = pd.merge(archivo_estudiantes, archivo_nrc, on=['ASIGNATURA', 'CAMPUS', 'DEPARTAMENTO', 'CODIGO'], how='inner')
        # Verificar y agregar la columna de Estudiantes o NRC a uno de los dataframes combinados
        # Por ejemplo, agregando la columna de NRC al dataframe combinado
        merged_df['NRC_PREDICHOS'] = merged_df['NRC_PREDICHOS_RF']  # Cambiar por la columna deseada

        # Guardar el resultado en un nuevo archivo Excel
        merged_df.to_excel(output_path, index=False)
        # Eliminar los archivos originales si todo sale bien
        try:
            os.remove(estudiantes_path)
            os.remove(nrc_path)
            print("Archivos originales eliminados exitosamente.")
        except Exception as e:
            print(f"Error al eliminar archivos originales: {e}")

    def merge_and_calculate(self, input_predicciones_path, input_catalogo_path, output_path='Data/Output/Prediccion/Dataframe_Calculo.xlsx'):
            # Leer los archivos Excel
            archivo_predicciones = pd.read_excel(input_predicciones_path)
            archivo_catalogo = pd.read_excel(input_catalogo_path)

            # Fusionar los datos según DEPARTAMENTO, CODIGO, ASIGNATURA
            merged_data = pd.merge(archivo_predicciones, archivo_catalogo, on=['DEPARTAMENTO', 'CODIGO', 'ASIGNATURA'], how='inner')

            # Definir una función para realizar la multiplicación condicional y obtener la observación
            def calcular_multiplicacion_fila(row):
                if row['TEORIA MINIMO'] == 0 or row['LABORATORIO MINIMO'] == 0:
                    observacion = 'MAX'
                    total_horas = row['TEORIA MAXIMO'] + row['LABORATORIO MAXIMO']
                    if total_horas > 4:
                        observacion = 'Division'
                    horas = row['NRC_PREDICHOS_RF'] * total_horas
                else:
                    observacion = 'MIN'
                    horas = row['NRC_PREDICHOS_RF'] * (row['TEORIA MINIMO'] + row['LABORATORIO MINIMO'])
                return horas, observacion

            # Aplicar la función de cálculo a las columnas correspondientes y obtener las observaciones
            merged_data['HORAS_RL'], merged_data['OBSERVACION_RL'] = zip(*merged_data.apply(lambda row: calcular_multiplicacion_fila(row), axis=1))
            merged_data['HORAS_SE'], merged_data['OBSERVACION_SE'] = zip(*merged_data.apply(lambda row: calcular_multiplicacion_fila(row), axis=1))
            merged_data['HORAS_AD'], merged_data['OBSERVACION_AD'] = zip(*merged_data.apply(lambda row: calcular_multiplicacion_fila(row), axis=1))

            merged_data = merged_data.rename(columns={
                'ESTUDIANTES_PREDICHOS_LR': 'ESTUDIANTES_RL',
                'ESTUDIANTES_PREDICHOS_EXPONENCIAL': 'ESTUDIANTES_SE',
                'ESTUDIANTES_PREDICHOS_DT': 'ESTUDIANTES_AD',
                'NRC_PREDICHOS_RF': 'NRC_RL',
                'NRC_PREDICHOS_EXPONENCIAL': 'NRC_SE',
                'NRC_PREDICHOS_DT': 'NRC_AD',
            })
            
            division_condition = (merged_data['OBSERVACION_AD'] == 'Division') | (merged_data['OBSERVACION_SE'] == 'Division') | (merged_data['OBSERVACION_RL'] == 'Division')
            merged_data.loc[division_condition, 'ESTUDIANTES_RL'] /= 2
            merged_data.loc[division_condition, 'ESTUDIANTES_SE'] /= 2
            merged_data.loc[division_condition, 'ESTUDIANTES_AD'] /= 2
            merged_data.loc[division_condition, 'CAMPUS'] = 'Division'
            
            # Seleccionar las columnas deseadas
            result_data = merged_data[['CAMPUS', 'DEPARTAMENTO', 'ASIGNATURA', 'ÁREA DE CONOCIMIENTO', 'CODIGO', 
                                    'ESTUDIANTES_RL', 'ESTUDIANTES_SE', 'ESTUDIANTES_AD', 
                                    'NRC_RL', 'NRC_SE', 'NRC_AD', 
                                    'HORAS_RL', 'HORAS_SE', 'HORAS_AD',
                                    'OBSERVACION_RL', 'OBSERVACION_SE', 'OBSERVACION_AD']]
            
            # Validar antes de guardar el resultado en un nuevo archivo Excel
            if division_condition.any():
                print("Se encontraron divisiones, realizando ajustes adicionales.")
                
            # Guardar el resultado en un nuevo archivo Excel
            result_data.to_excel(output_path, index=False)
            # Eliminar el archivo original de predicciones si todo sale bien
            try:
                os.remove(input_predicciones_path)
                print("Archivo original de predicciones eliminado exitosamente.")
            except Exception as e:
                print(f"Error al eliminar el archivo original de predicciones: {e}")

class ValidacionMallaActual:
    def __init__(self, catalogo_path, dataframe_path):
        # Leer los archivos Excel y cargar los datos en DataFrames
        self.catalogo_actualizado = pd.read_excel(catalogo_path)
        self.dataframe_combinado = pd.read_excel(dataframe_path)

    def calcular_similitud(self, texto_a, texto_b):
        if pd.isnull(texto_a) or pd.isnull(texto_b):
            return 0
        return fuzz.ratio(str(texto_a).lower(), str(texto_b).lower())

    def filtrar_filas(self, row):
        codigo = row['CODIGO']
        asignatura = row['ASIGNATURA']
        departamento = row['DEPARTAMENTO']

        catalogo_filtrado = self.catalogo_actualizado[
            (self.catalogo_actualizado['CODIGO'] == codigo) &
            (self.catalogo_actualizado['ASIGNATURA'].apply(lambda x: self.calcular_similitud(asignatura, x)) >= 80) &
            (self.catalogo_actualizado['ASIGNATURA'].str.lower() == asignatura.lower()) &
            (self.catalogo_actualizado['DEPARTAMENTO'] == departamento)
        ]

        if not catalogo_filtrado.empty:
            return True
        else:
            row['CAMPUS'] = 'Malla No Vigente'
            return True

    def realizar_validacion(self):
        self.dataframe_combinado['VALIDACION'] = self.dataframe_combinado['CODIGO'].isin(self.catalogo_actualizado['CODIGO'])
        self.dataframe_combinado.loc[~self.dataframe_combinado['VALIDACION'], 'CAMPUS'] = 'Malla No Vigente'
        self.dataframe_combinado = self.dataframe_combinado.drop(columns=['VALIDACION'])

    def guardar_resultado(self, output_path='Data/Output/Dataframe Actual/Dataframe_Actual.xlsx'):
        self.dataframe_combinado.to_excel(output_path, index=False)
        
    def reemplazar_valores_campus(self, ruta_archivo):
        # Cargar el archivo Excel
        data = pd.read_excel(ruta_archivo)

        # Mostrar los valores únicos en la columna 'CAMPUS'
        print("Valores únicos antes del reemplazo:")
        print(data['CAMPUS'].unique())

        # Reemplazar los valores específicos
        data['CAMPUS'] = data['CAMPUS'].replace(['ESPE LTGA-G RODRIGUEZ LARA', 'ESPE SEDE LATACUNGA CENTRO'], 'ESPE SEDE LATACUNGA')

        # Eliminar los campus específicos
        campus_a_eliminar = ['TEC. AERONAUTICA LTGA (UGT)', 'ESMA - SALINAS', 'ESPE A DISTANCIA']
        data = data[~data['CAMPUS'].isin(campus_a_eliminar)]
        
        # Guardar los cambios en un nuevo archivo Excel
        ruta_nuevo_archivo = 'Data/Output/Dataframe Actual/Dataframe_Actual.xlsx'
        data.to_excel(ruta_nuevo_archivo, index=False)
        
        
class EtlFinal:
    def filtrar_y_guardar_excel(self, ruta_archivo, columna_deseada, valores_a_filtrar, archivo_salida):
        # Cargar el archivo Excel en un DataFrame
        df = pd.read_excel(ruta_archivo)

        # Manejar valores NaN en la columna deseada antes de aplicar el filtro
        df[columna_deseada].fillna('', inplace=True)

        # Filtrar las filas que NO contengan ciertos valores en la columna deseada
        filtro = ~df[columna_deseada].str.contains(valores_a_filtrar, case=False, na=False)

        # Aplicar el filtro al DataFrame original para obtener los datos que no cumplen la condición
        resultados = df[filtro]

        # Guardar el resultado en un nuevo archivo Excel
        resultados.to_excel(archivo_salida, index=False)


def modificar_excel(archivo_excel):
    # Lee el archivo Excel
    df = pd.read_excel(archivo_excel)

    # Crea una nueva columna llamada 'CODIGO_ASIGNATURA' que contiene la concatenación de 'CODIGO' y 'ASIGNATURA'
    df['CODIGO_ASIGNATURA'] = df['CODIGO'] + ' - ' + df['ASIGNATURA']

    # Elimina las columnas originales 'CODIGO' y 'ASIGNATURA'
    df = df.drop(['CODIGO', 'ASIGNATURA'], axis=1)

    # Reordena las columnas según tu preferencia
    column_order = [
        'DEPARTAMENTO', 'CAMPUS', 'ÁREA DE CONOCIMIENTO', 'CODIGO_ASIGNATURA',
        'NRC', 'STATUS', '# EST', 'HI',	'HF', 'L', 'M', 'I', 'J', 'V', 'TIPO', 'PERIODO', 'OBSERVACION'
    ]
    

    df = df[column_order]

    # Guarda el DataFrame de vuelta en un nuevo archivo Excel o en el mismo archivo, según sea necesario
    nuevo_archivo = archivo_excel.replace('.xlsx', '.xlsx')
    df.to_excel(nuevo_archivo, index=False)

    print(f"Proceso completado. El archivo modificado se ha guardado como '{nuevo_archivo}'")


def crear_columna_codigo_asignatura(archivo_excel):
    # Lee el archivo Excel
    df = pd.read_excel(archivo_excel)

    # Crea una nueva columna llamada 'CODIGO_ASIGNATURA' que contiene la concatenación de 'CODIGO' y 'ASIGNATURA'
    df['CODIGO_ASIGNATURA'] = df['CODIGO'] + ' - ' + df['ASIGNATURA']

    # Guarda el DataFrame de vuelta en un nuevo archivo Excel o en el mismo archivo, según sea necesario
    nuevo_archivo = archivo_excel.replace('.xlsx', '.xlsx')
    df.to_excel(nuevo_archivo, index=False)

    print(f"Proceso completado. El archivo modificado se ha guardado como '{nuevo_archivo}'")

def cargar_y_procesar_datos(ruta_excel):
    # Cargar datos desde el archivo Excel
    df_combinado = pd.read_excel(ruta_excel)

    # Convierte la columna 'HI' a cadena y luego aplica zfill(4)
    df_combinado['HI'] = df_combinado['HI'].astype(str).str.zfill(4)
    # Convierte la columna 'HF' a cadena y luego aplica zfill(4)
    df_combinado['HF'] = df_combinado['HF'].astype(str).str.zfill(4)

    # Convertir las columnas 'HI' y 'HF' al formato datetime
    df_combinado['HI'] = pd.to_datetime(df_combinado['HI'], format='%H%M')
    df_combinado['HF'] = pd.to_datetime(df_combinado['HF'], format='%H%M')

    # Calcular la diferencia entre las horas HI y HF en horas y crear la columna 'DIF_HORAS'
    df_combinado['HORA_DIA'] = (df_combinado['HF'] - df_combinado['HI']).dt.total_seconds() / 3600

    # Aproximar los valores en la columna 'RESULTADO' a un solo valor entero
    df_combinado['HORA_DIA'] = df_combinado['HORA_DIA'].round(1).astype(int)

    # Calcular el número de días que no contienen la letra 'X' en las columnas L, M, I, J, y V y crear la columna 'NUM_DIAS'
    df_combinado['NUM_DIAS'] = df_combinado[['L', 'M', 'I', 'J', 'V']].apply(lambda row: row.str.count('X').eq(0).sum(), axis=1)

    # Multiplicar la columna 'DIF_HORAS' por la columna 'NUM_DIAS' y crear la columna 'RESULTADO'
    df_combinado['HORAS'] = df_combinado['HORA_DIA'] * df_combinado['NUM_DIAS']

    # Aproximar los valores en la columna 'RESULTADO' a un solo valor entero
    df_combinado['HORAS'] = df_combinado['HORAS'].round(0).astype(int)

    # Mostrar solo la hora en las columnas 'HI' y 'HF'
    df_combinado['HI'] = df_combinado['HI'].dt.time
    df_combinado['HF'] = df_combinado['HF'].dt.time

    # Lista con el nuevo orden de las columnas
    nuevo_orden = [
        'DEPARTAMENTO', 'CAMPUS', 'ÁREA DE CONOCIMIENTO', 'CODIGO_ASIGNATURA',
        'NRC', 'STATUS', '# EST', 'HI', 'HF', 'L', 'M', 'I', 'J', 'V', 'HORA_DIA',
        'NUM_DIAS', 'HORAS', 'TIPO', 'PERIODO', 'OBSERVACION'
    ]
    


    # Cambiar el orden de las columnas en el DataFrame 'df_combinado'
    df_combinado = df_combinado.reindex(columns=nuevo_orden)
    
    return df_combinado



####################################### UNIR PREDICCIONES
data_merger_calculator = DataMergerCalculator()
estudiantes_file_path = 'Data/Output/Prediccion/Prediccion_Matriculas.xlsx'
nrc_file_path = 'Data/Output/Prediccion/Predicciones_NRC.xlsx'
data_merger_calculator.merge_files(estudiantes_file_path, nrc_file_path)
####################################### CALCULAR PREDICCIONES MAXIMOS Y MINIMOS
input_predicciones_file = 'Data/Output/Prediccion/Combinado_Predicciones.xlsx'
input_catalogo_file = 'Data/Output/Catalogo/Catalogo_Actualizado.xlsx'
data_merger_calculator.merge_and_calculate(input_predicciones_file, input_catalogo_file)


####################################### PROCESAR REPORTE ACTUAL
ruta_carpeta = "Data/Reporte Actual/"
patron_archivos = "*.xlsx"
ruta_archivos_xlsx = glob.glob(ruta_carpeta + patron_archivos)
dataframes_procesados = []

for archivo_xlsx in ruta_archivos_xlsx:
    # Validar si el archivo existe
    if os.path.exists(archivo_xlsx):
        procesador = TransformProcess(archivo_xlsx)
        procesador.cargar_datos()
        procesador.obtener_periodo()
        procesador.eliminar_filas()
        procesador.establecer_encabezados()
        procesador.eliminar_columnas()
        procesador.reemplazar_nan()
        procesador.eliminar_filas_parametros()
        procesador.agregar_periodo()
        procesador.definir_tipos_datos()
        dataframes_procesados.append(procesador.df)
    else:
        print(f"El archivo {archivo_xlsx} no existe. No se realizará el procesamiento para este archivo.")
        
if dataframes_procesados:
    df_combinado = pd.concat(dataframes_procesados, ignore_index=True)
    # Ruta de la carpeta de resultados
    carpeta_resultados = "Data/Output/Dataframe Actual"
    # Crear la carpeta si no existe
    if not os.path.exists(carpeta_resultados):
        os.makedirs(carpeta_resultados)
    # Nombre completo del archivo de salida con la ruta de la carpeta
    nombre_archivo_salida = os.path.join(carpeta_resultados, "Dataframe_Actual.xlsx")
    # Exportar el DataFrame combinado como un archivo Excel en la carpeta especificada
    df_combinado.to_excel(nombre_archivo_salida, index=False)
    # Depuración: Imprimir la ruta del archivo de registro
    print(f"Archivo de registro: {nombre_archivo_salida}")
else:
    print("No hay archivos para procesar.")


####################################### COMPARAR REPORTE ACTUAL CON CATALOGO 
catalogo_path = 'Data/Output/Catalogo/Catalogo_Actualizado.xlsx'
dataframe_path = 'Data/Output/Dataframe Actual/Dataframe_Actual.xlsx'
# Validar si el archivo del DataFrame existe
if os.path.exists(dataframe_path):
    validador = ValidacionMallaActual(catalogo_path, dataframe_path)
    validador.realizar_validacion()
    validador.guardar_resultado()
    validador.reemplazar_valores_campus(dataframe_path)
else:
    print(f"El archivo {dataframe_path} no existe. No se realizarán las operaciones.")


####################################### COMPARAR CON CATALOGO
df_actual = pd.read_excel('Data/Output/Dataframe/Dataframe_Combinado.xlsx')
df_catalogo = pd.read_excel('Data/Output/Catalogo/Catalogo_Actualizado.xlsx')
# Filtrar las columnas necesarias en ambos DataFrames
df_actual = df_actual[['DEPARTAMENTO', 'CAMPUS', 'ÁREA DE CONOCIMIENTO', 'CODIGO', 'ASIGNATURA', 'NRC', 'STATUS', '# EST', 'HI', 'HF', 'L', 'M', 'I', 'J', 'V', 'TIPO', 'PERIODO']]
df_catalogo = df_catalogo[['DEPARTAMENTO', 'CODIGO', 'ASIGNATURA', 'TEORIA MINIMO', 'TEORIA MAXIMO', 'LABORATORIO MINIMO', 'LABORATORIO MAXIMO']]

# Combinar DataFrames por las columnas DEPARTAMENTO, CODIGO y ASIGNATURA
df_merged = pd.merge(df_actual, df_catalogo, on=['DEPARTAMENTO', 'CODIGO', 'ASIGNATURA'], how='inner')

# Definir una función para calcular las HORAS y la OBSERVACION según la lógica especificada
def calcular_horas_y_observacion(row):
    if row['CAMPUS'] == 'ESPE EN LINEA':
        return row['# EST'] / 11, 'En linea'
    elif row['TEORIA MINIMO'] == 0 or row['LABORATORIO MINIMO'] == 0:
        total_horas = row['TEORIA MAXIMO'] + row['LABORATORIO MAXIMO']
        if total_horas > 4:
            return total_horas, 'Division'
        return total_horas, 'MAX'
    else:
        return row['TEORIA MINIMO'] + row['LABORATORIO MINIMO'], 'MIN'
# Aplicar la función a las nuevas columnas 'HORAS' y 'OBSERVACION'
df_merged['HORAS'], df_merged['OBSERVACION'] = zip(*df_merged.apply(calcular_horas_y_observacion, axis=1))
# Eliminar las columnas que ya no son necesarias
df_actual = df_merged[['DEPARTAMENTO', 'CAMPUS', 'ÁREA DE CONOCIMIENTO', 'CODIGO', 'ASIGNATURA', 'NRC', 'STATUS', '# EST', 'HI', 'HF', 'L', 'M', 'I', 'J', 'V', 'TIPO', 'PERIODO']]
df_actual.loc[:, 'HORAS'] = df_merged['HORAS'].values
# Crear una copia explícita del DataFrame antes de realizar la asignación
df_actual.loc[:, 'OBSERVACION'] = df_merged['OBSERVACION'].values
division_condition = (df_actual['OBSERVACION'] == 'Division')
df_actual.loc[division_condition, 'CAMPUS'] = 'Division'

# Guardar el resultado en el mismo archivo Excel sobrescribiendo el original
df_actual.to_excel("Data/Output/Dataframe/Dataframe_Combinado.xlsx", index=False)

####################################### UNIR DATAFRAMES COMBINADO Y ACTUAL 
df_actual = pd.read_excel("Data/Output/Dataframe Actual/Dataframe_Actual.xlsx")
df_combinado = pd.read_excel("Data/Output/Dataframe/Dataframe_Combinado.xlsx")
# Concatenar los DataFrames
df_resultado = pd.concat([df_combinado, df_actual])
# Dividir # EST por 11 en los registros donde CAMPUS es igual a "En Linea"
df_resultado.loc[df_resultado['CAMPUS'] == 'ESPE EN LINEA', '# EST'] /= 11
# Guardar el resultado en un nuevo archivo Excel
df_resultado.to_excel("Data/Output/Dataframe/Dataframe_Combinado.xlsx", index=False)

###################
# Ejemplo de uso de la clase
etl = EtlFinal()
etl.filtrar_y_guardar_excel('Data\Output\Dataframe\Dataframe_Combinado.xlsx', 'ASIGNATURA', 'Profesionalizante|Complex', 'Data\Output\Dataframe\Dataframe_Combinado.xlsx')
etl.filtrar_y_guardar_excel('Data\Output\Prediccion\Dataframe_Calculo.xlsx', 'ASIGNATURA', 'Profesionalizante|Complex', 'Data\Output\Prediccion\Dataframe_Calculo.xlsx')
etl.filtrar_y_guardar_excel('Data\Output\Prediccion\Dataframe_Calculo.xlsx', 'CAMPUS', 'ESPE EN LINEA', 'Data\Output\Prediccion\Dataframe_Calculo.xlsx')

##################
modificar_excel('Data\Output\Dataframe\Dataframe_Combinado.xlsx')
crear_columna_codigo_asignatura('Data\Output\Prediccion\Dataframe_Calculo.xlsx')

################### CALCULO DE HORAS
df_combinado = cargar_y_procesar_datos('Data\Output\Dataframe\Dataframe_Combinado.xlsx')
df_combinado.to_excel('Data\Output\Dataframe\Dataframe_Combinado.xlsx', index=False)