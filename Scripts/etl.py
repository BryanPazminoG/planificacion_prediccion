import os
import pandas as pd
import glob
from fuzzywuzzy import fuzz

class TransformProcess:
    def __init__(self, archivo):
        self.archivo = archivo
        self.df = None
        self.periodo = None
        self.registros_eliminados = []
        
    def cargar_datos(self):
        self.df = pd.read_excel(self.archivo, sheet_name=0)

    def obtener_periodo(self):
        nombre_archivo = os.path.basename(self.archivo)
        periodo = nombre_archivo.split("_")[0]
        self.periodo = periodo
    
    def agregar_periodo(self):
        self.df['PERIODO'] = self.periodo
        
    def eliminar_filas(self):
        self.df = self.df.iloc[3:]
        self.df = self.df.drop(self.df.tail(4).index)
    
    def establecer_encabezados(self):
        nuevos_encabezados = self.df.iloc[0]
        self.df = self.df[1:]
        self.df.columns = nuevos_encabezados
    
    def eliminar_columnas(self):
        self.df = self.df.drop(self.df.columns[[14, 15, 16]], axis=1)
    
    def reemplazar_nan(self):
        self.df.iloc[:, 16:23] = self.df.iloc[:, 16:23].fillna("X")
        # Reemplazar solo los valores numéricos 0 con "X"
        self.df['DOCENTE'] = self.df['DOCENTE'].apply(lambda x: 'X' if str(x).isdigit() and int(x) == 0 else x)
        # Reemplazar solo los valores numéricos 0 con "X"
        self.df['ID DOCENTE'] = self.df['ID DOCENTE'].apply(lambda x: 'X' if str(x).isdigit() and int(x) == 0 else x)
        # Obtener el número de filas antes de la eliminación
        filas_antes = self.df.shape[0]
        # Columnas a eliminar registros NaN
        columnas_eliminar = ['NIVEL FORMACION', 'HI', 'HF']
        # Eliminar filas con NaN en las columnas específicas
        self.df = self.df.dropna(subset=columnas_eliminar)
        # Obtener el número de filas después de la eliminación
        filas_despues = self.df.shape[0]
        # Calcular la cantidad de filas eliminadas
        registros_eliminados = filas_antes - filas_despues
        # Actualizar el contador de registros eliminados
        self.registros_eliminados_reemplazar_nan = registros_eliminados

    def eliminar_filas_parametros(self):
        self.df = self.df.loc[self.df['CAMPUS'] != 'NO ASIGNADO']
        #self.df = self.df.loc[(self.df['STATUS'] != 'I') & (self.df['STATUS'] != 'C')  &  (self.df['PARTE PERIODO'] != '4UC') & (self.df['TIPO'] != 'TUTO')]
        self.df = self.df.loc[(self.df['STATUS'] != 'I') & (self.df['STATUS'] != 'C') & (self.df['PARTE PERIODO'] != '4UC') & (self.df['PARTE PERIODO'] != '4UL') & (self.df['TIPO'] != 'TUTO')]
        self.df = self.df.loc[self.df['TIPO'] != 'TUTO']
        if 'STATUS' in self.df.columns and 'ID DOCENTE' in self.df.columns and 'CÉDULA' in self.df.columns and 'DOCENTE' in self.df.columns:
            self.df = self.df.loc[(self.df['STATUS'] != 'A') | (self.df['ID DOCENTE'] != 0) | (self.df['CÉDULA'] != '0') | (self.df['DOCENTE'] != 0)]
        #self.df = self.df.loc[self.df['NRC'] != self.df['NRC'].shift()]

    def definir_tipos_datos(self):
        columnas_string = {'DEPARTAMENTO', 'CAMPUS', 'ID DOCENTE', 'CÉDULA', 'DOCENTE', 'ÁREA DE CONOCIMIENTO', 'NIVEL FORMACION', 'CODIGO', 'ASIGNATURA', 'PARTE PERIODO', 'STATUS', 'SECCION' }
        columnas_integer = {'NRC', '# CRED', '# EST', 'PERIODO'}
        tipos_datos = {col: str for col in columnas_string}
        tipos_datos.update({col: pd.Int64Dtype() for col in columnas_integer})  # Utilizar el tipo de datos pd.Int64Dtype() para columnas enteras
        tipos_datos['SECCION'] = str  # Especificar el tipo de datos de SECCION como una cadena
        self.df = self.df.astype(tipos_datos, errors='ignore')  # Ignorar los errores de conversión
        columnas_eliminar = ['CÉDULA', 'NIVEL FORMACION', 'PARTE PERIODO',  
                            'S', 'D', 'SECCION']
        self.df = self.df.drop(columns=columnas_eliminar)

class CatalogoEtl:
    def procesar_catalogo(self, input_folder, output_file):
        # Obtener la lista de archivos .xlsx en la carpeta de entrada
        archivos_xlsx = glob.glob(os.path.join(input_folder, '*.xlsx'))

        # Verificar si hay al menos un archivo .xlsx en la carpeta
        if not archivos_xlsx:
            print("No se encontraron archivos .xlsx en la carpeta de entrada.")
            return

        # Seleccionar el primer archivo .xlsx
        input_file = archivos_xlsx[0]
        
        data = pd.read_excel(input_file, skiprows=3, skipfooter=5)

        # Diccionario de mapeo de códigos a descripciones
        mapeo_dptos = {
            'COMP': 'CIENCIAS DE LA COMPUTACION',
            'CADM': 'CIENCIAS ECON. ADMIN. Y COMERC',
            'CESP': 'CIENCIAS ESPACIALES',
            'CHUM': 'CIENCIAS HUMANAS Y SOCIALES',
            'CMED': 'CIENCIAS MEDICAS',
            'CVDA': 'CIENCIAS DE LA VIDA',
            'ELEE': 'ELECTRICA Y ELECTRONICA',
            'EMEC': 'CIENCIAS DE ENERGIA Y MECANICA',
            'EXCT': 'CIENCIAS EXACTAS',
            'LENG': 'LENGUAS',
            'SEGD': 'SEGURIDAD Y DEFENSA',
            'TCON': 'CIENCIAS TIERRA Y CONSTRUCCION'   
        }

        # Reemplazar los valores en la columna 'DPTO' con las descripciones del diccionario
        data['DEPARTAMENTO'] = data['DPTO'].map(mapeo_dptos).fillna(data['DPTO'])\
        
        # Cambiar el nombre de la columna 'TITULO' a 'NUEVO_TITULO'
        data.rename(columns={'TITULO': 'ASIGNATURA'}, inplace=True)

        # Seleccionar las columnas requeridas
        columnas_requeridas = ['DEPARTAMENTO', 'CODIGO', 'ASIGNATURA', 'TEORIA MINIMO', 'TEORIA MAXIMO', 'LABORATORIO MINIMO', 'LABORATORIO MAXIMO']
        data_subset = data[columnas_requeridas]

        # Guardar el archivo Excel con los valores actualizados y las columnas seleccionadas
        data_subset.to_excel(output_file, index=False)


class ValidacionMalla:
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
        #else:
        #    row['CAMPUS'] = 'Malla No Vigente'
        #    return True

    #def realizar_validacion(self):
    #    self.dataframe_combinado['VALIDACION'] = self.dataframe_combinado['CODIGO'].isin(self.catalogo_actualizado['CODIGO'])
        #self.dataframe_combinado.loc[~self.dataframe_combinado['VALIDACION'], 'CAMPUS'] = 'Malla No Vigente'
    #    self.dataframe_combinado = self.dataframe_combinado.drop(columns=['VALIDACION'])
    def realizar_validacion(self):
        self.dataframe_combinado = self.dataframe_combinado[
            self.dataframe_combinado['CODIGO'].isin(self.catalogo_actualizado['CODIGO'])
        ]

    def guardar_resultado(self, output_path='Data/Output/Dataframe/Dataframe_Combinado.xlsx'):
        self.dataframe_combinado.to_excel(output_path, index=False)

class LimpiezaCampus:
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
        ruta_nuevo_archivo = 'Data/Output/Dataframe/Dataframe_Combinado.xlsx'
        data.to_excel(ruta_nuevo_archivo, index=False)



if __name__ == "__main__":
################################################################################################################## ETL COMBINADO
    # Directorio de los archivos Excel
    directorio = "Data/Input"

    # Obtener la lista de archivos Excel en el directorio
    archivos_excel = [archivo for archivo in os.listdir(directorio) if archivo.endswith(".xlsx")]

    # Lista para almacenar los DataFrames procesados
    dataframes_procesados = []
    totalEliminados = 0
    totalInicial = 0

    # Procesar cada archivo Excel
    for archivo_excel in archivos_excel:
        # Ruta completa del archivo
        ruta_archivo = os.path.join(directorio, archivo_excel)

        # Crear una instancia de TransformProcess para procesar el archivo
        procesador = TransformProcess(ruta_archivo) 
        
        # Realizar el procesamiento de los datos
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
        
    # Combinar los DataFrames en uno solo
    df_combinado = pd.concat(dataframes_procesados, ignore_index=True)
    
    # Ruta de la carpeta de resultados
    carpeta_resultados = "Data/Output/Dataframe"
    # Crear la carpeta si no existe
    if not os.path.exists(carpeta_resultados):
        os.makedirs(carpeta_resultados)
    # Nombre completo del archivo de salida con la ruta de la carpeta
    nombre_archivo_salida = os.path.join(carpeta_resultados, "Dataframe_Combinado.xlsx")
    # Exportar el DataFrame combinado como un archivo Excel en la carpeta especificada
    df_combinado.to_excel(nombre_archivo_salida, index=False)
    # Depuración: Imprimir la ruta del archivo de registro
    print(f"Archivo de registro: {nombre_archivo_salida}")


    ################################################################################################################## LIMPIAR CATALOGO
    # Crear una instancia de la clase
    etl_processor = CatalogoEtl()
    # Llamada a la función dentro de la clase
    etl_processor.procesar_catalogo('Data/Catalogo/', 'Data/Output/Catalogo/Catalogo_Actualizado.xlsx')

    ################################################################################################################## ACTUALIZAR CODIGOS SEGUN CATALOGO
    catalogo_path = 'Data/Output/Catalogo/Catalogo_Actualizado.xlsx'
    dataframe_path = 'Data/Output/Dataframe/Dataframe_Combinado.xlsx'
    validador = ValidacionMalla(catalogo_path, dataframe_path)
    validador.realizar_validacion()
    validador.guardar_resultado()

    ################################################################################################################## UNIR SEDES DATAFRAME
    sedes_procesador = LimpiezaCampus()
    ruta_archivo = 'Data/Output/Dataframe/Dataframe_Combinado.xlsx'
    sedes_procesador.reemplazar_valores_campus(ruta_archivo)
