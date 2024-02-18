import pandas as pd
import os
from fuzzywuzzy import fuzz
import glob
from extraccion import TransformProcess

class PrediccionesManager:
    def __init__(self):
        self.ruta_combinado_predicciones = "Data/Output/Prediccion/Combinado_Predicciones.xlsx"
        self.ruta_combinado_calculo='Data/Output/Prediccion/Dataframe_Predicciones_Calculo.xlsx'

    def unir_archivos_predicciones(self, matriculas_ruta, nrc_ruta):
        archivo_estudiantes = pd.read_excel(matriculas_ruta)
        archivo_nrc = pd.read_excel(nrc_ruta)
        merged_df = pd.merge(archivo_estudiantes, archivo_nrc, on=['ASIGNATURA', 'CAMPUS', 'DEPARTAMENTO', 'CODIGO'], how='inner')
        merged_df.to_excel(self.ruta_combinado_predicciones, index=False)
        try:
            #os.remove(matriculas_ruta)
            #os.remove(nrc_ruta)
            print("Archivos originales eliminados exitosamente.")
        except Exception as e:
            print(f"Error al eliminar archivos originales: {e}")
            

    def calcular_predicciones_catalogo(self, ruta_predicciones, ruta_catalogo):
            archivo_predicciones = pd.read_excel(ruta_predicciones)
            archivo_catalogo = pd.read_excel(ruta_catalogo)
            merged_data = pd.merge(archivo_predicciones, archivo_catalogo, on=['DEPARTAMENTO', 'CODIGO', 'ASIGNATURA'], how='inner')

            def calcular_multiplicacion_fila(row):
                if row['TEORIA MINIMO'] == 0 and row['LABORATORIO MINIMO'] == 0 and row['TEORIA MAXIMO'] == 0 and row['LABORATORIO MAXIMO'] == 0:
                    observacion = 'CERO'
                    horas = 0
                elif row['TEORIA MINIMO'] == 0 and row['LABORATORIO MINIMO'] == 0:
                    observacion = 'MAX'
                    total_horas = row['TEORIA MAXIMO'] + row['LABORATORIO MAXIMO']
                    if total_horas >= 4:
                        observacion = 'MAX4'
                    horas = row['NRC_PREDICHOS_RF'] * total_horas
                elif row['TEORIA MAXIMO'] == 0 and row['LABORATORIO MAXIMO'] == 0:
                    observacion = 'MIN'
                    horas = row['NRC_PREDICHOS_RF'] * (row['TEORIA MINIMO'] + row['LABORATORIO MINIMO'])
                else:
                    observacion = 'CERO'
                    horas = 0
                return horas, observacion

            # Aplicar la función de cálculo a las columnas correspondientes y obtener las observaciones
            merged_data['HORAS_SE'], merged_data['OBSERVACION_SE'] = zip(*merged_data.apply(lambda row: calcular_multiplicacion_fila(row), axis=1))

            merged_data = merged_data.rename(columns={
                'ESTUDIANTES_PREDICHOS_LR': 'ESTUDIANTES_RL',
                'ESTUDIANTES_PREDICHOS_EXPONENCIAL': 'ESTUDIANTES_SE',
                'ESTUDIANTES_PREDICHOS_DT': 'ESTUDIANTES_AD',
                'NRC_PREDICHOS_RF': 'NRC_RL',
                'NRC_PREDICHOS_EXPONENCIAL': 'NRC_SE',
                'NRC_PREDICHOS_DT': 'NRC_AD',
            })

            # Agrega la condición adicional para el 'DEPARTAMENTO' específico
            division_condition = (merged_data['OBSERVACION_SE'] == 'MAX4') & (merged_data['CAMPUS'] == 'ESPE MATRIZ SANGOLQUI')
                                #(merged_data['OBSERVACION_RL'] == 'Division')) & (merged_data['DEPARTAMENTO'] == 'CIENCIAS DE ENERGIA Y MECANICA') & (merged_data['CAMPUS'] == 'ESPE MATRIZ SANGOLQUI')

            merged_data.loc[division_condition, 'ESTUDIANTES_RL'] /= 2
            merged_data.loc[division_condition, 'ESTUDIANTES_SE'] /= 2
            merged_data.loc[division_condition, 'ESTUDIANTES_AD'] /= 2
            merged_data.loc[division_condition, 'CAMPUS'] = 'Campus Experimental'
            merged_data.loc[division_condition, 'OBSERVACION_SE'] = 'Division'
            
            
            # Reemplazar los valores 'MAX4' en la columna 'OBSERVACION_SE' por 'MAX'
            merged_data['OBSERVACION_SE'] = merged_data['OBSERVACION_SE'].replace('MAX4', 'MAX')


            result_data = merged_data[['CAMPUS', 'DEPARTAMENTO', 'ASIGNATURA', 'ÁREA DE CONOCIMIENTO', 'CODIGO', 'ESTUDIANTES_RL', 'ESTUDIANTES_SE', 'ESTUDIANTES_AD', 'NRC_RL', 'NRC_SE', 'NRC_AD', 'HORAS_SE', 'OBSERVACION_SE']]

            if division_condition.any():
                print("Se encontraron divisiones, realizando ajustes adicionales.")

            result_data.to_excel(self.ruta_combinado_calculo, index=False)
            try:
                os.remove(ruta_predicciones)
                print("Archivo original de predicciones eliminado exitosamente.")
            except Exception as e:
                print(f"Error al eliminar el archivo original de predicciones: {e}")


class ActualManager:
    def __init__(self):
        self.ruta_carpeta_actual = "Data/Reporte Actual/"
        self.carpeta_resultados = "Data/Output/Dataframe Actual"
        
    def procesar_reporte_actual(self):
        ruta_archivos_xlsx = glob.glob(self.ruta_carpeta_actual + "*.xlsx")
        dataframes_procesados = []

        for archivo_xlsx in ruta_archivos_xlsx:
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
            if not os.path.exists(self.carpeta_resultados):
                os.makedirs(self.carpeta_resultados)
            nombre_archivo_salida = os.path.join(self.carpeta_resultados, "Dataframe_Actual.xlsx")
            df_combinado.to_excel(nombre_archivo_salida, index=False)
            print(f"Archivo de registro: {nombre_archivo_salida}")
            self.realizar_validacion()
        else:
            print("No hay archivos para procesar.")
            
    def realizar_validacion(self):
        ruta_dataframe_actual = "Data/Output/Dataframe Actual/Dataframe_Actual.xlsx"
        if os.path.exists(ruta_dataframe_actual):
            catalogo_actualizado = pd.read_excel("Data/Output/Catalogo/Catalogo_Actualizado.xlsx")
            dataframe_combinado = pd.read_excel(ruta_dataframe_actual)
            dataframe_combinado['VALIDACION'] = dataframe_combinado['CODIGO'].isin(catalogo_actualizado['CODIGO'])
            dataframe_combinado.loc[~dataframe_combinado['VALIDACION'], 'CAMPUS'] = 'Malla No Vigente'
            dataframe_combinado.loc[~dataframe_combinado['VALIDACION'], 'OBSERVACION'] = 'No Vigente'
            dataframe_combinado = dataframe_combinado.drop(columns=['VALIDACION'])
            dataframe_combinado['CAMPUS'] = dataframe_combinado['CAMPUS'].replace(['ESPE LTGA-G RODRIGUEZ LARA', 'ESPE SEDE LATACUNGA CENTRO'], 'ESPE SEDE LATACUNGA')
            campus_a_eliminar = ['TEC. AERONAUTICA LTGA (UGT)', 'ESMA - SALINAS', 'ESPE A DISTANCIA']
            dataframe_combinado = dataframe_combinado[~dataframe_combinado['CAMPUS'].isin(campus_a_eliminar)]
            dataframe_combinado.to_excel(ruta_dataframe_actual, index=False)
        else:
            print(f"El archivo {ruta_dataframe_actual} no existe. No se realizarán las operaciones.")


class DataframesManager:
    def __init__(self):
        self.dataframe_combinado = "Data/Output/Dataframe/Dataframe_Combinado.xlsx"
        self.catalogo = "Data\Output\Catalogo\Catalogo_Actualizado.xlsx"

    def unir_y_procesar_dataframes(self):
        ruta_actual = "Data/Output/Dataframe Actual/Dataframe_Actual.xlsx"
        if os.path.exists(ruta_actual):
            df_actual = pd.read_excel(ruta_actual)
            df_combinado = pd.read_excel(self.dataframe_combinado)
            df_resultado = pd.concat([df_combinado, df_actual])
            malla_no_vigente = df_resultado[df_resultado['CAMPUS'] == 'Malla No Vigente']
            df_resultado.to_excel(self.dataframe_combinado, index=False)
            return malla_no_vigente
        else:
            print(f"El archivo actual no existe. No se realizarán las operaciones.")

    def calcular_horas_y_observacion(self, row):
        if row['TEORIA MINIMO'] == 0 and row['LABORATORIO MINIMO'] == 0:
            total_horas = row['TEORIA MAXIMO'] + row['LABORATORIO MAXIMO']
            if total_horas >= 4:
                return total_horas, 'MAX4'
            return total_horas, 'MAX'
        elif row['TEORIA MAXIMO'] == 0 and row['LABORATORIO MAXIMO'] == 0:
            return row['TEORIA MINIMO'] + row['LABORATORIO MINIMO'], 'MIN'
    
    def comparar_catalogo(self, no_vigente):
        ruta_dataframe_combinado = "Data/Output/Dataframe/Dataframe_Combinado.xlsx"
        df_actual = pd.read_excel(ruta_dataframe_combinado)
        df_catalogo = pd.read_excel(self.catalogo)
        df_actual = df_actual[['DEPARTAMENTO', 'CAMPUS', 'ÁREA DE CONOCIMIENTO', 'CODIGO', 'ASIGNATURA', 'NRC', 'STATUS', '# EST', 'HI', 'HF', 'L', 'M', 'I', 'J', 'V', 'TIPO', 'PERIODO']]
        df_catalogo = df_catalogo[['DEPARTAMENTO', 'CODIGO', 'ASIGNATURA', 'TEORIA MINIMO', 'TEORIA MAXIMO', 'LABORATORIO MINIMO', 'LABORATORIO MAXIMO']]
        df_merged = pd.merge(df_actual, df_catalogo, on=['DEPARTAMENTO', 'CODIGO', 'ASIGNATURA'], how='inner')

        df_merged['HORAS'], df_merged['OBSERVACION'] = zip(*df_merged.apply(self.calcular_horas_y_observacion, axis=1))
        df_actual = df_merged[['DEPARTAMENTO', 'CAMPUS', 'ÁREA DE CONOCIMIENTO', 'CODIGO', 'ASIGNATURA', 'NRC', 'STATUS', '# EST', 'HI', 'HF', 'L', 'M', 'I', 'J', 'V', 'TIPO', 'PERIODO']]
        
        # Avoid chained assignments
        df_actual = df_actual.copy()
        df_actual.loc[:, 'HORAS'] = df_merged['HORAS'].values
        df_actual.loc[:, 'OBSERVACION'] = df_merged['OBSERVACION'].values
        division_condition = (df_actual['OBSERVACION'] == 'MAX4') & (df_actual['CAMPUS'] == 'ESPE MATRIZ SANGOLQUI')
        df_actual.loc[division_condition, 'CAMPUS']  = 'Campus Experimental'
        df_actual.loc[division_condition, 'OBSERVACION']  = 'Division'
        
        df_actual['OBSERVACION'] = df_actual['OBSERVACION'].replace('MAX4', 'MAX')

        if no_vigente is not None:
            if not isinstance(no_vigente, pd.DataFrame):
                raise ValueError("El argumento 'no_vigente' debe ser un DataFrame de pandas.")

            columnas_a_eliminar = ['ID DOCENTE', 'DOCENTE', '# CRED']
            no_vigente = no_vigente.drop(columnas_a_eliminar, axis=1)
            df_actual = pd.concat([df_actual, no_vigente], ignore_index=True)
        
        df_actual.to_excel(ruta_dataframe_combinado, index=False)
        
class Calculos:
    def filtrar_y_guardar_excel(self, ruta_archivo, columna_deseada, valores_a_filtrar, archivo_salida):
        df = pd.read_excel(ruta_archivo)
        df[columna_deseada].fillna('', inplace=True)
        filtro = ~df[columna_deseada].str.contains(valores_a_filtrar, case=False, na=False)
        resultados = df[filtro]
        resultados.to_excel(archivo_salida, index=False)

    def modificar_excel(self, archivo_excel):
        df = pd.read_excel(archivo_excel)
        df['CODIGO_ASIGNATURA'] = df['CODIGO'] + ' - ' + df['ASIGNATURA']
        column_order = [
            'DEPARTAMENTO', 'CAMPUS', 'ÁREA DE CONOCIMIENTO', 'CODIGO_ASIGNATURA',
            'NRC', 'STATUS', '# EST', 'HI',	'HF', 'L', 'M', 'I', 'J', 'V', 'TIPO', 'PERIODO', 'OBSERVACION'
        ]
        df = df[column_order]
        nuevo_archivo = archivo_excel.replace('.xlsx', '.xlsx')
        df.to_excel(nuevo_archivo, index=False)
        print(f"Proceso completado. El archivo modificado se ha guardado como '{nuevo_archivo}'")

    def crear_columna_codigo_asignatura(self, archivo_excel):
        df = pd.read_excel(archivo_excel)
        df['CODIGO_ASIGNATURA'] = df['CODIGO'] + ' - ' + df['ASIGNATURA']
        nuevo_archivo = archivo_excel.replace('.xlsx', '.xlsx')
        df.to_excel(nuevo_archivo, index=False)
        print(f"Proceso completado. El archivo modificado se ha guardado como '{nuevo_archivo}'")
        
    def cargar_y_procesar_datos(self, ruta_combinado):
        df_combinado = pd.read_excel(ruta_combinado)
        df_combinado['HI'] = df_combinado['HI'].astype(str).str.zfill(4)
        df_combinado['HF'] = df_combinado['HF'].astype(str).str.zfill(4)
        df_combinado['HI'] = pd.to_datetime(df_combinado['HI'], format='%H%M')
        df_combinado['HF'] = pd.to_datetime(df_combinado['HF'], format='%H%M')
        df_combinado['HORA_DIA'] = (df_combinado['HF'] - df_combinado['HI']).dt.total_seconds() / 3600
        df_combinado['HORA_DIA'] = df_combinado['HORA_DIA'].round(1).astype(int)
        df_combinado['NUM_DIAS'] = df_combinado[['L', 'M', 'I', 'J', 'V']].apply(lambda row: row.str.count('X').eq(0).sum(), axis=1)
        
        # Agregar validación para 'CAMPUS' igual a 'ESPE EN LINEA'
        mask = df_combinado['CAMPUS'] == 'ESPE EN LINEA'
        
        df_combinado['HORAS'] = df_combinado['HORA_DIA'] * df_combinado['NUM_DIAS']
        df_combinado['HORAS'] = df_combinado['HORAS'].round(0).astype(int)
        df_combinado['HI'] = df_combinado['HI'].dt.time
        df_combinado['HF'] = df_combinado['HF'].dt.time
        df_combinado.loc[mask, 'HORAS'] = df_combinado.loc[mask, '# EST'] / 11
        nuevo_orden = ['DEPARTAMENTO', 'CAMPUS', 'ÁREA DE CONOCIMIENTO', 'CODIGO_ASIGNATURA', 'NRC', 'STATUS', '# EST', 'HI', 'HF', 'L', 'M', 'I', 'J', 'V', 'HORA_DIA', 'NUM_DIAS', 'HORAS', 'TIPO', 'PERIODO', 'OBSERVACION']
        
        df_combinado = df_combinado.reindex(columns=nuevo_orden)
        df_combinado.to_excel(ruta_combinado, index=False)



