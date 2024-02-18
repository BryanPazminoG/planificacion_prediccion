import os
import pandas as pd

def calcular_predicciones_catalogo(ruta_predicciones, ruta_catalogo):
        archivo_predicciones = pd.read_excel(ruta_predicciones)
        archivo_catalogo = pd.read_excel(ruta_catalogo)
        merged_data = pd.merge(archivo_predicciones, archivo_catalogo, on=['DEPARTAMENTO', 'CODIGO', 'ASIGNATURA'], how='inner')
        
        merged_data = merged_data.rename(columns={
            'ESTUDIANTES_PREDICHOS_LR': 'ESTUDIANTES_RL',
            'ESTUDIANTES_PREDICHOS_EXPONENCIAL': 'ESTUDIANTES_SE',
            'ESTUDIANTES_PREDICHOS_DT': 'ESTUDIANTES_AD',
            'NRC_PREDICHOS_RF': 'NRC_RL',
            'NRC_PREDICHOS_EXPONENCIAL': 'NRC_SE',
            'NRC_PREDICHOS_DT': 'NRC_AD',
        })
        
        def calcular_multiplicacion_fila(row):
            if row['TEORIA MINIMO'] == 0 and row['LABORATORIO MINIMO'] == 0 and row['TEORIA MAXIMO'] == 0 and row['LABORATORIO MAXIMO'] == 0:
                observacion = 'CERO'
                horas = 0
            elif row['TEORIA MINIMO'] == 0 and row['LABORATORIO MINIMO'] == 0:
                observacion = 'MAX'
                total_horas = row['TEORIA MAXIMO'] + row['LABORATORIO MAXIMO']
                if total_horas >= 4:
                    observacion = 'MAX4'
                horas = row['NRC_RL'] * total_horas
            elif row['TEORIA MAXIMO'] == 0 and row['LABORATORIO MAXIMO'] == 0:
                observacion = 'MIN'
                horas = row['NRC_RL'] * (row['TEORIA MINIMO'] + row['LABORATORIO MINIMO'])
            else:
                observacion = 'CERO'
                horas = 0
            # Multiplicar las horas por los valores de NRC_RL, NRC_SE y NRC_AD
            horas_rl = horas * row['NRC_RL']
            horas_se = horas * row['NRC_SE']
            horas_ad = horas * row['NRC_AD']
            
            return horas_rl, horas_se, horas_ad, horas, observacion
        
            ####return horas, observacion
            


        # Aplicar la función de cálculo a las columnas correspondientes y obtener las observaciones
        #####merged_data['HORAS_SE'], merged_data['OBSERVACION_SE'] = zip(*merged_data.apply(lambda row: calcular_multiplicacion_fila(row), axis=1))
        # Aplicar la función de cálculo a las columnas correspondientes y obtener las observaciones
        merged_data['HORAS_RL'], merged_data['HORAS_SE'], merged_data['HORAS_AD'], merged_data['HORAS_TOTALES'], merged_data['OBSERVACION_SE'] = zip(*merged_data.apply(lambda row: calcular_multiplicacion_fila(row), axis=1))


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


        #result_data = merged_data[['CAMPUS', 'DEPARTAMENTO', 'ASIGNATURA', 'ÁREA DE CONOCIMIENTO', 'CODIGO', 'ESTUDIANTES_RL', 'ESTUDIANTES_SE', 'ESTUDIANTES_AD', 'NRC_RL', 'NRC_SE', 'NRC_AD', 'HORAS_SE', 'OBSERVACION_SE']]
        result_data = merged_data[['CAMPUS', 'DEPARTAMENTO', 'ASIGNATURA', 'ÁREA DE CONOCIMIENTO', 'CODIGO', 'ESTUDIANTES_RL', 'ESTUDIANTES_SE', 'ESTUDIANTES_AD', 'NRC_RL', 'NRC_SE', 'NRC_AD', 'HORAS_RL', 'HORAS_SE', 'HORAS_AD', 'HORAS_TOTALES', 'OBSERVACION_SE']]
        
        if division_condition.any():
            print("Se encontraron divisiones, realizando ajustes adicionales.")

        ruta_salida = ruta_predicciones.replace('.xlsx', '_resultado.xlsx')
        result_data.to_excel(ruta_salida, index=False)

        try:
            #os.remove(ruta_predicciones)
            print("Archivo original de predicciones eliminado exitosamente.")
        except Exception as e:
            print(f"Error al eliminar el archivo original de predicciones: {e}")
            
            
calcular_predicciones_catalogo("Combinado_Predicciones.xlsx", "Data\Output\Catalogo\Catalogo_Actualizado.xlsx")
