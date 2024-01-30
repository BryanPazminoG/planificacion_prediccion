import pandas as pd


# ####################################### UNIR DATAFRAMES COMBINADO Y ACTUAL 
# df_actual = pd.read_excel("Data/Output/Dataframe Actual/Dataframe_Actual.xlsx")
# df_combinado = pd.read_excel("Data/Output/Dataframe/Dataframe_Combinado.xlsx")
# # Concatenar los DataFrames
# df_resultado = pd.concat([df_combinado, df_actual])
# # Guardar el resultado en un nuevo archivo Excel
# df_resultado.to_excel("Data/Output/Dataframe/Dataframe_Combinado_Actual.xlsx", index=False)


# import pandas as pd

# # Lee los DataFrames
# df_actual = pd.read_excel('Data/Output/Dataframe/Dataframe_Combinado.xlsx')
# df_catalogo = pd.read_excel('Data/Output/Catalogo/Catalogo_Actualizado.xlsx')

# # Filtra las columnas necesarias en ambos DataFrames
# df_actual = df_actual[['DEPARTAMENTO', 'CAMPUS', 'ÁREA DE CONOCIMIENTO', 'CODIGO', 'ASIGNATURA', 'NRC', 'STATUS', '# EST', 'HI', 'HF', 'L', 'M', 'I', 'J', 'V', 'TIPO', 'PERIODO']]
# df_catalogo = df_catalogo[['DEPARTAMENTO', 'CODIGO', 'ASIGNATURA', 'TEORIA MINIMO', 'TEORIA MAXIMO', 'LABORATORIO MINIMO', 'LABORATORIO MAXIMO']]

# # Combina DataFrames por las columnas DEPARTAMENTO, CODIGO y ASIGNATURA usando left join
# df_merged = pd.merge(df_actual, df_catalogo, on=['DEPARTAMENTO', 'CODIGO', 'ASIGNATURA'], how='left')

# # Define una función para calcular las HORAS y la OBSERVACION según la lógica especificada
# def calcular_horas_y_observacion(row):
#     if row['TEORIA MINIMO'] == 0 or row['LABORATORIO MINIMO'] == 0:
#         total_horas = row['TEORIA MAXIMO'] + row['LABORATORIO MAXIMO']
#         if total_horas > 4:
#             return total_horas, 'Division'
#         return total_horas, 'MAX'
#     else:
#         return row['TEORIA MINIMO'] + row['LABORATORIO MINIMO'], 'MIN'

# # Aplica la función a las nuevas columnas 'HORAS' y 'OBSERVACION'
# df_merged['HORAS'], df_merged['OBSERVACION'] = zip(*df_merged.apply(calcular_horas_y_observacion, axis=1))

# # Elimina las columnas que ya no son necesarias
# df_actual = df_merged[['DEPARTAMENTO', 'CAMPUS', 'ÁREA DE CONOCIMIENTO', 'CODIGO', 'ASIGNATURA', 'NRC', 'STATUS', '# EST', 'HI', 'HF', 'L', 'M', 'I', 'J', 'V', 'TIPO', 'PERIODO']]

# # Asigna las nuevas columnas 'HORAS' y 'OBSERVACION' al DataFrame original
# df_actual['HORAS'] = df_merged['HORAS']
# df_actual['OBSERVACION'] = df_merged['OBSERVACION']

# # Asigna 'Malla No Vigente' a CAMPUS en las filas sin match
# df_actual.loc[df_actual['CAMPUS'].isnull(), 'CAMPUS'] = 'Malla No Vigente'

# # Realiza las operaciones adicionales sin perder filas en df_actual
# division_condition = (df_actual['OBSERVACION'] == 'Division')
# df_actual.loc[division_condition, 'CAMPUS'] = 'Division'
# df_actual.loc[df_actual['CAMPUS'] == 'ESPE EN LINEA', '# EST'] /= 11

# # Guarda el resultado en el mismo archivo Excel sobrescribiendo el original
# df_actual.to_excel("Data/Output/Dataframe/Dataframe_CombinadoDiv.xlsx", index=False)

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
        ruta_nuevo_archivo = 'Data/Output/Dataframe Actual/Dataframe_Actual_Cat.xlsx'
        data.to_excel(ruta_nuevo_archivo, index=False)
        
import os

####################################### COMPARAR REPORTE ACTUAL CON CATALOGO 
catalogo_path = 'Data/Output/Catalogo/Catalogo_Actualizado.xlsx'
dataframe_path = 'Data\Output\Dataframe\Dataframe_Combinado.xlsx'
# Validar si el archivo del DataFrame existe
if os.path.exists(dataframe_path):
    validador = ValidacionMallaActual(catalogo_path, dataframe_path)
    validador.realizar_validacion()
    validador.guardar_resultado()
    validador.reemplazar_valores_campus(dataframe_path)
else:
    print(f"El archivo {dataframe_path} no existe. No se realizarán las operaciones.")