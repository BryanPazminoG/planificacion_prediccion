from helper import PrediccionesManager, ActualManager, DataframesManager, Calculos

predicciones_manager = PrediccionesManager()
actual_manager = ActualManager()
dataframes_manager = DataframesManager()
dataframe_calculos = Calculos()


def main():
    # PREDICCIONES
    # Paso 1) Unir Archivos de predicciones.
    predicciones_manager.unir_archivos_predicciones("Data/Output/Prediccion/Prediccion_Matriculas.xlsx", "Data/Output/Prediccion/Predicciones_NRC.xlsx")
    # Paso 2) Comparacion predicciones con catalogo.
    predicciones_manager.calcular_predicciones_catalogo("Data/Output/Prediccion/Combinado_Predicciones.xlsx", "Data/Output/Catalogo/Catalogo_Actualizado.xlsx")

    # # PROCESAR ARCHIVO ACTUAL SI EXISTE
    # # Paso 1) Limpiamos los datos del reporte actual.
    actual_manager.procesar_reporte_actual()
    # # Paso 2) Verificamos la informacion que existe con respecto al catalogo actual.
    #actual_manager.realizar_validacion()
    
    # # AGRUPAR DATAFRAME ACTUAL Y COMBINADO
    # # Paso 1) Agrupamos ambos dataframes
    malla_no_vigente = dataframes_manager.unir_y_procesar_dataframes()
    # # Paso 2)
    dataframes_manager.comparar_catalogo(malla_no_vigente)
    
    # # CALCULOS FINALES
    dataframe_calculos.filtrar_y_guardar_excel('Data\Output\Dataframe\Dataframe_Combinado.xlsx', 'ASIGNATURA', 'Profesionalizante|Complex', 'Data\Output\Dataframe\Dataframe_Combinado.xlsx')
    dataframe_calculos.filtrar_y_guardar_excel('Data\Output\Prediccion\Dataframe_Predicciones_Calculo.xlsx', 'ASIGNATURA', 'Profesionalizante|Complex', 'Data\Output\Prediccion\Dataframe_Predicciones_Calculo.xlsx')
    dataframe_calculos.filtrar_y_guardar_excel('Data\Output\Prediccion\Dataframe_Predicciones_Calculo.xlsx', 'CAMPUS', 'ESPE EN LINEA', 'Data\Output\Prediccion\Dataframe_Predicciones_Calculo.xlsx')
    dataframe_calculos.modificar_excel('Data\Output\Dataframe\Dataframe_Combinado.xlsx')
    dataframe_calculos.crear_columna_codigo_asignatura('Data\Output\Prediccion\Dataframe_Predicciones_Calculo.xlsx')
    dataframe_calculos.cargar_y_procesar_datos('Data\Output\Dataframe\Dataframe_Combinado.xlsx')
    
if __name__ == "__main__":
    main()
