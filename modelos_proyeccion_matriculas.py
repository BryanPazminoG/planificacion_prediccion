import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.tree import DecisionTreeRegressor
from sklearn.neighbors import KNeighborsRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error
import time
import multiprocessing
from statsmodels.tsa.arima_model import ARIMA

from joblib import Parallel, delayed


ruta_archivo = "Data/Output/Dataframe/Dataframe_Combinado.xlsx"
ruta_salida = "Data/Output/Prediccion/Prediccion_Matriculas.xlsx"

start_time = time.time()

# Leer el archivo Excel en un DataFrame
df_combinado = pd.read_excel(ruta_archivo)

df_combinado = df_combinado.loc[df_combinado['NRC'] != df_combinado['NRC'].shift()]

# Agrupar y sumar estudiantes por asignatura, periodo y campus
estudiantes_por_asignatura_periodo_campus = df_combinado.groupby(["ASIGNATURA", "PERIODO", "CAMPUS"])["# EST"].sum().reset_index()

# Ordenar por campus
estudiantes_por_asignatura_periodo_campus = estudiantes_por_asignatura_periodo_campus.sort_values(by="CAMPUS").reset_index(drop=True)

# Group the data by 'CAMPUS', 'DEPARTAMENTO', 'ASIGNATURA', and 'PERIODO', and sum the '# EST' values.
grouped_df = df_combinado.groupby(['CAMPUS', 'DEPARTAMENTO', 'ASIGNATURA', 'PERIODO'])['# EST'].sum().reset_index()

# Pivot the table to get the desired format with 'PERIODO' as columns and 'CAMPUS', 'DEPARTAMENTO', and 'ASIGNATURA' as rows.
df_periodos_estudiantes = grouped_df.pivot_table(index=['CAMPUS', 'DEPARTAMENTO', 'ASIGNATURA'], columns='PERIODO', values='# EST', fill_value=0)

# Reset the index to move 'CAMPUS', 'DEPARTAMENTO', and 'ASIGNATURA' back as columns.
df_periodos_estudiantes = df_periodos_estudiantes.reset_index()

###########################################################################
# Agrupar por asignatura y periodo y calcular la suma de estudiantes (# EST) en cada grupo
df_grouped = estudiantes_por_asignatura_periodo_campus.groupby(['ASIGNATURA', 'PERIODO'])['# EST'].sum().reset_index()

# Utilizar pivot_table para reorganizar los datos en la forma deseada
df_students_by_period = df_grouped.pivot_table(index='ASIGNATURA', columns='PERIODO', values='# EST', fill_value=0)

# Reiniciar los nombres de las columnas para eliminar el índice "PERIODO"
df_students_by_period.columns.name = None

# Resetear el índice del dataframe para convertir la columna "ASIGNATURA" en una columna regular
df_students_by_period = df_students_by_period.reset_index()


def exponential_smoothing(series, alpha):
    result = [series[0]]  # first value is same as series
    for i in range(1, len(series)):
        result.append(alpha * series[i] + (1 - alpha) * result[i - 1])
    return result

def predict_students(asignatura, campus, group):
    X = group['PERIODO'].values.reshape(-1, 1)
    y = group['# EST'].values
    
    # Normalize the data
    scaler = StandardScaler()
    X_normalized = scaler.fit_transform(X)

    # Use Linear Regression for comparison
    model_lr = RandomForestRegressor(n_estimators=100, random_state=42)
    model_lr.fit(X_normalized, y)  # Fit the model before making predictions

    # Exponential Smoothing
    alpha = 0.3  # You can adjust this value according to the nature of the data
    smoothed_students = exponential_smoothing(y, alpha)
    predicted_students_exponential = alpha * y[-1] + (1 - alpha) * smoothed_students[-1]

    # Decision Tree Regressor
    model_dt = DecisionTreeRegressor(random_state=42)
    model_dt.fit(X_normalized, y)
    predicted_students_dt = model_dt.predict(X_normalized[-1].reshape(1, -1))

    next_period = estudiantes_por_asignatura_periodo_campus['PERIODO'].max() + 1
    next_period_normalized = scaler.transform([[next_period]])
    predicted_students_lr = model_lr.predict(next_period_normalized)
    predicted_students_dt = model_dt.predict(next_period_normalized)

    department = df_combinado.loc[(df_combinado['ASIGNATURA'] == asignatura) & (df_combinado['CAMPUS'] == campus), 'DEPARTAMENTO'].iloc[0]
    codigo = df_combinado.loc[(df_combinado['ASIGNATURA'] == asignatura) & (df_combinado['CAMPUS'] == campus), 'CODIGO'].iloc[0]

    return {
        'CAMPUS': campus,
        'DEPARTAMENTO': department,
        'ASIGNATURA': asignatura,
        'CODIGO': codigo,
        'ESTUDIANTES_PREDICHOS_LR': predicted_students_lr[0],
        'ESTUDIANTES_PREDICHOS_EXPONENCIAL': predicted_students_exponential,
        'ESTUDIANTES_PREDICHOS_DT': predicted_students_dt[0],
    }

num_cores = multiprocessing.cpu_count()
num_jobs = max(1, int(num_cores * 0.8))

# Execute the for loop in parallel
results = Parallel(n_jobs=num_jobs)(
    delayed(predict_students)(asignatura, campus, group)
    for (asignatura, campus), group in estudiantes_por_asignatura_periodo_campus.groupby(['ASIGNATURA', 'CAMPUS'])
)

predictions_df = pd.DataFrame(results).sort_values(by='DEPARTAMENTO').reset_index(drop=True)

predictions_df.to_excel(ruta_salida, index=False)

end_time = time.time()
execution_time = end_time - start_time
print("Tiempo de ejecución:", execution_time, "segundos")

