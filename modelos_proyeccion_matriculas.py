import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler
import time

ruta_archivo = "Data/Output/Dataframe/Dataframe_Combinado.xlsx"
ruta_salida = "Data/Output/Prediccion/Prediccion_Matriculas.xlsx"

def exponential_smoothing(series, alpha):
    result = [series[0]]  # first value is same as series
    for i in range(1, len(series)):
        result.append(alpha * series[i] + (1 - alpha) * result[i - 1])
    return result

def predict_students(asignatura, campus, group, df_combinado):
    X = group['PERIODO'].values.reshape(-1, 1)
    y = group['# EST'].values
    
    # Normalize the data
    scaler = StandardScaler()
    X_normalized = scaler.fit_transform(X)

    # Random Forest Regression
    model_rf = RandomForestRegressor(n_estimators=100, random_state=42)
    model_rf.fit(X_normalized, y)
    predicted_students_rf = model_rf.predict(X_normalized[-1].reshape(1, -1))

    # Linear Regression
    model_lr = LinearRegression()
    model_lr.fit(X_normalized, y)
    predicted_students_lr = model_lr.predict(X_normalized[-1].reshape(1, -1))

    # Exponential Smoothing
    alpha = 0.3
    smoothed_students = exponential_smoothing(y, alpha)
    predicted_students_exponential = alpha * y[-1] + (1 - alpha) * smoothed_students[-1]

    next_period = df_combinado['PERIODO'].max() + 1
    next_period_normalized = scaler.transform([[next_period]])
    predicted_students_rf = model_rf.predict(next_period_normalized)
    predicted_students_lr = model_lr.predict(next_period_normalized)

    department = df_combinado.loc[(df_combinado['ASIGNATURA'] == asignatura) & (df_combinado['CAMPUS'] == campus), 'DEPARTAMENTO'].iloc[0]
    codigo = df_combinado.loc[(df_combinado['ASIGNATURA'] == asignatura) & (df_combinado['CAMPUS'] == campus), 'CODIGO'].iloc[0]

    return {
        'CAMPUS': campus,
        'DEPARTAMENTO': department,
        'ASIGNATURA': asignatura,
        'CODIGO': codigo,
        'ESTUDIANTES_PREDICHOS_DT': predicted_students_rf[0],
        'ESTUDIANTES_PREDICHOS_LR': predicted_students_lr[0],
        'ESTUDIANTES_PREDICHOS_EXPONENCIAL': predicted_students_exponential
    }

def main():
    start_time = time.time()

    df_combinado = pd.read_excel(ruta_archivo)

    df_combinado = df_combinado.loc[df_combinado['NRC'] != df_combinado['NRC'].shift()]

    estudiantes_por_asignatura_periodo_campus = df_combinado.groupby(["ASIGNATURA", "PERIODO", "CAMPUS"])["# EST"].sum().reset_index()

    results = []

    for (asignatura, campus), group in estudiantes_por_asignatura_periodo_campus.groupby(['ASIGNATURA', 'CAMPUS']):
        result = predict_students(asignatura, campus, group, df_combinado)
        results.append(result)

    predictions_df = pd.DataFrame(results).sort_values(by='DEPARTAMENTO').reset_index(drop=True)

    predictions_df.to_excel(ruta_salida, index=False)

    end_time = time.time()
    execution_time = end_time - start_time
    print("Tiempo de ejecución:", execution_time, "segundos")

if __name__ == "__main__":
    main()