import dask.dataframe as dd
import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

# Función para convertir una columna a tipo numérico
def convertir_a_numerico(particion):
    return particion.apply(pd.to_numeric, errors='coerce')

# Especifica la ruta del archivo CSV
archivo_csv = "air_traffic_data.csv"

# Lee el archivo CSV en un dataframe de Dask
datos_traf = dd.read_csv(archivo_csv)

# Convertir las columnas a tipo numérico
datos_traf = datos_traf.map_partitions(convertir_a_numerico, meta=datos_traf)

# Reemplazar 'foofoo' por NaN en todas las columnas
datos_traf = datos_traf.replace('foofoo', np.nan)

# Configurar las características o columnas con los tipos necesarios para el cálculo de la matriz de correlación
datos_traf['GEO Region'] = datos_traf['GEO Region'].astype(str)
datos_traf['Activity Type Code'] = datos_traf['Activity Type Code'].astype(str)
datos_traf['Price Category Code'] = datos_traf['Price Category Code'].astype(str)
datos_traf['Terminal'] = datos_traf['Terminal'].astype(str)
datos_traf['Boarding Area'] = datos_traf['Boarding Area'].astype(str)
datos_traf['Operating Airline'] = datos_traf['Operating Airline'].astype(str)
datos_traf['Operating Airline IATA Code'] = datos_traf['Operating Airline IATA Code'].astype(str)
datos_traf['Published Airline'] = datos_traf['Published Airline'].astype(str)
datos_traf['Published Airline IATA Code'] = datos_traf['Published Airline IATA Code'].astype(str)
datos_traf['GEO Summary'] = datos_traf['GEO Summary'].astype(str)
datos_traf['Adjusted Activity Type Code'] = datos_traf['Adjusted Activity Type Code'].astype(str)

# Calcular la matriz de correlación
matriz_correlacion = datos_traf.corr().compute()

# Seleccionar los 10 elementos más importantes de la matriz de correlación
elementos_importantes = matriz_correlacion.unstack().sort_values(ascending=False)[:10]

# Imprimir los 10 elementos más importantes y sus valores de correlación
print("\nElementos más importantes de la matriz de correlación:")
for elementos, correlacion in elementos_importantes.items():
    print(f"{elementos}: {correlacion}")

# Selección de columnas para el algoritmo de aprendizaje automático
columnas_seleccionadas = ['Passenger Count', 'Adjusted Passenger Count', 'Year', 'Month']
target_columna = 'Label'

# Preprocesamiento de datos para el algoritmo de aprendizaje automático
datos_preprocesados = datos_traf[columnas_seleccionadas].compute()

print('\nNuestros datos preprocesados:\n')
print(datos_preprocesados.head())

# Codificación de variables categóricas si es necesario
label_encoder = LabelEncoder()
datos_preprocesados['GEO Region'] = label_encoder.fit_transform(datos_traf['GEO Region'].astype(str))
datos_preprocesados['Activity Type Code'] = label_encoder.fit_transform(datos_traf['Activity Type Code'].astype(str))
datos_preprocesados['Price Category Code'] = label_encoder.fit_transform(datos_traf['Price Category Code'].astype(str))
datos_preprocesados['Terminal'] = label_encoder.fit_transform(datos_traf['Terminal'].astype(str))
datos_preprocesados['Boarding Area'] = label_encoder.fit_transform(datos_traf['Boarding Area'].astype(str))
datos_preprocesados['Operating Airline'] = label_encoder.fit_transform(datos_traf['Operating Airline'].astype(str))
datos_preprocesados['Operating Airline IATA Code'] = label_encoder.fit_transform(datos_traf['Operating Airline IATA Code'].astype(str))
datos_preprocesados['Published Airline'] = label_encoder.fit_transform(datos_traf['Published Airline'].astype(str))
datos_preprocesados['Published Airline IATA Code'] = label_encoder.fit_transform(datos_traf['Published Airline IATA Code'].astype(str))
datos_preprocesados['GEO Summary'] = label_encoder.fit_transform(datos_traf['GEO Summary'].astype(str))
datos_preprocesados['Adjusted Activity Type Code'] = label_encoder.fit_transform(datos_traf['Adjusted Activity Type Code'].astype(str))

# Separar los datos en conjuntos de entrenamiento y prueba
X_train, X_test, y_train, y_test = train_test_split(datos_preprocesados[columnas_seleccionadas], datos_preprocesados[target_columna], test_size=0.2, random_state=42)

# Crear y entrenar el clasificador de bosque aleatorio
clf = RandomForestClassifier()
clf.fit(X_train, y_train)

# Predecir los resultados en el conjunto de prueba
y_pred = clf.predict(X_test)

# Calcular la precisión del clasificador
precision = accuracy_score(y_test, y_pred)
print(f"\nPrecisión del clasificador: {precision}")
