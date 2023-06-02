import dask.dataframe as dd
import pandas as pd
from jinja2 import Template

# Especifica la ruta del archivo CSV
archivo_csv = "air_traffic_data.csv"

# Lee el archivo CSV en un dataframe de Dask
datos_traf = dd.read_csv(archivo_csv)

# Vamos a obtener informaión sobre la estructura de los datos:
campos = datos_traf.columns
tipos = datos_traf.dtypes

# Cargar la plantilla desde un archivo o una cadena
with open('plantilla_datos.html') as f:
    plantilla = Template(f.read())


# Renderizar la plantilla con los datos
datos = zip(campos, tipos)
informe = plantilla.render(datos=datos)

# Guardar el informe en un archivo como se nos pide en el primer ejercicio
with open('informe.html', 'w') as f:
    f.write(informe)

# Leer el archivo HTML y extraer las tablas
tablas = pd.read_html('informe.html')

# Obtener la primera tabla (si hay varias tablas en el archivo HTML)
tabla = tablas[0]

# Imprimir el DataFrame de la tabla
print(tabla)
print('\n\nLos datos se han cargado correctamente, hemos guardado el informe en el archivo informe.html\n\n')

# Análisis de los datos
num_filas = len(datos_traf)  # Número de filas en el DataFrame
num_columnas = len(datos_traf.columns)  # Número de columnas en el DataFrame
columnas = datos_traf.columns.tolist()  # Lista de nombres de las columnas

# Información sobre las columnas
info_columnas = []
for columna in columnas:
    tipo_dato = datos_traf[columna].dtype  # Tipo de dato de la columna
    valores_unicos = datos_traf[columna].nunique().compute()  # Número de valores únicos en la columna
    info_columnas.append({'Nombre del campo': columna, 'Tipo de dato': tipo_dato, 'Valores únicos': valores_unicos})

# Presentar los resultados
for columna_info in info_columnas:
    print(f"Nombre del campo: {columna_info['Nombre del campo']}")
    print(f"Tipo de dato: {columna_info['Tipo de dato']}")
    print(f"Número de valores únicos: {columna_info['Valores únicos']}")
    print()


# Obtener las compañías únicas de la columna "Operating Airline"
companias_operating = datos_traf['Operating Airline'].nunique().compute()
operadora = datos_traf['Operating Airline']

# Obtener las compañías únicas de la columna "Published Airline"
companias_published = datos_traf['Published Airline'].nunique().compute()
publicada = datos_traf['Published Airline']

# Imprimir los resultados
print(f"Número de compañías en la columna 'Operating Airline': {companias_operating}")
print(f"Número de compañías en la columna 'Published Airline': {companias_published}")

# Realizar un merge basado en las columnas "Operating Airline" y "Published Airline"
merged_data = dd.merge(datos_traf[['Operating Airline']], datos_traf[['Published Airline']], left_index=True, right_index=True)

# Filtrar los datos donde las columnas "Operating Airline" y "Published Airline" no coinciden
mismatched_data = merged_data[merged_data['Operating Airline'] != merged_data['Published Airline']].compute()

# Imprimir los datos que no coinciden
print("\nDatos que no coinciden entre 'Operating Airline' y 'Published Airline':\n")
print(mismatched_data)
print('\nCon un total de', len(mismatched_data), 'datos que no coinciden')
