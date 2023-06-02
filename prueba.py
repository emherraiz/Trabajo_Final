import dask.dataframe as dd
from jinja2 import Template

# Especifica la ruta del archivo CSV
archivo_csv = "air_traffic_data.csv"

# Lee el archivo CSV en un dataframe de Dask
datos_traf = dd.read_csv(archivo_csv)

# Muestra el dataframe
print(datos_traf.head())

# Vamos a obtener informaión sobre la estructura de los datos:
campos = datos_traf.columns
tipos = datos_traf.dtypes

# Cargar la plantilla desde un archivo o una cadena
with open('plantilla_datos.html') as f:
    plantilla = Template(f.read())


# Renderizar la plantilla con los datos
datos = zip(campos, tipos)
informe = plantilla.render(datos=datos)

# Guardar el informe en un archivo
with open('informe.html', 'w') as f:
    f.write(informe)

'''
for campo, tipo in zip(campos, tipos):
    print(f"Nombre del campo: {campo}")
    print(f"Tipo de dato: {tipo}")
    print()

for campo, tipo in zip(campos, tipos):
    resultado = f"Nombre del campo: {campo}\nTipo de dato: {tipo}\n"
    print(resultado)
'''
