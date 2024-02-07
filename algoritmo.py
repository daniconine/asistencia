#Algoritmo para cargar el excel to plantilla odoo

import pandas as pd
import openpyxl 
from openpyxl.utils.dataframe import dataframe_to_rows

excelp3 = 'huellero_piso3.xls'
excelti = 'huellero_ti.xls'
columnas = ['Name', 'Time']
replace_name = { 74836601: 7, 40037388: 6, 10707488: 5, 42634466: 4, 10706046: 3, 70412457: 2}

# Leer datos desde el archivo Excel

df1 = pd.read_excel(excelp3 , sheet_name='Sheet 1', usecols=columnas)
df2 = pd.read_excel(excelti, sheet_name='Sheet 1', usecols=columnas)
df = pd.concat([df1, df2], ignore_index=True)
df['Name'] = df['Name'].replace(replace_name)

min_max_times = df.groupby(['Name', df['Time'].dt.date])['Time'].agg(['min', 'max']).reset_index()
print(min_max_times)
tuplas = [tuple(x) for x in min_max_times[['Name', 'min', 'max']].to_numpy()]
#data_mins = [tuple(x) for x in min_max_times[['Name', 'min']].to_numpy()]
#data_maxs = [tuple(x) for x in min_max_times[['Name', 'max']].to_numpy()]

print([tuplas])

# Convertir las tuplas en un DataFrame de pandas
df = pd.DataFrame(tuplas, columns=['Name', 'min', 'max'])

# Crear un nuevo archivo Excel
wb = openpyxl.Workbook()
ws = wb.active

# Insertar los encabezados de columna
for col, header in enumerate(df.columns, start=1):
    ws.cell(row=1, column=col, value=header)

# Insertar los datos
for row_data in dataframe_to_rows(df, index=False, header=False):
    ws.append(row_data)

# Guardar el archivo Excel
wb.save('odoo.xlsx')
