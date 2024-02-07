import psycopg2
import pandas as pd

ruta_archivo_excel = 'huellero_prueba.xls'
columnas = ['Name', 'Time']
replace_name = {42634466: 4, 10706046: 3, 70412457: 2}

# Parámetros de conexión a PostgreSQL
dbname = "gerens1"
user = "odoo"
password = "odoo"
host = "localhost"
port = "5432"

# Intentar conectar a la base de datos
try:
    connection = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port
    )
    cursor = connection.cursor()

    # Leer datos desde el archivo Excel
    df = pd.read_excel(ruta_archivo_excel, sheet_name='Sheet 1', usecols=columnas)
    
    df['Name'] = df['Name'].replace(replace_name)
    
    min_max_times = df.groupby(['Name', df['Time'].dt.date])['Time'].agg(['min', 'max']).reset_index()
    print(min_max_times)
    tuplas = [tuple(x) for x in min_max_times[['Name', 'min', 'max']].to_numpy()]
    #data_mins = [tuple(x) for x in min_max_times[['Name', 'min']].to_numpy()]
    #data_maxs = [tuple(x) for x in min_max_times[['Name', 'max']].to_numpy()]
    
    tabla = 'hr_attendance'

    
    sql_insert = f"INSERT INTO {tabla} (employee_id, check_in, check_out) VALUES (%s, %s, %s);"

    for tupla in tuplas:
        cursor.execute(sql_insert, tupla)
    
        
    connection.commit()
    connection.close()
    
except psycopg2.Error as e:
    print("Error al conectar a la base de datos PostgreSQL:", e)

except FileNotFoundError:
    print(f"No se encontró el archivo en la ruta especificada: {ruta_archivo_excel}")

except Exception as e:
    print(f"Error general: {e}")

finally:
    # Asegúrate de cerrar la conexión y el cursor al finalizar
    if connection:
        connection.close()
    if cursor:
        cursor.close()