#Code extraction de db mysql the min and max
import pandas as pd
import pymysql
#import psycopg2

replace_dni = {
                8385054:84,
                8385341:96,
                25744988:120,
                42634466:95,
                7953755:88,
                70178301:103,
                16720837:86,
                70412457:5,
                44811647:97,
                10122870:118,
                43753029:116,
                48438822:102,
                41512033:8,
                41456692:104,
                47463775:99,
                9997663:110,
                10224222:92,
                10490736:113,
                72658526:105,
                42771108:100,
                46176231:94,
                10706046:6,
                7436026:111,
                72190052:106,
                29272792:112,
                8435931:114,
                10770688:101,
                7823039:90,
                10061772:83,
                10707488:91,
                4742841:115,
                71206053:108,
                7181251:119,
                71220323:122,
                71528197:109,
                9392701:87,
                8799993:85,
                74836601:7,
                72692811:107,
                7427530:117,
                40037388:93,
                9677790:121,
                10224221:89,
                10602720:98,
               }

def extract_mysql_minmax():
    # Conectar a la base de datos donde resides tus datos de "Registros"
    mysql_connection = pymysql.connect(host='localhost', user='dani', password='dani', database='huellero')

    # Consulta SQL para agrupar por dni y fecha, y calcular el mínimo y máximo para cada día
    query = """
        SELECT dni, DATE(hora) as date, MIN(hora) as min_time, MAX(hora) as max_time 
        FROM Registros 
        WHERE procesado = 0
        GROUP BY dni, DATE(hora)
    """
    df = pd.read_sql(query, mysql_connection)

    # # Actualizar la columna "procesado" en la tabla "Registros"
    # update_query = """
    #     UPDATE Registros 
    #     SET procesado = 1
    #     WHERE procesado = 0;
    # """
    # with connection.cursor() as cursor:
    #     cursor.execute(update_query)
    #     connection.commit()

    # Cerrar la conexión
    mysql_connection.close()

    # Imprimir el DataFrame resultante
    return df

df = extract_mysql_minmax()

# Convertir claves del diccionario a cadenas
replace_dni_str = {str(key): value for key, value in replace_dni.items()}

# Realizar el reemplazo con las claves convertidas a cadenas
df['dni'].replace(replace_dni_str, inplace=True)
print("Reemplazo:")
print(df)

# Filtrar por las claves de 'replace_dni'
df_filtrado = df[df['dni'].isin(replace_dni.values())]

# Exportar los datos filtrados a Excel
df_filtrado.to_excel('febrero2024.xlsx', index=False)

print("Filtrado")
print(df_filtrado)
print("Datos exportados satisfactoriamente.")