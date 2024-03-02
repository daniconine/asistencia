#Code complete
#extrae de zk and clean then into mysql
from zk import ZK, const
import mysql.connector

def conectar_mysql(host, usuario, contraseña, base_datos):
    try:
        # Crear una conexión a la base de datos
        conexion = mysql.connector.connect(
            host=host,
            user=usuario,
            password=contraseña,
            database=base_datos
        )
        print("Conexión exitosa a MySQL")
        return conexion
    except mysql.connector.Error as error:
        print(f"Error al conectar a MySQL: {error}")
        return None

def subir_registros_mysql(conexion, registros):
    try:
        cursor = conexion.cursor()
        for registro in registros:
            cursor.execute("""
                INSERT INTO Registros (dni, hora)
                VALUES (%s, %s)
            """, (registro.user_id, registro.timestamp))
        conexion.commit()
        print("Registros insertados con éxito en MySQL")
    except mysql.connector.Error as error:
        print(f"Error al insertar registros en MySQL: {error}")

def main():
    # Conectar al huellero ZKTeco
    zk = ZK('192.168.0.14', port=4370)
    conn = zk.connect()

    if conn:
        print("Conexión exitosa al huellero ZKTeco")
        
        # Obtener registros biométricos
        users = zk.get_users()
        for user in users:
            print(f"ID: {user.user_id}, Nombre: {user.name}")
        
        print("Asistencia \n")
        # Obtener registro de marcaciones
        attendances = zk.get_attendance()
        for attendance in attendances:
            print(f"ID: {attendance.user_id}, Fecha y Hora: {attendance.timestamp}")

        # Subir registros a MySQL
        conexion_mysql = conectar_mysql('localhost', 'dani', 'dani', 'huellero')
        if conexion_mysql:
            subir_registros_mysql(conexion_mysql, attendances)
            conexion_mysql.close()
        else:
            print("No se pudo conectar a MySQL")

        zk.clear_attendance()
        #print("Registros limpiados correctamente")
        # Desconectar del dispositivo ZKTeco
        zk.disconnect()
    else:
        print("Error al conectar al huellero ZKTeco")

if __name__ == '__main__':
    main()