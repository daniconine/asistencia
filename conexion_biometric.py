#Code para extraer info de huellero
from zk import ZK, const
import mysql.connector

def main():
    # Crear una instancia del objeto ZK
    zk = ZK('192.168.0.15', port=4370)

    # Conectar al dispositivo
    conn = zk.connect()
    if conn:
        print("Conexión exitosa al dispositivo")
        
        # Obtener registros biométricos
        users = zk.get_users()
        for user in users:
            print(f"ID: {user.user_id}, Nombre: {user.name}")
        
        print("Asistencia \n")
        #Obtener registro de marcaciones
        attendances = zk.get_attendance()
        for attendance in attendances:
            print(attendance.user_id,attendance.timestamp)

        # Desconectar del dispositivo
        zk.disconnect()
    else:
        print("Error al conectar al dispositivo")

if __name__ == '__main__':
    main()