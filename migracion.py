# -*- coding: utf-8 -*-
# Especifica la codificación del archivo como UTF-8 para admitir caracteres especiales.

import pymysql  # Librería para interactuar con bases de datos MySQL.
from pymongo import MongoClient  # Librería para conectar y operar con MongoDB.
from cassandra.cluster import Cluster  # Para conectar con un clúster de Cassandra.
from cassandra.auth import PlainTextAuthProvider  # Manejo de autenticación en Cassandra.
from dotenv import load_dotenv  # Librería para cargar variables de entorno desde un archivo .env.
import os  # Permite acceder a variables del sistema operativo.
import json  # Para manejar datos en formato JSON.
import datetime  # Para manejar fechas y tiempos.
from decimal import Decimal  # Para manejar números decimales.

# Cargar variables de entorno desde un archivo .env (credenciales y configuraciones).
load_dotenv('.env')

# Configuración de MySQL obtenida desde las variables de entorno.
mysql_config = {
    "host": os.getenv("MYSQL_HOST"),  # Dirección del servidor MySQL.
    "port": int(os.getenv("MYSQL_PORT")),  # Puerto del servidor MySQL.
    "user": os.getenv("MYSQL_USER"),  # Usuario de la base de datos MySQL.
    "password": os.getenv("MYSQL_PASSWORD"),  # Contraseña del usuario MySQL.
    "database": os.getenv("MYSQL_DATABASE")  # Nombre de la base de datos MySQL.
}

# Configuración de MongoDB obtenida desde las variables de entorno.
mongo_config = {
    "host": os.getenv("MONGO_HOST"),  # Dirección del servidor MongoDB.
    "port": int(os.getenv("MONGO_PORT")),  # Puerto del servidor MongoDB.
    "database": os.getenv("MONGO_DATABASE")  # Nombre de la base de datos MongoDB.
}

# Configuración de Cassandra obtenida desde las variables de entorno.
cassandra_config = {
    "hosts": os.getenv("CASSANDRA_HOSTS").split(","),  # Lista de nodos del clúster Cassandra.
    "keyspace": os.getenv("CASSANDRA_KEYSPACE"),  # Keyspace de Cassandra.
    "username": os.getenv("CASSANDRA_USERNAME"),  # Usuario para autenticación.
    "password": os.getenv("CASSANDRA_PASSWORD")  # Contraseña del usuario.
}

# Función para establecer conexión con la base de datos MySQL.
def connect_mysql():
    return pymysql.connect(
        host=mysql_config["host"],  # Dirección del servidor MySQL.
        port=mysql_config["port"],  # Puerto de conexión.
        user=mysql_config["user"],  # Usuario de MySQL.
        password=mysql_config["password"],  # Contraseña del usuario.
        database=mysql_config["database"],  # Nombre de la base de datos.
        charset="utf8mb4"  # Establece codificación de caracteres.
    )

# Función para establecer conexión con MongoDB.
def connect_mongodb():
    client = MongoClient(host=mongo_config["host"], port=mongo_config["port"])  # Crea un cliente para MongoDB.
    return client[mongo_config["database"]]  # Retorna la base de datos especificada.

# Función para establecer conexión con Cassandra.
def connect_cassandra():
    auth_provider = PlainTextAuthProvider(
        username=cassandra_config["username"],  # Usuario para autenticación.
        password=cassandra_config["password"]  # Contraseña para autenticación.
    )
    cluster = Cluster(cassandra_config["hosts"], auth_provider=auth_provider)  # Conecta con el clúster.
    session = cluster.connect()  # Crea una sesión de conexión.
    session.set_keyspace(cassandra_config["keyspace"])  # Selecciona el keyspace.
    return session  # Retorna la sesión de Cassandra.

# Función para convertir fechas a formato adecuado para JSON y manejar caracteres especiales
def convert_date_fields(document):
    for key, value in document.items():
        if isinstance(value, datetime.date):
            document[key] = value.isoformat()  # Convertir a string en formato 'YYYY-MM-DD'
        elif isinstance(value, Decimal):
            document[key] = float(value)  # Convertir Decimal a float
        elif isinstance(value, str):
            document[key] = value.decode("utf-8")  # Convertir cadenas a Unicode
    return document  # Retorna el documento con los datos convertidos.

# Migrar datos de MySQL a JSON
def migrate_to_json():
    mysql_conn = connect_mysql()    # Establece conexión con MySQL.
    try:
        with mysql_conn.cursor() as cursor:     # Crea un cursor para ejecutar consultas.
            cursor.execute("SHOW TABLES")     # Obtiene todas las tablas en la base de datos.
            tables = [table[0] for table in cursor.fetchall()]    # Lista de nombres de tablas.

            for table in tables:    # Itera sobre cada tabla.
                try:
                    print(u"Exportando tabla {} a JSON...".format(table))    # Imprime el progreso.
                    cursor.execute("SELECT * FROM {}".format(table))      # Obtiene todos los registros de la tabla.
                    rows = cursor.fetchall()       # Recupera los resultados.
                    columns = [desc[0] for desc in cursor.description]     # Recupera los nombres de las columnas.

                    data = []
                    for row in rows:     # Itera sobre cada fila.
                        document = dict(zip(columns, row))       # Combina columnas con valores de fila.
                        document = convert_date_fields(document)  # Convertir las fechas y decimales.
                        data.append(document)      # Agrega el documento al listado.

                    # Escribe los datos en un archivo JSON con el nombre de la tabla.
                    with open('{}.json'.format(table), 'w') as json_file:
                        json.dump(data, json_file, indent=4)

                except Exception as e:
                    print(u"Error al exportar la tabla {} a JSON: {}".format(table, e))     # Maneja errores.

    finally:
        mysql_conn.close()     # Cierra la conexión a MySQL.

# Migrar datos de MySQL a MongoDB
def migrate_to_mongodb():
    # Conecta a la base de datos MySQL.
    mysql_conn = connect_mysql()
    # Conecta a la base de datos MongoDB.
    mongo_db = connect_mongodb()
    try:
        # Abre un cursor para interactuar con la base de datos MySQL.
        with mysql_conn.cursor() as cursor:
            # Recupera una lista con los nombres de todas las tablas en la base de datos MySQL.
            cursor.execute("SHOW TABLES")
            tables = [table[0] for table in cursor.fetchall()]     # Crea una lista con los nombres de las tablas.

            # Itera sobre cada tabla recuperada de MySQL.
            for table in tables:
                try:
                    # Imprime un mensaje indicando qué tabla se está migrando.
                    print(u"Migrando tabla {} a MongoDB...".format(table))
                    # Ejecuta una consulta para seleccionar todos los datos de la tabla actual.
                    cursor.execute("SELECT * FROM {}".format(table))
                    rows = cursor.fetchall()     # Recupera todas las filas de la tabla.
                    columns = [desc[0] for desc in cursor.description]     # Obtiene los nombres de las columnas.

                    # Obtiene o crea una colección en MongoDB con el mismo nombre que la tabla.
                    collection = mongo_db[table]

                    # Itera sobre cada fila recuperada de MySQL.
                    for row in rows:
                        # Crea un diccionario combinando los nombres de las columnas con los valores de la fila.
                        document = dict(zip(columns, row))
                        # Convierte las fechas y otros tipos de datos a un formato compatible con JSON.
                        document = convert_date_fields(document)

                        # Inserta el documento (registro) en la colección de MongoDB.
                        collection.insert_one(document)

                # Maneja cualquier excepción que ocurra durante la migración de una tabla.
                except Exception as e:
                    print(u"Error al migrar la tabla {} a MongoDB: {}".format(table, e))

    # Finalmente, cierra la conexión a la base de datos MySQL, asegurando que los recursos se liberen.
    finally:
        mysql_conn.close()

# Migrar datos de MySQL a Cassandra
def migrate_to_cassandra():
    # Conecta a la base de datos MySQL.
    mysql_conn = connect_mysql()
    # Conecta a la base de datos Cassandra.
    cassandra_session = connect_cassandra()
    try:
        # Abre un cursor para interactuar con la base de datos MySQL.
        with mysql_conn.cursor() as cursor:
            # Obtiene todas las tablas existentes en la base de datos MySQL.
            cursor.execute("SHOW TABLES")
            tables = [table[0] for table in cursor.fetchall()]    # Lista de nombres de tablas.

            # Itera sobre cada tabla en la base de datos MySQL.
            for table in tables:
                try:
                    # Imprime el nombre de la tabla que se está migrando.
                    print(u"Migrando tabla {} a Cassandra...".format(table))
                    # Recupera todos los registros de la tabla actual.
                    cursor.execute("SELECT * FROM {}".format(table))
                    rows = cursor.fetchall()      # Almacena los datos de las filas.
                    columns = [desc[0] for desc in cursor.description]       # Obtiene los nombres de las columnas.

                    # Crear la tabla en Cassandra
                    create_table_query = u"CREATE TABLE IF NOT EXISTS {} (".format(table)
                    primary_key = None  # Variable para almacenar la clave primaria.

                    # Itera sobre la descripción de las columnas de la tabla MySQL.
                    for column_desc in cursor.description:
                        col_name = column_desc[0].decode('utf-8')  # Convierte el nombre de la columna a Unicode.
                        col_type = column_desc[1]    # Tipo de dato de la columna.
                        cassandra_type = u"text"  # Por defecto, define el tipo de dato como "text".

                        # Asigna tipos de datos equivalentes en Cassandra.
                        if col_type in (3, 8):  # INT o FLOAT en MySQL.
                            cassandra_type = u"int" if col_type == 3 else u"float"
                        elif col_type in (253, 252):  # VARCHAR o TEXT en MySQL.
                            cassandra_type = u"text"

                        # Agrega la columna al query de creación de tabla.
                        create_table_query += u"{} {}, ".format(col_name, cassandra_type)

                        # Identificar claves primarias por el nombre de la columna que empieza con 'nid_'
                        if col_name.startswith("nid_"):
                            primary_key = col_name  # La primera columna que empieza con 'nid_' será la clave primaria.

                    # Define la clave primaria si fue encontrada.
                    if primary_key:
                        create_table_query += u"PRIMARY KEY ({}))".format(primary_key)
                    else:
                        # Si no hay clave primaria, finaliza el query eliminando la última coma.
                        create_table_query = create_table_query.rstrip(u", ") + u")"

                    # Ejecuta el query para crear la tabla en Cassandra.
                    cassandra_session.execute(create_table_query)

                    # Itera sobre los registros recuperados de MySQL.
                    for row in rows:
                        # Convierte las celdas a string si es necesario.
                        row = [str(cell) if isinstance(cell, str) else cell for cell in row]
                        # Construye la consulta de inserción para la tabla en Cassandra.
                        insert_query = u"INSERT INTO {} ({}) VALUES ({})".format(
                            table, u", ".join(columns), u", ".join(["%s"] * len(columns))
                        )
                        # Inserta el registro en la tabla de Cassandra.
                        cassandra_session.execute(insert_query, row)

                except Exception as e:
                    # Imprime un mensaje de error si ocurre algún problema durante la migración de una tabla.
                    print(u"Error al migrar la tabla {} a Cassandra: {}".format(table, e))

    finally:
        # Cierra la conexión a la base de datos MySQL.
        mysql_conn.close()

# Ejecutar las migraciones
if __name__ == "__main__":
    # Exportar los datos de MySQL a archivos JSON.
    print(u"Exportando datos de MySQL a JSON...")
    migrate_to_json()
    print(u"Exportación a JSON completada.\n")

    # Migrar los datos de MySQL a MongoDB.
    print(u"Migrando datos de MySQL a MongoDB...")
    migrate_to_mongodb()
    print(u"Migración a MongoDB completada.\n")

    # Migrar los datos de MySQL a Cassandra.
    print(u"Migrando datos de MySQL a Cassandra...")
    migrate_to_cassandra()
    print(u"Migración a Cassandra completada.\n")
