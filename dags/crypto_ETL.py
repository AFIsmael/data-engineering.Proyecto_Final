from datetime import timedelta,datetime
from email import message
from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
from airflow.models import DAG, Variable
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

import json
import psycopg2
import smtplib
import pandas as pd
import os

dag_path = os.getcwd()     #path original.. home en Docker

url="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws"
with open(dag_path+'/keys/'+"db.txt",'r') as f:
    data_base= f.read()
with open(dag_path+'/keys/'+"user.txt",'r') as f:
    user= f.read()
with open(dag_path+'/keys/'+"pwd.txt",'r') as f: # Generar archivo local con tu password
    pwd= f.read()

with open(dag_path+'/keys/'+"API_KEY.txt",'r') as f: # Generar archivo local con tu api key
    api_key= f.read()

redshift_conn = {
    'host': url,
    'username': user,
    'database': data_base,
    'port': '5439',
    'pwd': pwd
}

# argumentos por defecto para el DAG
default_args = {
    'owner': 'IsmaelAF',
    'start_date': datetime(2023,7,14),
    'retries':5,
    'retry_delay': timedelta(minutes=5)
}

BC_dag = DAG(
    dag_id='CRYPTO_ETL',
    default_args=default_args,
    description='Monitoreo de cryptocurrency diario, coins: BTC, ETH, BNB',
    schedule_interval="@daily",
    catchup=False
)


def extraer_data(exec_date):
    try:
        print(f"Adquiriendo data para la fecha: {exec_date}")
        date = datetime.strptime(exec_date, '%Y-%m-%d %H')
        url ='https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
        parameters = {
           'start':'1',
           'limit':'5000',
           'convert':'USD'
        }
        headers = {
           'Accepts': 'application/json',
           'X-CMC_PRO_API_KEY': f'{api_key}',
        }
        session = Session()
        session.headers.update(headers)
        try:
            response = session.get(url, params=parameters)
        except (ConnectionError, Timeout, TooManyRedirects) as e:
            print(e)
        if response.status_code == 200:
             print('Success!')
             data = response.json()
             path = '/opt/airflow/raw_data/'  # Ruta correspondiente al directorio en el contenedor Docker
             with open(path + "data_" + str(date.year) + '-' + str(date.month) + '-' + str(date.day) + '-' + str(date.hour) + ".json", "w+") as json_file:
               json_file.write(json.dumps(data))  # Escribe los datos en el archivo
               json_file.seek(0)  # Vuelve al principio del archivo
               loaded_data = json.load(json_file)  # Lee los datos del archivo
        else:
              print('An error has occurred.') 
    except ValueError as e:
        print("Formato datetime debería ser %Y-%m-%d %H", e)
        raise e      


# Funcion de transformacion en tabla
def transformar_data(exec_date):       
    print(f"Transformando la data para la fecha: {exec_date}") 
    date = datetime.strptime(exec_date, '%Y-%m-%d %H')
    with open(dag_path+'/raw_data/'+"data_"+str(date.year)+'-'+str(date.month)+'-'+str(date.day)+'-'+str(date.hour)+".json", "r") as json_file:
        loaded_data=json.load(json_file)
   
    #Procesamos los datos en un data Frame:
    df = pd.DataFrame(loaded_data["data"])
 
    # Filtrar los datos para los símbolos BTC, ETH y BNB
    symbols = ["BTC", "ETH", "BNB"]
    filtered_df = df[df["symbol"].isin(symbols)]

    # Seleccionar solo las columnas deseadas
    filtered_df = filtered_df[["name", "symbol", "quote", "last_updated"]]

    # Extraer el valor "price" de la columna "quote"
    filtered_df["price"] = filtered_df["quote"].apply(lambda x: x["USD"]["price"])

    # Eliminar la columna "quote"
    filtered_df = filtered_df.drop("quote", axis=1)
    filtered_df["last_updated"] = pd.to_datetime(filtered_df["last_updated"]).dt.date

    # Guardamos la data procesada: 
    filtered_df = filtered_df[["name", "symbol", "price", "last_updated"]]
    filtered_df.to_csv(dag_path+'/processed_data/'+"data_"+str(date.year)+'-'+str(date.month)+'-'+str(date.day)+'-'+str(date.hour)+".csv", index=False, mode='w')

# Funcion conexion a redshift
def conexion_redshift(exec_date):
    print(f"Conectandose a la BD en la fecha: {exec_date}") 
    url="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
    try:
        conn = psycopg2.connect(
            host=url,
            dbname=redshift_conn["database"],
            user=redshift_conn["username"],
            password=redshift_conn["pwd"],
            port='5439')
        print(conn)
        print("Connected to Redshift successfully!")
    except Exception as e:
        print("Unable to connect to Redshift.")
        print(e)


from psycopg2.extras import execute_values
# Funcion de envio de data
def cargar_data(exec_date):
    print(f"Cargando la data para la fecha: {exec_date}")
    date = datetime.strptime(exec_date, '%Y-%m-%d %H')
    records=pd.read_csv(dag_path+'/processed_data/'+"data_"+str(date.year)+'-'+str(date.month)+'-'+str(date.day)+'-'+str(date.hour)+".csv")
    dtypes= records.dtypes
    # Define the table name
    table_name = 'cryptocurrency_price'
    # Define the columns you want to insert data into
    cols= list(dtypes.index)
    tipos= list(dtypes.values)
    type_map = {'int64': 'INT','int32': 'INT','float64': 'FLOAT','object': 'VARCHAR(50)','bool':'BOOLEAN'}
    sql_dtypes = [type_map[str(dtype)] for dtype in tipos]
    # Definir formato SQL VARIABLE TIPO_DATO
    column_defs = ["id INT IDENTITY(1,1) PRIMARY KEY"] + [f"{name} {data_type}" for name, data_type in zip(cols, sql_dtypes)]
    # Combine column definitions into the CREATE TABLE statement
    table_schema = f"""
        DROP TABLE IF EXISTS {table_name};

        CREATE TABLE {table_name} (
            {', '.join(column_defs)}
        );
        """
    # conexion a database
    url="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
    conn = psycopg2.connect(
        host=url,
        dbname=redshift_conn["database"],
        user=redshift_conn["username"],
        password=redshift_conn["pwd"],
        port='5439')

    from psycopg2.extras import execute_values
    #Crear la Tabla
    cur = conn.cursor()
    cur.execute(table_schema)
    # Generar los valores a insertar
    values = [tuple(x) for x in records.to_numpy()]
    # Definir el INSERT
    insert_sql = f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES %s"
    # Execute the transaction to insert the data
    cur.execute("BEGIN")
    execute_values(cur, insert_sql, values)
    cur.execute("COMMIT")
    print('Proceso terminado')
    
# Funcion de envio de reporte SMTP
def reporte_SMTP(exec_date):
    print(f"Enviando reporte para la fecha: {exec_date}")
    date = datetime.strptime(exec_date, '%Y-%m-%d %H')
    records = pd.read_csv(dag_path+'/processed_data/'+"data_"+str(date.year)+'-'+str(date.month)+'-'+str(date.day)+'-'+str(date.hour)+".csv")

    try:
        x = smtplib.SMTP('smtp.gmail.com', 587)
        x.starttls()
        x.login('arcefigueroaismael@gmail.com', 'htqfmbzgqpurjwew') # Contraseña vinculada al dispositivo, no importa su envio en el codigo!
        subject = 'Reporte diario cryptocurrency'
        message = 'Subject: {}\n\n'.format(subject)
        
        for _, row in records.iterrows(): #iteramos sobre los datos recolectados para armar el cuerpo del mensaje extrayendo los datos recopilados
            name = row['name']
            symbol = row['symbol']
            price = row['price']
            last_updated = row['last_updated']
            
            row_text = f"El precio del {name} para la fecha: {last_updated} es de:  ${price}"
            message += row_text + '\n\n'
        
        x.sendmail('arcefigueroaismael@gmail.com', 'arcefigueroaismael@gmail.com', message)
        print('Éxito')
    except Exception as exception:
        print(exception)
        print('Failure')




# Tareas
##1. Extraccion
task_1 = PythonOperator(
    task_id='extraer_data',
    python_callable=extraer_data,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=BC_dag,
)

##2. Transformacion
task_2 = PythonOperator(
    task_id='transformar_data',
    python_callable=transformar_data,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=BC_dag,
)

## 3.Conexion a base de datos
task_3= PythonOperator(
    task_id="conexion_BD",
    python_callable=conexion_redshift,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=BC_dag
)

## 4. Envio datos
task_4 = PythonOperator(
    task_id='cargar_data',
    python_callable=cargar_data,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=BC_dag,
)

## 5. Envio Reporte mail 
task_5 = PythonOperator(
    task_id='envio_reporte',
    python_callable=reporte_SMTP,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=BC_dag,
)


# Definicion orden de tareas
task_1 >> task_2 >> task_3 >> task_4 >> task_5