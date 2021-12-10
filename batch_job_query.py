"""
Script para convertir en lote los archivos tipo "dato-timestamp.parquet.gzip"
Debe ejecutarse dentro de la carpeta donde estén los archivos.
Requiere Python 3 o superior e instalar las librerías Pandas y Pyarrow* con pip3 install.

*Si Pyarrow da error al instalar via pip, debe hacerse
pip3 install --upgrade
Y luego instalar Cython a traves de pip3
"""
import os
import json
import psycopg2
import gzip
import pytz
# import schedule
import tempfile
import sqlalchemy as sql
import requests
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import dataframe as datos
from time import time
# from scipy.fft import fft, fftfreq
from datetime import datetime as dt
from datetime import date as dt_date
from datetime import timedelta as delta
# from datetime import timezone as tz
from pytz import timezone as tz

db_engine=sql.create_engine('postgresql://postgres:puentes123@52.67.122.51:5432/thingsboard')

sampling_freq = 120
CLT = tz('America/Santiago')
puente = "LA_MOCHITA"   #TODO Esto debe obtenerse de una query a la bd de inventario
main_dir = os.path.dirname(os.path.realpath(__file__))
work_folder = "DB_DATAFRAMES"
results_folder = "RESULTS"
hours_folder = "HOUR_DATA"
data_dir = main_dir+"/"+work_folder.replace("/", os.path.sep)
results_dir = main_dir+"/"+results_folder.replace("/", os.path.sep)
hours_dir = main_dir+"/"+hours_folder.replace("/", os.path.sep)

def getFileDataframe(data_dir,filename,sensor):
    df_out = pd.read_parquet(os.path.join(data_dir,filename), columns=["timestamp",sensor]) #Leer archivo parquet del sensor seleccionado
    df_out['timestamp'] = pd.to_datetime(df_out['timestamp'])
    return df_out

def makeDatasetFile(data_dir, filepath, sensor):
    init = True
    pqwriter = None
    file_count = 0
    total_rows = 0
    print("Extrayendo datos y Timestamp del sensor "+sensor+" a el archivo "+filepath)
    for filename in os.listdir(data_dir):
        path = os.path.join(data_dir, filename)     # generar direccion completa del archivo a leer
        if os.path.isfile(path) and path.endswith('.gzip'): # Revisar que el objeto a iterar sea un archivo y que sea un archivo con extensión .gzip
            x_vals = []
            y_vals = []
            z_vals = []
            file_count += 1
            curr_df = getFileDataframe(data_dir,filename,sensor)
            ts_vals = curr_df['timestamp'].values.tolist()
            data = curr_df[sensor]

            for triplet in data:
                x_vals.append(triplet["x"])
                y_vals.append(triplet["y"])
                z_vals.append(triplet["z"])

            proc_df = pd.DataFrame({'timestamp':ts_vals,'x':x_vals,'y':y_vals,'z':z_vals})
            proc_df['timestamp'] = pd.to_datetime(proc_df['timestamp'], unit='ms')
            #proc_df['timestamp'] = proc_df['timestamp'].dt.tz_localize('UTC')
            #proc_df['timestamp'] = proc_df['timestamp'].dt.tz_convert('America/Santiago')
            proc_df = proc_df.set_index("timestamp")
            table = pa.Table.from_pandas(proc_df)
            # print(table)
            if init:
                init = False
                pqwriter = pq.ParquetWriter(filepath,table.schema)
            pqwriter.write_table(table)
            total_rows += len(proc_df.index)
            # print("archivo "+filename+" adjuntado a archivo final\n")
    if pqwriter:
        pqwriter.close()

    return total_rows

def processBatch(batch):
    batch = batch.replace(-9999.0, np.nan) #EL VALOR -9999 REPRESENTA DATOS FALTANTES
    open = batch.head(1)
    max = batch.max()
    max_count = batch.loc[batch == max].count()
    min = batch.min()
    min_count = batch.loc[batch == min].count()
    close = batch.tail(1)
    fecha_ultimo_max = batch.loc[batch == max].tail(1).index.to_pydatetime()
    fecha_ultimo_min = batch.loc[batch == min].tail(1).index.to_pydatetime()
    mean = batch.mean()
    kurtosis = batch.kurtosis()

    output_df = pd.DataFrame({"timestamp":open.index.to_pydatetime(),"open":open.values,"max":max,"min":min,"close":close.values,"avg":mean,"kur":kurtosis})
    #output_df["timestamp"] = output_df["timestamp"].dt.tz_convert('America/Santiago')
    return output_df

def processDatasetFile(batch_size, dataset_path, results_filepath):
    init = True
    pqwriter = None
    # results_filepath = os.path.dirname(dataset_path)+os.path.sep+sensor+"results.parquet"
    print(results_filepath)
    _file = pa.parquet.ParquetFile(dataset_path)
    batches = _file.iter_batches(batch_size) #batches es generador, iterando sobre el archivo en lotes de filas

    batch_count = 0
    for batch in batches:
        batch_count += 1
        batch_df = batch.to_pandas()

        batch_x = batch_df['x']
        batch_y = batch_df['y']
        batch_z = batch_df['z']

        df_x = processBatch(batch_x)
        df_y = processBatch(batch_y)
        df_z = processBatch(batch_z)

        if not (df_x.size == df_y.size and df_x.size == df_z.size): # Si no son todos del mismo largo, eliminar elementos extra
            min_size = min([df_x.size, df_y.size, df_z.size])       # y usar el largo menor
            if df_x.size != min_size:
                diff = df_x.size - min_size
                df_x = df_x.iloc[:-diff]
            if df_y.size != min_size:
                diff = df_y.size - min_size
                df_y = df_y.iloc[:-diff]
            if df_z.size != min_size:
                diff = df_z.size - min_size
                df_z = df_z.iloc[:-diff]

        full_df = pd.DataFrame({"timestamp":df_x["timestamp"].values,"open_x":df_x["open"].values,"max_x":df_x["max"],"min_x":df_x["min"],"close_x":df_x["close"].values,"avg_x":df_x["avg"],"kur_x":df_x["kur"],
                                                                     "open_y":df_y["open"].values,"max_y":df_y["max"],"min_y":df_y["min"],"close_y":df_y["close"].values,"avg_y":df_y["avg"],"kur_y":df_y["kur"],
                                                                     "open_z":df_z["open"].values,"max_z":df_z["max"],"min_z":df_z["min"],"close_z":df_z["close"].values,"avg_z":df_z["avg"],"kur_z":df_z["kur"]})
        #full_df["timestamp"] = full_df["timestamp"].dt.tz_localize('UTC')
        #full_df["timestamp"] = full_df["timestamp"].dt.tz_convert('America/Santiago')
        full_df = full_df.set_index("timestamp")
        #print(full_df)
        table = pa.Table.from_pandas(full_df)
        if init:
            init = False
            pqwriter = pq.ParquetWriter(results_filepath,table.schema)
        pqwriter.write_table(table)
    if pqwriter:
        pqwriter.close()

    results = pd.read_parquet(results_filepath)

    return results

def read_sql_tmpfile(query):
    with tempfile.TemporaryFile() as tmpfile:
        copy_sql = "COPY ({query}) TO STDOUT WITH CSV {head}".format(
           query=query, head="HEADER"
        )
        conn = db_engine.raw_connection()
        cur = conn.cursor()
        cur.copy_expert(copy_sql, tmpfile)
        tmpfile.seek(0)
        df = pd.read_csv(tmpfile)
        return df

def query_data(fecha_i,fecha_f,sensor,id_sensor,eje):
    query = ("SELECT t.ts AS fecha, t.dbl_v FROM public.ts_kv as t "
            "JOIN public.device as d ON d.id = t.entity_id "
            "JOIN public.ts_kv_dictionary as dic ON t.key = dic.key_id "
            "WHERE (dic.key = '"+str(eje)+"') and (d.id = '"+str(id_sensor)+"') and (t.ts BETWEEN "+str(int(fecha_i.timestamp()*1000))+" and "+str(int(fecha_f.timestamp()*1000))+") "
            "GROUP BY fecha, t.dbl_v "
            "ORDER BY fecha ASC ")
    print("\nConsulta de datos para sensor:  "+sensor+" y eje: "+eje)
    # "Se realizará QUERY entre fechas:  "+query_date_start.strftime("%d/%m/%Y %H:%M:%S")+" -- "+query_date_end.strftime("%d/%m/%Y %H:%M:%S")
    print("DATAFRAME %s: " %eje)
    fecha_inicio = dt(fecha_i.year, fecha_i.month, fecha_i.day, fecha_i.hour, fecha_i.minute, fecha_i.second, fecha_i.microsecond)
    #fecha_inicio = fecha_inicio.astimezone(CLT)
    fecha_fin = dt(fecha_f.year, fecha_f.month, fecha_f.day, fecha_f.hour, fecha_f.minute, fecha_f.second, fecha_i.microsecond)
    #fecha_fin = fecha_fin.astimezone(CLT)

    start_time = time()
    # df = pd.read_sql_query(query,conexion) #QUERY A BASE DE DATOS
    df = read_sql_tmpfile(query)
    # print(df)

    if df.empty == False:
        df = df.rename(columns={"fecha":'timestamp'})
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        #df["timestamp"] = df["timestamp"].dt.tz_localize('UTC')
        #df["timestamp"] = df["timestamp"].dt.tz_convert('America/Santiago')
        df = df.set_index("timestamp")
        #print(df)

        # print("N de valores nulos: ", curr_df_x['dbl_v'].isna().sum())
        exp_len = pd.Timedelta(fecha_fin - fecha_inicio,'ms').total_seconds()*sampling_freq
        print("Numero de datos esperados en el DF: ", int(exp_len))
        if len(df.index) < int(exp_len):
            print("Numero de datos presentes es inferior a los datos esperados. Completando fechas faltantes.")
            head = df.head(1).index.to_pydatetime()[0]
            tail = df.tail(1).index.to_pydatetime()[0]

            diff1 = pd.Timedelta(head - fecha_inicio,'ms')
            diff2 = pd.Timedelta(fecha_fin - tail,'ms')
            if diff1.total_seconds() > 0.01:
                print("primera fila diferente, diferencia en segundos: ",diff1.total_seconds())
                print("head date ", head)
                print("expected date ", fecha_inicio)
                date_vals = list(df.index.values)
                data = df["dbl_v"].values

                r1 = pd.Timedelta(head - fecha_inicio,'ms').total_seconds()*sampling_freq+1
                #r = pd.date_range(start=fecha_inicio, end=head,periods=r1,tz='America/Santiago',closed='left')
                r = pd.date_range(start=fecha_inicio, end=head,periods=r1,closed='left')
                df_a = pd.DataFrame({'timestamp':r}).set_index("timestamp")
                df = pd.concat([df_a,df])
                df = df.replace(np.nan, -9999.0)

            if diff2.total_seconds() > 0.01:
                print("ultima fila diferente, diferencia en segundos: ", diff2.total_seconds())
                print("tail date ", tail)
                print("expected date ", fecha_fin)
                date_vals = list(df.index.values)
                data = df["dbl_v"].values

                r2 = pd.Timedelta(fecha_fin - tail,'ms').total_seconds()*sampling_freq+1
                #r = pd.date_range(start=tail, end=fecha_fin,periods=r2,tz='America/Santiago',closed='left')
                r = pd.date_range(start=tail, end=fecha_fin,periods=r2,closed='left')
                df_b = pd.DataFrame({'timestamp':r}).set_index("timestamp")
                df = pd.concat([df,df_b])
                df = df.replace(np.nan, -9999.0)

            if len(df.index) > int(exp_len):
                rows_to_delete = len(df.index)-int(exp_len)
                print("Filas extras detectadas, eliminando %d filas del final de DF" %rows_to_delete)
                # Delete the last n rows in the DataFrame
                df = df.drop(df.index[-rows_to_delete])
        elif len(df.index) > int(exp_len):
            rows_to_delete = len(df.index)-int(exp_len)
            print("Filas extras detectadas, eliminando %d filas del final de DF" %rows_to_delete)
            # Delete the last n rows in the DataFrame
            df = df.drop(df.index[-rows_to_delete])
        else:
            print("Numero correcto de datos en Query realizada.")
        elapsed_time = time() - start_time
        print(df)
        print("Tiempo Transcurrido en crear DF desde Query: %0.1f segundos." % elapsed_time)
    else:
        #TODO Dataframe vacío
        print("Dataframe Vacío, generando archivo vacío.")
        column_names = ["timestamp", "dbl_v"]
        df = pd.DataFrame(columns = column_names)

        r3 = pd.Timedelta(fecha_fin - fecha_inicio,'ms').total_seconds()*sampling_freq+1
        #r = pd.date_range(start=fecha_inicio, end=fecha_fin,periods=r3,tz='America/Santiago',closed='left')
        r = pd.date_range(start=fecha_inicio, end=fecha_fin,periods=r3,closed='left')
        df_b = pd.DataFrame({'timestamp':r})
        df = pd.concat([df,df_b])
        df = df.set_index("timestamp")
        df = df.replace(np.nan, -9999.0)
        # print(df)

    return df

def get_sensor_names():
    query = ("SELECT DISTINCT name as nombre_sensor, id "
            "FROM public.device "
            # "WHERE (type = 'Acelerometro') and (name like '%AC%')")
            "WHERE (type = 'Acelerómetro') and (name like '%AC%')")
    df = read_sql_tmpfile(query)
    return(df)

def generate_datafile(curr_date, sensor, uuid, filepath):
    init = True
    pqwriter = None
    total_rows_per_file=0 #Contador de filas totales en cada archivo diario
    for h in range(0,24):
        # print("variable h: ", h)
        query_date_start = dt(curr_date.year,curr_date.month,curr_date.day,h,00,00,0000)
        query_date_end = query_date_start+delta(hours=1)
        print("Se realizará QUERY entre fechas:  "+query_date_start.strftime("%d/%m/%Y %H:%M:%S")+" -- "+query_date_end.strftime("%d/%m/%Y %H:%M:%S"))

        curr_df_x = query_data(query_date_start,query_date_end,sensor,uuid,"x")
        curr_df_y = query_data(query_date_start,query_date_end,sensor,uuid,"y")
        curr_df_z = query_data(query_date_start,query_date_end,sensor,uuid,"z")

        print("largo query x,y,z: ", len(curr_df_x['dbl_v'].values), len(curr_df_y['dbl_v'].values), len(curr_df_z['dbl_v'].values))

        if not (curr_df_x.size == curr_df_y.size and curr_df_x.size == curr_df_z.size): # Si no son todos del mismo largo, eliminar elementos extra
            min_size = min([curr_df_x.size, curr_df_y.size, curr_df_z.size])       # y usar el largo menor
            if curr_df_x.size != min_size:
                diff = curr_df_x.size - min_size
                curr_df_x = curr_df_x.iloc[:-diff]
            if curr_df_y.size != min_size:
                diff = curr_df_y.size - min_size
                curr_df_y = curr_df_y.iloc[:-diff]
            if curr_df_z.size != min_size:
                diff = curr_df_z.size - min_size
                curr_df_z = curr_df_z.iloc[:-diff]

        #ts_vals = curr_df_x.index
        ts_vals = curr_df_x.index
        print("largo coreccion x,y,z: ", len(curr_df_x['dbl_v'].values), len(curr_df_y['dbl_v'].values), len(curr_df_z['dbl_v'].values))
        df = pd.DataFrame({'timestamp':ts_vals,'x':curr_df_x['dbl_v'].values,'y':curr_df_y['dbl_v'].values,'z':curr_df_z['dbl_v'].values})
        #df["timestamp"] = df["timestamp"].dt.tz_convert('America/Santiago')
        df = df.set_index("timestamp")
        # print(df)

        table = pa.Table.from_pandas(df)
        if init:
            init = False
            # print(table.schema)
            pqwriter = pq.ParquetWriter(filepath,table.schema,use_deprecated_int96_timestamps=True)
        pqwriter.write_table(table)
        total_rows_per_file += len(df.index)
    else:
        if pqwriter:
            pqwriter.close()
    return total_rows_per_file

def generate_results_file(total_rows_per_file, dataset_path, results_path):
    print("Procesando archivo de resultados.")
    rows_per_batch = int(total_rows_per_file / 300)
    print("rows per batch in file: ", rows_per_batch)
    start_time = time()
    results = processDatasetFile(rows_per_batch, dataset_path, results_path)
    elapsed_time = time() - start_time
    # print("DATAFRAME RESULTADOS: ", results)
    print("Tiempo Transcurrido en construir archivo "+os.path.basename(results_path)+": %0.1f segundos." % elapsed_time)

def daily_batch_job(data_dir, sensors, results_size, offset = 0):
    hoy = dt.now()
    offset = 0  #Offset de días hacia atrás para iniciar procesamiento, en caso de querer saltarse días más cercanos.
    print("\nBATCH JOB EJECUTADO CON FECHA: ", hoy)
    print("OFFSET DE %d DIAS ATRAS" %offset)
    fecha_final = dt(hoy.year, hoy.month, hoy.day,23,59,59) - delta(days=1+offset)# (hoy - 1 dia) pues el job se correrá en la madrugada para el día anterior
    fecha_inicial = dt(hoy.year, hoy.month, hoy.day,00,00,00) - delta(days=15)
    #fecha_inicial = dt(hoy.year, hoy.month, hoy.day,00,00,00) - delta(days=4)
    fecha_delete = dt(hoy.year, hoy.month, hoy.day,00,00,00) - delta(days=16)
    print("fecha inicial: ",fecha_inicial)
    print("fecha final: ",fecha_final)
    sensors_ids = get_sensor_names()
    print("dia inicial: ", fecha_inicial.day)
    print("dia final: ", fecha_final.day)
    date_diff = pd.Timedelta(fecha_final - fecha_inicial, 'ms')
    n_days = date_diff.days
    print("numero de dias: ", n_days)

    #ITERACION DIAS DESDE MAS RECIENTE A MAS ANTIGUO
    for d in range(0,n_days): #Se itera sobre cada día del rango entre las fechas inicial y final
        curr_date = fecha_final-delta(days=d)       # Iterar desde fecha más reciente (día anterior) a más antigua
        #curr_date = fecha_inicial+delta(days=d)      # Iterar desde fecha más antigua a más reciente
        for sensor in sensors:  #Loop externo itera sobre cada sensor, para crear un archivo por sensor
            print("\nITERACIÓN DE SENSOR: ",sensor)
            uuid = sensors_ids.loc[sensors_ids["nombre_sensor"]==sensor]["id"].values[0] #cruza de nombre sensor actual con tabla de uuid's
            print("UUID de sensor: ",uuid)
            filename = sensor+"-"+curr_date.strftime("%d_%m_%Y")+"_dataset.parquet"
            results_filename = sensor+"-"+curr_date.strftime("%d_%m_%Y")+"_results.parquet"
            filepath = data_dir+"/"+filename.replace("/", os.path.sep)
            results_filepath = results_dir+"/"+results_filename.replace("/", os.path.sep)
            exp_rows_per_file = int(delta(days=1).total_seconds()*sampling_freq)

            if filename not in os.listdir(data_dir): #SI NO EXISTE ARCHIVO CON EL NOMBRE CORRESPONDIENTE
                print("obtención de datos para el "+dt.strftime(curr_date, "%d/%m/%Y"))
                start_time = time()
                total_rows_per_file = generate_datafile(curr_date, sensor, uuid, filepath)
                elapsed_time = time() - start_time
                print("Tiempo Transcurrido en construir archivo de día "+curr_date.strftime("%d/%m")+": %0.1f segundos." % elapsed_time)

                if not os.path.exists(results_filepath): #SI NO EXISTE ARCHIVO DE RESULTADOS CORRESPONDIENTE
                    print("No se encuentra archivo de resultados, iniciando procesamiento.")
                    generate_results_file(total_rows_per_file, filepath, results_filepath) #Procesasr archivo resultados
            else:
                print("Archivo con el nombre %s ya existente." %filename) #SI YA EXISTE ARCHIVO CON ESE NOMBRE
                # curr_file_df = pd.read_parquet(filepath)
                # print(curr_file_df)
                pq_file = pq.ParquetFile(filepath)
                num_rows = pq_file.metadata.num_rows
                # print("Number of rows in Current File:", len(curr_file_df.index))
                print("Number of rows in Current File:", num_rows)
                print("Expected rows", exp_rows_per_file)
                if (exp_rows_per_file - num_rows) > 10: #El archivo detectado esta incompleto, se genera de nuevo (tolerancia: 10 filas de diferencia)
                # if len(curr_file_df.index) < exp_rows_per_file: #El archivo detectado esta incompleto, se genera de nuevo
                    print("Se detecta que archivo no contiene cantidad de datos esperados, reseteando e iniciando extracción de datos.")
                    #os.remove(filepath)
                    total_rows_per_file = generate_datafile(curr_date, sensor, uuid, filepath)

                    if not os.path.exists(results_filepath): #Si no existe archivo resultados
                        print("No se encuentra archivo de resultados, iniciando procesamiento.")
                        generate_results_file(total_rows_per_file, filepath, results_filepath) #Procesasr archivo resultados
                else:
                    print("Archivo correcto")
                    if not os.path.exists(results_filepath): #Si no existe archivo resultados
                        print("No se encuentra archivo de resultados, iniciando procesamiento.")
                        generate_results_file(num_rows, filepath, results_filepath) #Procesasr archivo resultados
    #BORRAR ARCHIVOS OBSOLETOS
    for sensor in sensors:
        file_to_delete = sensor+"-"+fecha_delete.strftime("%d_%m_%Y")+"_dataset.parquet"
        if os.path.exists(file_to_delete):
            os.remove(file_to_delete)


def hourly_batch_job(conexion, data_dir, sensors, results_size):
    hoy = dt.now()
    print("\nBATCH JOB DE HORA EJECUTADO EN : ", hoy)
    tiempo_inicial = dt(hoy.year, hoy.month, hoy.day, hoy.hour-1, 00, 00)
    tiempo_final = dt(hoy.year, hoy.month, hoy.day, hoy.hour, 00, 00)

    print("hora inicial: ",tiempo_inicial)
    print("hora final: ",tiempo_final)

    sensors_ids = get_sensor_names(conexion)

    #LOOP SENSORES
    for sensor in sensors:  #Loop externo itera sobre cada sensor, para crear un archivo por sensor
        # filepath = data_dir+sensor+"_dataset.parquet"
        init = True #Crear archivo nuevo por cada sensor en la misma hora
        pqwriter = None
        print("\nITERACIÓN DE SENSOR: ",sensor)
        uuid = sensors_ids.loc[sensors_ids["nombre_sensor"]==sensor]["id"].values[0] #cruza de nombre sensor actual con tabla de uuid's
        print("UUID de sensor: ",uuid)

        filepath = data_dir+"/"+sensor+"-"+tiempo_inicial.strftime("%d_%m_%Y-%H_%M_%S")+"-hour_data.parquet".replace("/", os.path.sep)

        print("Se realizará QUERY entre fechas:  "+tiempo_inicial.strftime("%d/%m/%Y %H:%M:%S")+" -- "+tiempo_final.strftime("%d/%m/%Y %H:%M:%S"))
        start_time = time()
        curr_df_x = query_data(conexion,tiempo_inicial,tiempo_final,sensor,uuid,"x")
        curr_df_y = query_data(conexion,tiempo_inicial,tiempo_final,sensor,uuid,"y")
        curr_df_z = query_data(conexion,tiempo_inicial,tiempo_final,sensor,uuid,"z")

        ts_vals = curr_df_x.index
        df = pd.DataFrame({'timestamp':ts_vals,'x':curr_df_x['dbl_v'],'y':curr_df_y['dbl_v'],'z':curr_df_z['dbl_v']})
        df["timestamp"] = df["timestamp"].dt.tz_convert('America/Santiago')
        df = df.set_index("timestamp")

        elapsed_time = time() - start_time
        print("Tiempo Transcurrido en construir archivo de hora "+tiempo_inicial.strftime("%H:%M:%S-%d/%m")+": %0.1f segundos." % elapsed_time)

        # print(df)
        print("datetime : ", df.head(1).index)

        table = pa.Table.from_pandas(df)
        # print(table)
        if init:
            init = False
            # print(table.schema)
            pqwriter = pq.ParquetWriter(filepath,table.schema,use_deprecated_int96_timestamps=True)
            # total_rows = len(df.index)
        pqwriter.write_table(table)
        # total_rows += len(df.index)
            # print("archivo "+filename+" adjuntado a archivo final\n")
        if pqwriter:
            pqwriter.close()

def get_remaining_time():
    curr_time = dt.now()   #Obtener tiempo inicial de la iteración
    next_iter_time = dt(curr_time.year,curr_time.month,curr_time.day,curr_time.hour,00,00,0000000)+delta(hours=1)
    remaining_time = next_iter_time - curr_time
    return remaining_time

if __name__ == '__main__':
    results_size = 300
    sensors = ["AC05","AC06","AC07","AC08","AC09","AC10","AC11","AC12","AC13","AC14","AC15","AC16","AC17","AC18"]

    #if work_folder not in os.listdir(main_dir):
    #    os.mkdir(data_dir)
    #if results_folder not in os.listdir(main_dir):
    #    os.mkdir(results_folder)
    #if hours_folder not in os.listdir(main_dir):
    #    os.mkdir(hours_folder)

    #conexion = psycopg2.connect(user="postgres", password="puentes123",
    #                            host="52.67.122.51", port="5432",
    #                            database="thingsboard")
    #esquema = ['public.','inventario_puentes.','llacolen.']
    hourly_fname_pattern = '*_hour-data.parquet'

    ##PROCESOS INDIVIDUALES
    # hourly_batch_job(conexion, hours_dir, sensors, results_size)
    daily_batch_job(data_dir, sensors, results_size)

    # while True:
    #     try:
    #         daily_batch_job(conexion, data_dir, sensors, results_size, offset = 2)
    #     except KeyboardInterrupt:
    #         break
    #     except Exception:
    #         print("ERROR. REINICIANDO")
    #         pass

    ##PROCESO PRINCIPAL CON SCHEDULER
    # while True:
    #     try:
    #         remaining_time = get_remaining_time()
    #         if remaining_time > delta(minutes=15) :   #Si restan menos de 15 minutos en la hora actual, esperar a la siguiente para realizar batch job
    #             hourly_batch_job(conexion, hours_dir, sensors, results_size)
    #             curr_time = dt.now()
    #             remaining_time = get_remaining_time()
    #             if curr_time.hour == 0 and remaining_time > delta(minutes=40): #AL INICIO DEL DIA HACER DAILY BATCH JOB
    #                 for dirpath, dirnames, files in os.walk(hours_dir):
    #                     if files: #IF archivos hourly de hora anterior presentes -> output: archivo dia anterior
    #                         print(dirpath, 'contiene archivos')
    #                         for file in files:
    #                             split_tup = os.path.splitext(file)
    #                             filename = split_tup[0]
    #                             filedata = filename.split('-')
    #                             sensor = filedata[0]
    #                             date = filedata[1]
    #                             time = filedata[2]
    #                             filetype = filedata[3]
    #                             print(sensor, date, time, filetype)
    #
    #                     if not files:
    #                         print(dirpath, 'se encuentra vacio')
    #
    #                 #ELSE REALIZAR DAILY BATCH JOB
    #                 daily_batch_job(conexion, data_dir, sensors, results_size, process_results = False)
    #                 #PROCESAR 7 y 14  DIAS CON DATOS NUEVOS
    #
    #             remaining_time = get_remaining_time()
    #             time.sleep(remaining_time.total_seconds())
    #         else:
    #             remaining_time = get_remaining_time()
    #             time.sleep(remaining_time.total_seconds())  #ESPERAR SIGUIENTE ITERACIÓN O REVISAR CAMBIO DE HORA AL FINAL (SIEMPRE REVISAR?)
    #     except:
    #         pass


    # for dirpath, dirnames, files in os.walk(data_dir):
    #     if files:
    #         print(dirpath, 'contiene archivos')
    #         for file in files:
    #             split_tup = os.path.splitext(file)
    #             filename = split_tup[0]
    #             extension = split_tup[1]
    #             sensor = filename.split('-')[0]
    #             filedata = filename.split('-')[1]
    #             filedata = date.split('_')
    #             print(sensor, filedata[0], filedata[1], filedata[2], filedata[3])
    #     if not files:
    #         print(dirpath, 'se encuentra vacio')




    # while True:
    #     start_time = dt.now()   #Obtener tiempo inicial de la iteración
    #     if start_time.minute < 45:
    #         #TODO DO WORK
    #     else:
    #         print("time left :", 60-start_time.minute)
    #         # time.sleep(60-start_time.minute)



    #time
    # schedule.every(1).minutes.do(do_stuff)
    # while True:
    #    schedule.run_pending()
    #    time.sleep(1)




    # AUTOMATICALLY RESTART SCRIPT
    # def main():
    # while True:
    #     try:
    #         your_logic()
    #     except:
    #         pass
