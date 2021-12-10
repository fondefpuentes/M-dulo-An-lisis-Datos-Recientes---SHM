import os
import sys
import pandas as pd
import numpy as np
import tempfile
import sqlalchemy as sql
import fnmatch
import unicodedata

import plotly
import plotly.graph_objects as go
from datetime import datetime as dt
from datetime import timedelta as td
from kaleido.scopes.plotly import PlotlyScope
currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)

db_engine=sql.create_engine('postgresql://postgres:puentes123@52.67.122.51:5432/thingsboard')

main_dir = os.path.dirname(os.path.realpath(__file__))+"/".replace("/", os.path.sep)
work_folder = "DB_DATAFRAMES/"
results_folder = "RESULTS/"
hours_folder = "HOUR_DATA/"
data_dir = main_dir+work_folder.replace("/", os.path.sep)
results_dir = main_dir+results_folder.replace("/", os.path.sep)
hours_dir = main_dir+hours_folder.replace("/", os.path.sep)

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

# Funcion que retorna los nombres de los tipos de sensores disponibles a seleccionar en el Dropdown
def tipos_sensores():
    query = ("SELECT DISTINCT type as tipo_sensor "
            "FROM public.device")
    df = read_sql_tmpfile(query)
    tipos = df['tipo_sensor'].tolist()
    tipos.sort(reverse=False)
    tipos_sensores = {}
    #for tipo in tipos:
    #    tipos_sensores[str(elimina_tildes(tipo)).lower().replace(' ', '-') ] = str(tipo)
    tipos_sensores[str(elimina_tildes(tipos[0])).lower().replace(' ', '-') ] = str(tipos[0])

    return dict(tipos_sensores)

# Funcion que retorna los nombres de los sensores disponibles a seleccionar en el Dropdown
def nombres_sensores(tipo_sensor):
    query = (   "SELECT DISTINCT name as nombre_sensor, id "
                "FROM public.device "
                "WHERE (type = '"+str(tipo_sensor)+"') and (name like '%AC%')"
                )

    df = read_sql_tmpfile(query)
    df = df.sort_values(by='nombre_sensor', ascending=True)
    df = df.set_index('nombre_sensor')
    df_dict = df.index.tolist()

    return df_dict

def get_available_dates():
    pattern = '*_results.parquet'
    date_list = []

    for file in fnmatch.filter(os.listdir(results_dir),pattern):
        split_tup = os.path.splitext(file)
        filename = split_tup[0]
        filedata = filename.split('-')
        date = filedata[1]
        date = date.split('_')
        d_obj = dt(int(date[2]),int(date[1]),int(date[0]))
        date_list.append(d_obj)
    date_list = list(dict.fromkeys(date_list))
    date_list = sorted(date_list)

    return date_list

def get_available_timestamps():
    pattern = '*_results.parquet'
    ts_list = []
    for file in fnmatch.filter(os.listdir(results_dir),pattern):
        split_tup = os.path.splitext(file)
        filename = split_tup[0]
        filedata = filename.split('-')
        date = filedata[1]
        date = date.split('_')
        d_obj = dt(int(date[2]),int(date[1]),int(date[0]))
        ts_list.append(dt.timestamp(d_obj))
    ts_list = list(dict.fromkeys(ts_list))
    ts_list = sorted(ts_list)

    return ts_list

def get_available_sensors():
    pattern = '*_results.parquet'
    sensores_list = []
    for file in fnmatch.filter(os.listdir(results_dir),pattern):
        split_tup = os.path.splitext(file)
        filename = split_tup[0]
        filedata = filename.split('-')
        sensor = filedata[0]
        sensores_list.append(sensor)
    sensores_list = list(dict.fromkeys(sensores_list))
    return sensores_list

def get_available_types():
    pattern = '*_results.parquet'
    type_list = []
    for file in fnmatch.filter(os.listdir(results_dir),pattern):
        split_tup = os.path.splitext(file)
        filename = split_tup[0]
        filedata = filename.split('-')
        type = filedata[0]
        type_list.append(type)
    type_list = list(dict.fromkeys(type_list)) #Retirar los elementos repetidos
    return type_list

sensores = get_available_sensors()

def get_dict_key_from_value(dict,val):
    for key, value in dict.items():
         if val == value:
             return key

    return "key doesn't exist"

def get_filename(sensor,date_obj):
    format = "%Y-%m-%d"
    try:
        filepath = results_dir+sensor+"-"+date_obj.strftime("%d_%m_%Y")+"_results.parquet".replace("/", os.path.sep)
    except:
        filepath = 'None'
    return filepath

#Funcion para crear el dataframe a utilizar en el grafico OHLC, ademas el valor de la columna avg se utiliza para para el histograma
#La funcion requiere de una fecha inicial, para calcular a partir de esta los rangos de fechas
#La frecuencia, la cual corresponde al intervalo para generar el rango de fechas (12seg,288seg,2016seg y 4032seg)
#Y por ultimo requiere del nombre del sensor
def datos_ace(sensor,tipo_sensor,eje,ventana_tiempo):
    end_date = fecha_final(tipo_sensor)
    df = pd.DataFrame()
    new_df = pd.DataFrame()
    if ventana_tiempo == '1D':
        filename = get_filename(sensor, end_date)
        df = pd.read_parquet(filename, columns=["open_"+eje.lower(),"max_"+eje.lower(),"min_"+eje.lower(),"close_"+eje.lower(),"avg_"+eje.lower()])
        new_df = df.rename(columns={"open_"+eje.lower(): 'open',"max_"+eje.lower(): 'max',"min_"+eje.lower(): 'min',"close_"+eje.lower(): 'close',"avg_"+eje.lower():'avg'})
    elif ventana_tiempo == '7D':
        start_date = end_date - td(days=7)
        days = pd.date_range(start=start_date, end=end_date, freq='D', closed='right')
        for day in days:
            filename = get_filename(sensor, day)
            if os.path.isfile(filename):
                curr_df = pd.read_parquet(filename, columns=["open_"+eje.lower(),"max_"+eje.lower(),"min_"+eje.lower(),"close_"+eje.lower(),"avg_"+eje.lower()])
                curr_df = curr_df.rename(columns={"open_"+eje.lower(): 'open',"max_"+eje.lower(): 'max',"min_"+eje.lower(): 'min',"close_"+eje.lower(): 'close',"avg_"+eje.lower():'avg'})
                if df.empty:
                    df = curr_df
                else:
                    frames = [df,curr_df]
                    df = pd.concat(frames)
            else:
                print("archivo no encontrado, omitiendo.")
                pass
        # new_df = df.resample('2016S').mean()
        new_df = df
    elif ventana_tiempo == '14D':
        start_date = end_date - td(days=14)
        days = pd.date_range(start=start_date, end=end_date, freq='D', closed='right')
        for day in days:
            filename = get_filename(sensor, day)
            if os.path.isfile(filename):
                curr_df = pd.read_parquet(filename, columns=["open_"+eje.lower(),"max_"+eje.lower(),"min_"+eje.lower(),"close_"+eje.lower(),"avg_"+eje.lower()])
                curr_df = curr_df.rename(columns={"open_"+eje.lower(): 'open',"max_"+eje.lower(): 'max',"min_"+eje.lower(): 'min',"close_"+eje.lower(): 'close',"avg_"+eje.lower():'avg'})
                if df.empty:
                    df = curr_df
                else:
                    frames = [df,curr_df]
                    df = pd.concat(frames)
            else:
                print("archivo no encontrado, omitiendo.")
                pass
        # new_df = df.resample('4032S').mean()
        new_df = df
    return new_df

def datos_histograma(sensor,tipo_sensor,eje,ventana_tiempo):
    dict_sensores = nombres_sensores("Acelerómetro")
    # key_list = list(dict_sensores.keys())

    end_date = fecha_final(tipo_sensor)
    df = pd.DataFrame()
    curr_df = pd.DataFrame()
    if ventana_tiempo == '1D':
        filename = get_filename(sensor, end_date)
        curr_df = pd.read_parquet(filename, columns=["avg_"+eje.lower()])
        df = curr_df.rename(columns={"avg_"+eje.lower():'avg'})
    elif ventana_tiempo == '7D':
        start_date = end_date - td(days=7)
        days = pd.date_range(start=start_date, end=end_date, freq='D', closed='right')
        for day in days:
            filename = get_filename(sensor, day)
            if os.path.isfile(filename):
                curr_df = pd.read_parquet(filename, columns=["avg_"+eje.lower()])
                curr_df = curr_df.rename(columns={"avg_"+eje.lower():'avg'})
                if df.empty:
                    df = curr_df.copy(deep=True)
                else:
                    frames = [df,curr_df]
                    df = pd.concat(frames)
            else:
                print("archivo no encontrado, omitiendo.")
                pass
        # new_df = df.resample('2016S').mean()
        df = df.resample('2016S').mean()
    elif ventana_tiempo == '14D':
        start_date = end_date - td(days=14)
        days = pd.date_range(start=start_date, end=end_date, freq='D', closed='right')
        for day in days:
            filename = get_filename(sensor, day)
            if os.path.isfile(filename):
                curr_df = pd.read_parquet(filename, columns=["avg_"+eje.lower()])
                curr_df = curr_df.rename(columns={"avg_"+eje.lower():'avg'})
                if df.empty:
                    df = curr_df.copy(deep=True)
                else:
                    frames = [df,curr_df]
                    df = pd.concat(frames)
            else:
                print("archivo no encontrado, omitiendo.")
                pass
        # new_df = df.resample('4032S').mean()
        df = df.resample('4032S').mean()
    return df

# Funcion que elimina tildes
def elimina_tildes(cadena):
    sin = ''.join((c for c in unicodedata.normalize('NFD',cadena) if unicodedata.category(c) != 'Mn'))
    return sin

# Funcion que retorna la minima fecha que existe en la base de datos
def fecha_inicial(tipo_sensor):
    if tipo_sensor == 'acelerometro' or tipo_sensor == 'Acelerometro' or tipo_sensor == 'Acelerómetro':
        fecha = get_available_dates()[0]
        # fecha = fecha.strftime("%Y-%m-%d")
        print("fecha inicial: ", fecha)
        return fecha
    else:
        print("TIPO NO VALIDO")
        pass

# Funcion que retorna la ultima fecha que existe en la base de datos
def fecha_final(tipo_sensor):
    if tipo_sensor == 'acelerometro' or tipo_sensor == 'Acelerometro' or tipo_sensor == 'Acelerómetro':
        fecha = get_available_dates()[-1]
        # fecha = fecha.strftime("%Y-%m-%d")
        # print("fecha final: ",fecha)
        return fecha
    else:
        print("TIPO NO VALIDO")
        pass

# Funcion que retorna la minima fecha que existe en la base de datos
def timestamp_inicial(tipo_sensor):
    if tipo_sensor == 'acelerometro' or tipo_sensor == 'Acelerometro' or tipo_sensor == 'Acelerómetro':
        fechas = get_available_timestamps()
        return fechas[0]/1000
    else:
        pass

# Funcion que retorna la ultima fecha que existe en la base de datos
def timestamp_final(tipo_sensor):
    if tipo_sensor == 'acelerometro' or tipo_sensor == 'Acelerometro' or tipo_sensor == 'Acelerómetro':
        fechas = get_available_timestamps()
        return fechas[-1]/1000
    else:
        pass

#Funcion que cuenta la cantidad de dias entre 2 fechas
def dias_entre_fechas(fecha_ini,fecha_fin):
    #print('dias entre fechas')
    return abs(fecha_fin - fecha_ini).days

# Funcion que dado un dataframe y un sensor especifico, calcula el promedio, maximo, minimo, cuantas repeticiones de maximos, cuantas repeticiones de minimos
# y entrega las fechas de la ultima repeticon de maximo y minimo de todo el dataframe que se le entrega
# estos datos son visualizados sobre el grafico OHLC
def datos_mini_container(df):
    if (len(df) > 0):
        df['max'] = df['max'].round(decimals = 3)
        df['min'] = df['min'].round(decimals = 3)
        df['avg'] = df['avg'].round(decimals = 3)
        max = df['max'].max()
        min = df['min'].min()
        avg = df['avg'].mean()
        promedio = round(avg,3)
        maximo = round(max,3)
        minimo = round(min,3)
        count_max = 'N° Veces: '+str(df['max'].tolist().count(max))
        count_min = 'N° Veces: '+str(df['min'].tolist().count(min))

        index = df.index
        max_condition = df["max"] == max
        max_indices = index[max_condition]
        max_indices_list = max_indices.tolist()
        fecha_ultimo_max = max_indices_list.pop().strftime("%d/%m/%Y %H:%M:%S")

        min_condition = df["min"] == min
        min_indices = index[min_condition]
        min_indices_list = min_indices.tolist()
        fecha_ultimo_min = min_indices_list.pop().strftime("%d/%m/%Y %H:%M:%S")
    else:
        promedio,maximo,minimo,fecha_ultimo_max,fecha_ultimo_min,count_max,count_min = '---','---','---','---','---','---','---'

    return promedio,maximo,minimo,fecha_ultimo_max,fecha_ultimo_min,count_max,count_min

# Funcion que retorna dependiendo de la frecuencia seleccionada su equivalente para incluirlo en la descripcion del grafico OHLC
def titulo_OHLC(freq):
    if freq == '12S':
        freq = '1 hora'
    elif freq == '288S':
        freq = '1 dia'
    elif freq == '2016S':
        freq = '7 dias'
    elif freq == '4032S':
        freq = '14 dias'
    return freq

# Funcion que retorna dependiendo de la frecuencia seleccionada su equivalente para incluirlo en la descripcion del grafico histograma
def titulo_freq_datos(freq):
    if freq == '12S':
        freq = '12 seg'
    elif freq == '288S':
        freq = '4 min y 48 seg'
    elif freq == '2016S':
        freq = '33 min y 36 seg'
    elif freq == '4032S':
        freq = '1 hr, 7 min y 12 seg'
    return freq

#Funcion que retorna el rango de fecha para incluirlo en el titulo de los graficos
def fecha_titulo(fecha_inicial,freq):
    if freq == '288S':
        sum_fecha = td(days=1)
    elif freq == '2016S':
        sum_fecha = td(days=7)
    elif freq == '4032S':
        sum_fecha = td(days=14)
    fecha_final = str(fecha_inicial + sum_fecha)
    fecha_inicial = str(fecha_inicial)
    return fecha_inicial,fecha_final

# Funcion que crea el dataframe para una cajita del grafico boxplot, ya que por cada una de ellas se seleccionan 300 datos que son el
# promedio de un rango de tiempo, este rango de tiempo depende de la frecuencia que puede ser 12seg, 288seg, 2016seg y 4032seg
# la obtencion de estos datos se realiza de la misma forma que para el grafico OHLC
def datos_box(sensor,tipo_sensor,eje,ventana_tiempo):
    end_date = fecha_final(tipo_sensor)
    df = pd.DataFrame()
    curr_df = pd.DataFrame()
    days=[]
    if ventana_tiempo == '1D':
        filename = get_filename(sensor, end_date)
        curr_df = pd.read_parquet(filename, columns=["avg_"+eje.lower()])
        df = curr_df.rename(columns={"avg_"+eje.lower():'avg'})
        days = end_date
    elif ventana_tiempo == '7D':
        start_date = end_date - td(days=7)
        days = pd.date_range(start=start_date, end=end_date, freq='D', closed='right')
        for day in days:
            filename = get_filename(sensor, day)
            if os.path.isfile(filename):
                curr_df = pd.read_parquet(filename, columns=["avg_"+eje.lower()])
                curr_df = curr_df.rename(columns={"avg_"+eje.lower():'avg'})
                if df.empty:
                    df = curr_df.copy()
                else:
                    frames = [df,curr_df]
                    df = pd.concat(frames)
            else:
                print("archivo no encontrado, omitiendo.")
                pass
        # new_df = df.resample('2016S').mean()
        df = df.resample('2016S').mean()
        #new_df = df
    elif ventana_tiempo == '14D':
        start_date = end_date - td(days=14)
        days = pd.date_range(start=start_date, end=end_date, freq='D', closed='right')
        for day in days:
            filename = get_filename(sensor, day)
            if os.path.isfile(filename):
                curr_df = pd.read_parquet(filename, columns=["avg_"+eje.lower()])
                curr_df = curr_df.rename(columns={"avg_"+eje.lower():'avg'})
                if df.empty:
                    df = curr_df
                else:
                    frames = [df,curr_df]
                    df = pd.concat(frames)
            else:
                print("archivo no encontrado, omitiendo.")
                pass
        # new_df = df.resample('4032S').mean()
        df = df.resample('4032S').mean()
        #new_df = df
    df['eje'] = [eje.lower()]*len(df.index)

    # return df,days
    return df

# Funcion que retorna dependiendo de la frecuencia seleccionada su equivalente para incluirlo en la descripcion del grafico boxplot
def titulo_box(freq):
    if freq == '12S':
        freq = '5 min'
    elif freq == '288S':
        freq = '2 horas'
    elif freq == '2016S':
        freq = '12 horas'
    elif freq == '4032S':
        freq = '24 horas'
    return freq
