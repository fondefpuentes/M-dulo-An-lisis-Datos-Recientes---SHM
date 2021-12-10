"""
Programa principal del servidor Dash utilizado en la plataforma como módulo Datos Recientes.
Utiliza dataframe.py para obtener datos desde los archivos procesados y bd
Para el diseño web del módulo se utiliza layout.py
"""

import dash
import dataframe as datos
import layout as layout
import plotly.express as px
import pandas as pd
import plotly.graph_objects as go
from dash.dependencies import Input, Output,State
from datetime import datetime as dt
#import numpy as np

#Declaración de aplicación Dash para usar como servidor
app = dash.Dash(__name__,meta_tags=[{"name": "viewport", "content": "width=device-width"}], url_base_pathname="/dash/", assets_folder="./assets")
app.layout = layout.datos_recientes_layout
server = app.server

cantidad_sensores = '1-sensor'

#Inicialización de servidor PLotly Dash
def init_plotly(server):
    app.title = 'Datos Recientes - Plataforma de Monitoreo Salud Estructural'
    app.layout = layout.datos_recientes_layout
    app.server = server
    return app.server

#Funcion que construye y actualiza el grafico OHLC, a la vez retorna los valores utilizados en el panel de promedio y conteo min max.
@app.callback([Output('valor-promedio', 'children'), Output('valor-max', 'children'), Output('valor-min', 'children'),
              Output('fecha-valor-max','children'), Output('fecha-valor-min','children'), Output('num-valor-max','children'),
              Output('num-valor-min','children'),Output('grafico-principal','figure'), Output("ohlc_text", "children")],
              Input('boton-aceptar', 'n_clicks'),
              [State('elegir-sensor','value'),State('elegir-tipo-sensor','value'),State('elegir-eje','value'),State('ventana-tiempo','value')])
def update_grafico_principal(n_clicks,sensor,tipo_sensor,ejes,ventana_tiempo):
    segundos = 0
    antiguedad = "1 día"

    if ventana_tiempo == "1D":
        segundos = int((24*3600)/300)
        antiguedad = "1 día"
    elif ventana_tiempo == "7D":
        segundos = int((7*24*3600)/300)
        antiguedad = "7 días"
    elif ventana_tiempo == "14D":
        segundos = int((14*24*3600)/300)
        antiguedad = "14 días"
    else:
        pass

    texto = "Datos cada "+str(segundos)+" segundos de "+sensor+" durante "+antiguedad+" (seleccione para ampliar)"

    if len(ejes) < 1 :
        ejes.append('x')

    #Se inicializan variables
    df = pd.DataFrame()
    fig_principal = go.Figure()
    #Listas que conteneran cada trace generado por cada dataframe creado para poder visualizarlos en una grafica
    trace_principal = []
    if n_clicks >= 0:
        #Dependiendo del tipo de sensor se crean visualizaciones distintas
        if tipo_sensor == 'Acelerómetro':
            if cantidad_sensores == '1-sensor':
                # La variable df contiene el dataframe que se utiliza para generar los graficos OHLC e histograma
                full_df = pd.DataFrame()
                colors = ['#e6194b', '#3cb44b', '#ffe119', '#4363d8', '#f58231', '#911eb4']
                count = 0
                new_count_de = 0
                new_count_in = 1

                for eje in ejes:
                    df = datos.datos_ace(sensor,tipo_sensor,eje,ventana_tiempo)
                    if full_df.empty:
                        full_df = df.copy()
                    else:
                        frames = [full_df,df]
                        full_df = pd.concat(frames)

                    if ventana_tiempo ==  "7D":
                        df = df.resample('2016S').mean()
                    elif ventana_tiempo == "14D":
                        df = df.resample('4032S').mean()
                    else:
                        pass
                    # Aqui se crea el grafico OHLC
                    trace_principal.append(go.Ohlc(x=df.index,
                        open=df['open'],
                        high=df['max'],
                        low=df['min'],
                        close=df['close'],
                        increasing_line_color= colors[new_count_in],
                        decreasing_line_color= colors[new_count_de],
                        name=str(sensor)+' eje: '+str(eje),
                        showlegend=True))
                    count = count + 1
                    new_count_in = new_count_in + 2
                    new_count_de = new_count_de + 2

                fig_principal = go.Figure(data=trace_principal)
                fig_principal.update_layout(yaxis_title='Aceleración(cm/s2)')
                promedio,maximo,minimo,fecha_ultimo_max,fecha_ultimo_min,count_max,count_min = datos.datos_mini_container(full_df)

        return promedio,maximo,minimo,fecha_ultimo_max,fecha_ultimo_min,count_max,count_min,fig_principal,texto

#Funcion que actualiza el grafico histograma 
@app.callback([Output('grafico-2','figure'),Output("histogram_text", "children")],
              Input('boton-aceptar', 'n_clicks'),
              [State('elegir-sensor','value'),State('elegir-tipo-sensor','value'),State('elegir-eje','value'),State('ventana-tiempo','value')])

def update_grafico_2(n_clicks,sensor,tipo_sensor,ejes,ventana_tiempo):
    antiguedad = ""
    minutos = ""
    if ventana_tiempo == "1D":
        minutos = int((24*60)/300)
        antiguedad = "1 día"
    elif ventana_tiempo == "7D":
        minutos = int((7*24*60)/300)
        antiguedad = "7 días"
    elif ventana_tiempo == "14D":
        minutos = int((14*24*60)/300)
        antiguedad = "14 días"
    else:
        pass

    texto = "Distribución de promedios obtenidos cada "+str(minutos)+" minutos de "+sensor+" durante "+antiguedad+" (seleccione para ampliar)"

    if len(ejes) < 1 :
        ejes.append('x')

    #Se inicializan variables
    df = pd.DataFrame()
    fig_2 = go.Figure()
    if n_clicks >= 0:

        trace_sec2 = []
        colors = ['#4363d8', '#f58231', '#911eb4']
        count = 0
        ####################
        for eje in ejes:
            df = datos.datos_histograma(sensor,tipo_sensor,eje,ventana_tiempo)
            trace_sec2.append(
                # go.Histogram(x=df['avg'], showlegend=True, marker_color = colors[count], name=sensor+"_"+eje, nbinsx=20)
                go.Histogram(x=df['avg'], showlegend=True, marker_color = colors[count], name=sensor+"_"+eje)
            )
            count += 1
            fig_2 = go.Figure(data=trace_sec2)
            fig_2.update_layout(#barmode='stack',
                                xaxis_title_text='Promedios cada '+str(minutos)+' minutos', # xaxis label
                                yaxis_title_text='Cantidad de datos', # yaxis label
                                bargap=0.2, # gap between bars of adjacent location coordinates
                                bargroupgap=0.1 # gap between bars of the same location coordinates)
                                )
        ################################

        # curr_df = pd.DataFrame()
        # for eje in ejes:
        #     curr_df = datos.datos_histograma(sensor,tipo_sensor,eje,ventana_tiempo)
        #     if df.empty:
        #         df = curr_df.copy()
        #     else:
        #         frames = [df,curr_df]
        #         df = pd.concat(frames)
        # df['date'] = [d.date() for d in df.index]
        #
        # fig_2 = px.histogram(df, x='avg',nbins=20)
        # fig_2.update_layout(# barmode='stack',
        #                     xaxis_title_text='Promedios cada '+str(minutos)+' minutos', # xaxis label
        #                     yaxis_title_text='Cantidad de datos', # yaxis label
        #                     bargap=0.2, # gap between bars of adjacent location coordinates
        #                     bargroupgap=0.1 # gap between bars of the same location coordinates)
        #                     )

        # df = px.data.tips()
        # # create the bins
        # counts, bins = np.histogram(df.total_bill, bins=range(0, 60, 5))
        # bins = 0.5 * (bins[:-1] + bins[1:])
        #
        # fig = px.bar(x=bins, y=counts, labels={'x':'total_bill', 'y':'count'})
        # fig.show()

        return fig_2, texto


#Funcion que actualiza el grafico Boxplot
@app.callback([Output('grafico-1','figure'),Output("boxplot_text", "children")],
              Input('boton-aceptar', 'n_clicks'),
              [State('elegir-sensor','value'),State('elegir-tipo-sensor','value'),State('elegir-eje','value'),State('ventana-tiempo','value')])

def update_grafico_1(n_clicks,sensor,tipo_sensor,ejes,ventana_tiempo):
    antiguedad = ""
    minutos = ""
    if ventana_tiempo == "1D":
        minutos = int((24*60)/300)
        antiguedad = "1 día"
    elif ventana_tiempo == "7D":
        minutos = int((7*24*60)/300)
        antiguedad = "7 días"
    elif ventana_tiempo == "14D":
        minutos = int((14*24*60)/300)
        antiguedad = "14 días"
    else:
        pass
    texto = "Distribución en cuartiles de promedios cada "+str(minutos)+" minutos de "+sensor+" durante "+antiguedad+" (seleccione para ampliar)"

    if len(ejes) < 1 :
        ejes.append('x')
    #Se inicializan variables
    df = pd.DataFrame()
    fig_1 = go.Figure()
    #Listas que conteneran cada trace generado por cada dataframe creado para poder visualizarlos en una grafica
    trace_principal = []
    if n_clicks >= 0:
        #Dependiendo del tipo de sensor se crean visualizaciones distintas
        if tipo_sensor == 'Acelerómetro':
            if cantidad_sensores == '1-sensor':
                curr_df = pd.DataFrame()
                for eje in ejes:
                    curr_df = datos.datos_box(sensor,tipo_sensor,eje,ventana_tiempo)
                    if df.empty:
                        df = curr_df.copy()
                    else:
                        frames = [df,curr_df]
                        df = pd.concat(frames)
                df['date'] = [d.date() for d in df.index]

                fig_1 = px.box(df, x='date', y='avg', color='eje')
                fig_1.update_layout(
                                    xaxis_title_text='', # xaxis label
                                    yaxis_title_text='Promedios cada '+str(minutos)+' minutos' # yaxis label
                                    )

                # Titulos para el grafico OHLC y Boxplot
                # titulo_box = '2 horas'
                # titulo_OHLC = '1 dia'
                # titulo_box = datos.titulo_box(ventana_tiempo)
                # titulo_OHLC = datos.titulo_OHLC(ventana_tiempo)
            return fig_1, texto


# Main
if __name__ == "__main__":
    app.title = "Datos Recientes - Plataforma de Monitoreo de Salud Estructural"
    app.layout = layout.datos_recientes_layout
    app.run_server(debug=True, use_reloader=True)
    server = app.server
