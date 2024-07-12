#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#Forecast per ragioni sociali con pdr giornalieri con almeno 12 mesi di storico (Prophet)


#librerie
import os
import sys
import datetime
import itertools
import math

# data transforamtion and manipulation
import pandas as pd
import numpy as np
from math import sqrt
#import pandas_profiling


#from prophet import Prophet
#from matplotlib import pyplot as plt
#import pickle

# prevent crazy long numpy prints
np.set_printoptions(precision=4, suppress=True)

# prevent crazy long pandas prints
#pd.options.display.max_columns = 1000
#pd.options.display.max_rows = 1000
#pd.set_option('display.float_format', lambda x: '%.5f' % x)
#pd.set_option('display.expand_frame_repr', False)
#pd.set_option('display.max_columns', 1000)
#pd.set_option('display.width', 1000)


# remove warnings
import warnings
warnings.filterwarnings('ignore')


# plotting and plot styling
#import matplotlib.pyplot as plt
#import matplotlib as mpl
#import seaborn as sns
#import plotly.express as px
#import plotly.graph_objects as go


# set params
#plt.rcParams['figure.figsize'] = (16,10)
#plt.rcParams['axes.grid'] = True
#plt.rcParams['axes.labelsize'] = 14
#plt.rcParams['xtick.labelsize'] = 12
#plt.rcParams['ytick.labelsize'] = 12
#plt.rcParams['text.color'] = 'k'
#plt.style.use('fivethirtyeight')


from prophet import Prophet
from prophet.diagnostics import cross_validation

# metrics 
#from random import random
#from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error, median_absolute_error, mean_squared_log_error

#date
import datetime

import pyodbc

ragsoc = list()
idzona = list()
result = dict()
error = dict()
indice = list()


# In[90]:


#Funzione per il preprocessing del dataset che gestisce valori mancanti o valori nulli, controlla che la serie sia in ordine di data e interpola i dati mancanti con una serie lineare
def Preprocessing_dataframe(df):
    first = df.columns[0]
    second = df.columns[1]
    df[first] = pd.to_datetime(df[first])
    # sort:
    df.sort_values(by=[first], axis=0, ascending=True, inplace=True)

    df.drop_duplicates(subset=first, keep='last', inplace=True)
    if len(df.index) > 0:
        df = df.set_index(first)
        date_range = pd.date_range(start=min(df.index), end=max(df.index), freq='D')

        df = df.reindex(date_range)
        df[second].interpolate(method='linear', inplace=True)
    else:
        df = df.set_index(first)

    return df


#Funzione Prophet per il forecast con parametri testati che ritorna il dataframe di forecast
def run_prophet(df2, delta):

    m = Prophet(yearly_seasonality=35, weekly_seasonality = 5, daily_seasonality = 10, seasonality_mode='multiplicative', changepoint_prior_scale = 0.01, seasonality_prior_scale = 30, uncertainty_samples = False)
    m.add_seasonality(name='monthly', period=30.5, fourier_order=15, mode='multiplicative')
    m.add_country_holidays(country_name='IT')

    m.fit(df2)

    future = m.make_future_dataframe(periods=delta, freq = 'D')

    forecast = m.predict(future)

    return forecast


# In[93]:


#salvataggio della datamese utilizzata per le tabelle di anagrafica
from datetime import datetime
from datetime import date
from datetime import timedelta
from dateutil.relativedelta import relativedelta

oggi = date.today()
#dd = date(oggi.year, oggi.month, 1)


if __name__ == "__main__":
    #parametri inseriti in fase di esecuzione con all'interno data inizio e fine del forecast desiderato
    a = sys.argv[1]
    b = sys.argv[2]
    datainizio = datetime.strptime(str(a), '%Y-%m-%d')
    datafine = datetime.strptime(str(b), '%Y-%m-%d')
    datainizio = datainizio.date()
    datafine = datafine.date()

    dd = date(datainizio.year, datainizio.month, 1)

    #creazione delle liste di ragioni sociali e indici per l'algoritmo in questione, prese dalla tabella Anagrafica_RagSoc_Tipo
    connC = pyodbc.connect(
        "Driver={SQL Server Native Client 11.0};"
        "Server=db_sql_prod;"
        "Database=DB_Power;"
        "Trusted_Connection=yes;"
    )
    cursorC = connC.cursor()

    cursorC.execute("""select distinct Cognome_Rag_Soc, Indice from DB_Gas.dbo.tbl_Anagrafica_RagSoc_Tipo where Mesi >= 12 and Tipo = 'G' and Datamese = ?""", dd)

    x = cursorC.fetchall()

    if len(x) == 0:
        dd = dd - relativedelta(months=1)

        cursorC.execute("""select distinct Cognome_Rag_Soc, Indice from DB_Gas.dbo.tbl_Anagrafica_RagSoc_Tipo where Tipo = 'G' and Datamese = ?""", dd)

        x = cursorC.fetchall()

    for i in range(len(x)):
        ragsoc.append(x[i][0])
        indice.append(x[i][1])

    connC.close()


    #ciclo for sulle ragioni sociali
    for cl in range(len(ragsoc)):

#### Codice per test su singola ragione sociale
###############################################################################
    #result.clear()
    #ragsoc.clear()
    #indice.clear()
    #ragsoc.append('CARCANO ANTONIO SPA')
    #indice.append('Fisso')
    #cl = 0
###############################################################################

        connC = pyodbc.connect(
        "Driver={SQL Server Native Client 11.0};"
        "Server=db_sql_prod;"
        "Database=DB_Gas;"
        "Trusted_Connection=yes;"
        )
        cursorC = connC.cursor()


        #cursorC.execute("""select min(Dt_Fine_Fornitura)
        #                from Db_CDg.dbo.tbl_Anagrafica_Datamax_GAS where Cognome_Rag_Soc = ? 
        #                and Indice = ? and Datamese = ?""", ragsoc[cl], indice[cl], dd)
        #q = cursorC.fetchone()

        #if q[0] < datafine:
        #    continue

        #ricavo il consumo storico dagli SBG per la ragione sociale in questione
        cursorC.execute("""select Data, isnull(sum(Consumo), 0)
        from Db_Gas.dbo.tbl_Consumo_SBG_Giornalieri g
        join Db_CDg.dbo.tbl_Anagrafica_Datamax_GAS a on a.pdr = g.pdr and Datamese = ?
        and Cognome_Rag_Soc = ? and indice = ?
        group by Data
        order by Data""", dd, ragsoc[cl], indice[cl])
        t = cursorC.fetchall()

        precons = list()
        data = list()

        for i in range(len(t)):
            data.append(t[i][0])
            precons.append(float(t[i][1]))

        connC.close()

        #creo il dataframe con Data e Consumo consuntivo
        g = {'ds':data, 'y':precons}

        df = pd.DataFrame(g)

        #Pulizia e preprocessing del dataframe
        df = Preprocessing_dataframe(df)

        df = pd.DataFrame(df).reset_index().rename(columns={
                                                                    'index':'ds', 
                                                                })

        #setto la variabile d all'ultima data disponibile dello storico
        if len(data) > 0:
            d = data[len(data)-1]
        else:
            d = datafine

        #calcolo il delta fra datafine e d per conoscere il periodo (in giorni) per il quale calcolare il forecast
        delta = datafine - d

        #Nuovo dataframe con gli stessi dati settato per il lancio di Prophet
        df2 = df[['ds', 'y']]

        #funzione Prophet
        if len(df) > 0:
            forecast = run_prophet(df2, delta.days)

            for i in range(len(forecast['yhat'])):
                if forecast['yhat'].values[i] < 0:
                    forecast['yhat'].values[i] = 0

            #Salvataggio in database dei risultati
            data = list()
            consumo = list()
            nome = list()
            id = list()
            for i in range(len(forecast['ds'])):
                if datetime.strptime(str(forecast['ds'].values[i])[:10], '%Y-%m-%d').date() >= datainizio and datetime.strptime(str(forecast['ds'].values[i])[:10], '%Y-%m-%d').date() <= datafine:
                    data.append(datetime.strptime(str(forecast['ds'].values[i])[:10], '%Y-%m-%d').date())
                    consumo.append(forecast['yhat'].values[i])
                    nome.append(ragsoc[cl])
                    id.append(indice[cl])
                else:
                    continue
        
            x = {'Data':data, 'Forecast':consumo, 'RagioneSociale':nome, 'Indice':id}

            result = pd.DataFrame(x)

            connC = pyodbc.connect(
            "Driver={SQL Server Native Client 11.0};"
            "Server=db_sql_prod;"
            "Database=DB_Gas;"
            "Trusted_Connection=yes;"
            )
            cursorC = connC.cursor()

            cursorC.execute("""select Consumo from Db_Gas.dbo.tbl_Forecast_Medio_Termine where Cognome_Rag_Soc = ? and Data = ? and idAlgoritmo = 3 and Indice = ?""", result['RagioneSociale'].values[0], result['Data'].values[0], result['Indice'].values[0])
            k = cursorC.fetchall()

            if len(k) == 0:
                for j in range(len(result)):
                    cursorC.execute("""insert into DB_Gas.dbo.tbl_Forecast_Medio_Termine values (?, '-', ?, NULL, 3, ?, ?)""", result['Data'].values[j], result['Forecast'].values[j], result['RagioneSociale'].values[j], result['Indice'].values[j])
                    connC.commit()
            else:
                for j in range(len(result)):
                    cursorC.execute("""update Db_GAs.dbo.tbl_Forecast_Medio_Termine set Consumo = ? where Indice = ? and Cognome_Rag_Soc = ? and idAlgoritmo = 3 and Data = ?""", result['Forecast'].values[j], result['Indice'].values[j], result['RagioneSociale'].values[j], result['Data'].values[j])
                    connC.commit()

            connC.close()

    


# In[ ]:




