#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#Forecast per ragioni sociali con pdr giornalieri con almeno 12 mesi di storico (Prophet)

#####################################################################################################################################
#librerie
#####################################################################################################################################
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

# prevent crazy long numpy prints
np.set_printoptions(precision=4, suppress=True)
import statistics as st


# remove warnings
import warnings
warnings.filterwarnings('ignore')


from prophet import Prophet
from prophet.diagnostics import cross_validation

#date
import datetime

import pyodbc

ragsoc = list()
idzona = list()
result = dict()
error = dict()
indice = list()


# In[90]:

#####################################################################################################################################
#DEFINIZIONE FUNZIONI GLOBALI
#####################################################################################################################################


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

#funzione che prende come argomento una data e restituisce una lista di mesi di stagionalità gas comparabili
def Get_seasonality_date (d_inizio):
    rr = list()
    if 11 <= d_inizio.month or d_inizio.month <= 2:
        rr = [date(2023, 11, 1), date(2023, 12, 1), date(2024, 1, 1), date(2024, 2, 1)]
        return rr
    elif 3 <= d_inizio.month <= 5:
        rr = [date(2024, 3, 1), date(2024, 4, 1), date(2024, 5, 1)]
        return rr
    elif 6 <= d_inizio.month <= 8:
        rr = [date(2024, 6, 1), date(2024, 7, 1), date(2024, 8, 1)]
        return rr
    elif 9 <= d_inizio.month <= 10:
        rr = [date(2024, 9, 1), date(2024, 10, 1)]
        return rr
    
def Recal_month (d_partenza, d_result, cons):
    s = sum(cons)
    perc = list()
    wd = list()
    wdd = list()
    forec =  list()

    if s == 0:
        return(forec)

    
    for i in range(len(cons)):
        perc.append(cons[i]/s)

    for i in range(len(d_partenza)):
        wd.append(d_partenza[i].weekday())

    x = {'Data':d_partenza, 'Profilo':perc, 'Weekday':wd}

    df = pd.DataFrame(x)

    zero = st.mean(df.loc[df['Weekday'] == 0, 'Profilo'])
    uno = st.mean(df.loc[df['Weekday'] == 1, 'Profilo'])
    due = st.mean(df.loc[df['Weekday'] == 2, 'Profilo'])
    tre = st.mean(df.loc[df['Weekday'] == 3, 'Profilo'])
    quattro = st.mean(df.loc[df['Weekday'] == 4, 'Profilo'])
    cinque = st.mean(df.loc[df['Weekday'] == 5, 'Profilo'])
    sei = st.mean(df.loc[df['Weekday'] == 6, 'Profilo'])

    for i in range(len(d_result)):
        wdd.append(d_result[i].weekday())

    for i in range(len(d_result)):
        if wdd[i] == 0:
            forec.append(zero*s)
        if wdd[i] == 1:
            forec.append(uno*s)
        if wdd[i] == 2:
            forec.append(due*s)
        if wdd[i] == 3:
            forec.append(tre*s)
        if wdd[i] == 4:
            forec.append(quattro*s)
        if wdd[i] == 5:
            forec.append(cinque*s)
        if wdd[i] == 6:
            forec.append(sei*s)

    return forec


# In[93]:


#salvataggio della datamese utilizzata per le tabelle di anagrafica
from datetime import datetime
from datetime import date
from datetime import timedelta
from dateutil.relativedelta import relativedelta

oggi = date.today()
#dd = date(oggi.year, oggi.month, 1)


if __name__ == "__main__":

#####################################################################################################################################
#FORECAST PROPHET PER PDR CON STORICO > 12 MESI
#####################################################################################################################################

    #parametri inseriti in fase di esecuzione con all'interno data inizio e fine del forecast desiderato
    a = sys.argv[1]
    b = sys.argv[2]
    c = sys.argv[3]
    datainizio = datetime.strptime(str(a), '%Y-%m-%d')
    datafine = datetime.strptime(str(b), '%Y-%m-%d')
    tipo_forecast = str(c)  #tipo_forecast A -> esegui per tutte le tipologie  P -> esegui solo per pdr con storico >= 12  G -> esegui solo per pdr con storico < 12
    datainizio = datainizio.date()
    datafine = datafine.date()
    delta = datafine - datainizio

    dd = date(datainizio.year, datainizio.month, 1)

    if tipo_forecast == 'A' or tipo_forecast == 'P':
        #creazione delle liste di ragioni sociali e indici per l'algoritmo in questione, prese dalla tabella Anagrafica_RagSoc_Tipo
        connC = pyodbc.connect(
            "Driver={SQL Server Native Client 11.0};"
            "Server=db_sql_prod;"
            "Database=DB_Power;"
            "Trusted_Connection=yes;"
        )
        cursorC = connC.cursor()

        cursorC.execute("""select distinct pdr from DB_Gas.dbo.tbl_Anagrafica_Forecast_Pdr where Mesi >= 12 and Datamese = ? and Sticker != 'Domestico'""", dd)

        x = cursorC.fetchall()

        if len(x) == 0:
            dd = dd - relativedelta(months=1)

            cursorC.execute("""select distinct pdr from DB_Gas.dbo.tbl_Anagrafica_Forecast_Pdr where Mesi >= 12 and Datamese = ? and Sticker != 'Domestico'""", dd)

            x = cursorC.fetchall()

        for i in range(len(x)):
            ragsoc.append(x[i][0])
            #indice.append(x[i][1])

        connC.close()


        #ciclo for sulle ragioni sociali
        for cl in range(len(ragsoc)):


            connC = pyodbc.connect(
            "Driver={SQL Server Native Client 11.0};"
            "Server=db_sql_prod;"
            "Database=DB_Gas;"
            "Trusted_Connection=yes;"
            )
            cursorC = connC.cursor()

            #ricavo il consumo storico dagli SBG per la ragione sociale in questione
            cursorC.execute("""select Data, isnull(sum(Consumo), 0)
            from DB_Gas.dbo.tbl_Misure_Giornaliere_SBG g
            where pdr = ?
            group by Data
            order by Data""", ragsoc[cl])
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
                        #id.append(indice[cl])
                    else:
                        continue
            
                x = {'Data':data, 'Forecast':consumo, 'pdr':nome}

                result = pd.DataFrame(x)

                connC = pyodbc.connect(
                "Driver={SQL Server Native Client 11.0};"
                "Server=db_sql_prod;"
                "Database=DB_Gas;"
                "Trusted_Connection=yes;"
                )
                cursorC = connC.cursor()

                cursorC.execute("""select Consumo from Db_Gas.dbo.tbl_Forecast_Medio_Termine_Pdr where pdr = ? and Data = ? and idAlgoritmo = 4""", result['pdr'].values[0], result['Data'].values[0])
                k = cursorC.fetchall()

                if len(k) == 0:
                    for j in range(len(result)):
                        cursorC.execute("""insert into Db_Gas.dbo.tbl_Forecast_Medio_Termine_Pdr values (?, ?, ?, '-', 4, NULL, NULL)""", result['Data'].values[j], result['pdr'].values[j] ,result['Forecast'].values[j])
                        connC.commit()
                else:
                    for j in range(len(result)):
                        cursorC.execute("""update Db_Gas.dbo.tbl_Forecast_Medio_Termine_Pdr set Consumo = ? where Pdr = ? and idAlgoritmo = 4 and Data = ?""", result['Forecast'].values[j], result['pdr'].values[j], result['Data'].values[j])
                        connC.commit()

                connC.close()

    
#####################################################################################################################################
#FORECAST GIORNALIERI PER PDR
#####################################################################################################################################
    if tipo_forecast == 'A' or tipo_forecast == 'G':
        connC = pyodbc.connect(
            "Driver={SQL Server Native Client 11.0};"
            "Server=db_sql_prod;"
            "Database=DB_Power;"
            "Trusted_Connection=yes;"
        )
        cursorC = connC.cursor()

        r = list()

        cursorC.execute("""select distinct Pdr from DB_Gas.dbo.tbl_Anagrafica_Forecast_Pdr where Mesi < 12 and Datamese = ? and Sticker != 'Domestico'""", dd)

        x = cursorC.fetchall()

        if len(x) == 0:
            dd = dd - relativedelta(months=1)

            cursorC.execute("""select distinct Pdr from DB_Gas.dbo.tbl_Anagrafica_Forecast_Pdr where Mesi < 12 and Datamese = ? and Sticker != 'Domestico'""", dd)

            x = cursorC.fetchall()

        pdr = list()

        for i in range(len(x)):
            pdr.append(x[i][0])
        connC.close()

        for cl in range(len(pdr)):
            
            connC = pyodbc.connect(
            "Driver={SQL Server Native Client 11.0};"
            "Server=db_sql_prod;"
            "Database=DB_Power;"
            "Trusted_Connection=yes;"
            )
            cursorC = connC.cursor()
            
            if 'LIN SUSHI' in pdr[cl]:
                continue

            #storico dei consumi per la ragione sociale in questione
            #r.clear()

            cursorC.execute("""select Data, isnull(sum(Consumo), 0)
                from DB_Gas.dbo.tbl_Misure_Giornaliere_SBG g
                where pdr = ?
                group by Data
                order by Data""", pdr[cl])
            t = cursorC.fetchall()

            data_db = list()
            consumo = list()

            for i in range(len(t)):
                data_db.append(t[i][0])
                consumo.append(t[i][1])

            #creazione di una lista datamese dove salvo l'informazione relativa al mese delle date dello storico
            datamese = list()
            for i in range(len(data_db)):
                datamese.append(data_db[i].month)
            id_inizio = 0
            id_fine = 0

            d_result = list()
            gg = datainizio

            while gg <= datafine:
                d_result.append(gg)
                gg = gg + timedelta(days=1)
            
            #CASO 1: il periodo per cui calcolo il forecast è presente nello storico passato della ragione sociale 
            # -- estraggo il periodo di storico e imposto la previsione come ricalendarizzazione di quel periodo
            count = 0

            if datainizio.month in datamese:
                for i in range(len(data_db)):
                    if count != 0:
                        break
                    if data_db[i].month == datainizio.month:
                        id_inizio = i
                        for j in range(i, len(data_db)):
                            if data_db[j].month != datainizio.month:
                                id_fine = j
                                count = 1
                                break
                            elif j == len(data_db)-1:
                                id_fine = j+1
                                count= 1
                                break
                            else:
                                continue
                    else:
                        continue

                r = consumo[id_inizio:id_fine]

            #CASO 2: il periodo per cui calcolo il forecast non è presente nello storico ma è presente un periodo con stagionalità simile
            # -- estraggo il periodo di stagionalità simile e imposto la previsione come ricalendarizzazione di quel periodo 
            else:
                dt = list()
                dt = Get_seasonality_date(datainizio)
                id_inizio  = 0
                id_fine = 0
                for j in dt:
                    if id_fine != 0:
                        break
                    if j.month in datamese:
                        for i in range(len(data_db)):
                            if id_fine != 0:
                                break
                            if data_db[i].month == j.month:
                                id_inizio = i
                                id_fine = i+delta.days+1
                            else:
                                continue
                        if len(consumo) > id_fine:
                            xx = consumo[id_inizio:id_fine]
                            while len(xx) < delta.days+1:
                                xx.append((consumo[id_inizio]+consumo[id_inizio+1])/2)
                        else:
                            xx = consumo[id_inizio:id_fine]
                        #r = xx

                d_partenza = list()
                d_partenza = data_db[id_inizio:id_fine]

                if len(xx) != 0:
                    r = Recal_month(d_partenza, d_result, xx)

                #CASO 3: nessuno dei due precedenti metodi ha funzionato dunque prendiamo l'ultimo mese disponibile come misura e lo ricalendarizziamo
                if len(r) == 0:
                    r.clear()
                    cursorC.execute("""select Data, SUM(Consumo)
                    from DB_Gas.dbo.tbl_Misure_Giornaliere_SBG
                    where Pdr = ? and DATEPART(month, data) = (select top 1 DATEPART(month, data) from DB_Gas.dbo.tbl_Misure_Giornaliere_SBG where Pdr = ? order by Data desc)
                    group by Data
                    order by Data""", pdr[cl], pdr[cl])

                    x = cursorC.fetchall()

                    k = list()
                    giorno = list()
                    for i in range(len(x)):
                        giorno.append(x[i][0])
                        k.append(x[i][1])

                    if len(k) != 0:
                        r = Recal_month(giorno, d_result, k)
                        
                #CASO 4: nessuno dei due precedenti metodi ha funzionato dunque non abbiamo informazioni sul periodo di forecast
                # -- ricaviamo il forecast dai profili standard forniti dal SII moltiplicati per il consumo annuo estratto da Datamax
                
                if len(r) == 0:
                    r.clear()
                    #Profilo standar x consumo stimato per pdr
                    cursorC.execute("""select sum(p.Valore*d.Consumo_Stimato)/100
                    from DB_CDG.dbo.tbl_Anagrafica_Datamax_Gas d
                    join Db_Gas.dbo.tbl_Tisg_ProfiliStandard_Aeeg p on p.Codice = case when d.CategoriaUsoAEEG = 'C2' or d.CategoriaUsoAEEG = 'C4' or d.CategoriaUsoAEEG = 'T1' then
                    concat(d.CategoriaUsoAEEG, 'X', d.ClassePrelievoAEEG) else concat(d.CategoriaUsoAEEG, left(d.ZonaClimatica,1), d.ClassePrelievoAEEG) end and cast(p.DAta as date)
                    between ? and ?
                    where Pdr = ? and d.DataMese = ?
                    group by p.Data
                    order by p.Data""", datainizio, datafine, pdr[cl], dd)

                    x = cursorC.fetchall()

                    r = list()

                    for i in range(len(x)):
                        r.append(x[i][0])

            while(len(r) > delta.days+1 and len(r) != 0):
                r.append(np.mean(r[-2:]))

            while(len(r) < delta.days+1 and len(r) != 0):
                del r[-1]

            #Salvataggio in database dei risultati
            data = list()
            #data.append(d_inizio)

            if len(r) == 0:
                r.clear()
                u = delta.days+1
                r = consumo[-u:]

            for i in range(delta.days+1):
                data.append(datainizio + timedelta(days=i))

            consumo = list()
            nome = list()
            for i in range(len(r)):
                consumo.append(float(r[i]))
                nome.append(pdr[cl])

            if len(data) != len(consumo) or len(data) != len(nome) or len(consumo) != len(nome):
                connC.close()
                continue

            else:
                x = {'Data':data, 'Forecast':consumo, 'pdr':nome}

                result = pd.DataFrame(x)

                connC = pyodbc.connect(
                "Driver={SQL Server Native Client 11.0};"
                "Server=db_sql_prod;"
                "Database=DB_Gas;"
                "Trusted_Connection=yes;"
                )
                cursorC = connC.cursor()


                for j in range(len(result)):
                    cursorC.execute("""select Consumo from Db_Gas.dbo.tbl_Forecast_Medio_Termine_Pdr where pdr = ? and Data = ? and idAlgoritmo = 4""", result['pdr'].values[0], result['Data'].values[j])
                    k = cursorC.fetchall()

                    if len(k) == 0:
                            cursorC.execute("""insert into Db_Gas.dbo.tbl_Forecast_Medio_Termine_Pdr values (?, ?, ?, '-', 4, NULL, NULL)""", result['Data'].values[j], result['pdr'].values[j] ,result['Forecast'].values[j])
                            connC.commit()
                    else:
                            cursorC.execute("""update Db_Gas.dbo.tbl_Forecast_Medio_Termine_Pdr set Consumo = ? where pdr = ? and idAlgoritmo = 4 and Data = ?""", result['Forecast'].values[j], result['pdr'].values[j], result['Data'].values[j])
                            connC.commit()

                connC.close()


#####################################################################################################################################
#FORECAST DOMESTICI
#####################################################################################################################################
    mese = dd.month

    connC = pyodbc.connect(
        "Driver={SQL Server Native Client 11.0};"
        "Server=db_sql_prod;"
        "Database=DB_Gas;"
        "Trusted_Connection=yes;"
    )
    cursorC = connC.cursor()

    cursorC.execute("""delete from Db_Gas.dbo.tbl_Forecast_Medio_Termine_Pdr
                   where pdr = '-' and Data between ? and dateadd(day, -1, dateadd(month, 1, ?))""", dd, dd)

    cursorC.execute("""insert into Db_Gas.dbo.tbl_Forecast_Medio_Termine_Pdr
        select b.Data, '-', (AVG(a1.Consumo_unitario)*a.num_pdr)*b.Valore, a.Profilo_std, 4, NULL, NULL
        from DB_Gas.dbo.tbl_Anagrafica_Forecast_Pdr a
        join DB_Gas.dbo.tbl_Anagrafica_Forecast_Pdr a1 on a.Profilo_std = a1.Profilo_std and a1.Sticker = 'Domestico' and a1.Datamese < ? and DATEPART(month, a1.Datamese) = ?
        join DB_Gas.dbo.tbl_Tisg_ProfiliStandard_Base1Mese b on b.Codice = a.Profilo_std and b.Data between ? and DATEADD(day, -1, dateadd(month, 1, ?))
        where a.Sticker = 'Domestico' and a.Datamese = ?
        group by a.Profilo_std, a.num_pdr, b.Data, b.Valore""", dd, mese, dd, dd, dd)
    
    connC.commit()


    cursorC.execute("""update f
                    set Cognome_Rag_Soc = case when f.Pdr = '-' then '-' else a.Cognome_rag_soc end, Indice = case when f.pdr = '-' then 'Fisso' else a.Indice end
                    from Db_Gas.dbo.tbl_Forecast_Medio_Termine_Pdr f
                    left join Db_CDG.dbo.tbl_Anagrafica_Datamax_Gas a on f.pdr = a.pdr and a.Datamese = ?
                    where f.Data between ? and dateadd(day, -1, dateadd(month, 1, ?))""", dd, dd, dd)
    connC.commit()

    connC.close()
    



# In[ ]:




