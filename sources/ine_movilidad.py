repos = r"C:\Users\ManuBenito\Documents\GitHub"
import sys
import os
sys.path.append(repos)

import pandas as pd
import datetime
import numpy as np
import warnings
warnings.filterwarnings('ignore')

days = {0:"Lunes",1:"Martes",2:"Miércoles",3:"Jueves",4:"Viernes",5:"Sábado",6:"Domingo"}
months = {"Enero":1,"Febrero":2,"Marzo":3,"Abril":4,"Mayo":5,"Junio":6,"Julio":7,"Agosto":8,"Septiembre":9,"Octubre":10,"Noviembre":11,"Diciembre":12}

data_columns = {'CELDA_ORIGEN ':"Origin ID", 'NOMBRE_CELDA_ORIGEN ':"Origin NAME", 'CELDA_DESTINO ':"Destination ID",
       'NOMBRE_CELDA_DESTINO ':"Destination NAME", 'FLUJO ':"Trips NUMBER", 'day_name': "Day NAME",
       'day_number': "Day NUMBER", 'weekend' : "Day WEEKEND", 'week_number': "Week NUMBER", 'month_number':"Month NUMBER", 'month_name': "Month NAME",
       'year_number': "Year NUMBER", 'date_name': "Date NAME", 'Población Origen': 'Origin POPULATION','Comunidad Autónoma de residencia':"Origin REGION", 'Provincia de residencia':"Origin PROVINCE",
       'Código área de residencia':"Origin ID", 'Nombre área de residencia': "Origin NAME",
       'Comunidad Autónoma de destino':"Destination REGION", 'Provincia de destino':"Destination PROVINCE",
       'Código área de destino' : "Destination ID", 'Nombre área de destino': "Destination NAME",
       'Flujo origen-destino (nº de personas)':"Trips NUMBER"}

final_columns = ["Date NAME","Day NAME", "Day NUMBER","Day WEEKEND", "Week NUMBER", "Month NUMBER", "Month NAME", "Year NUMBER",
                 "Origin ID","Origin NAME","Origin REGION","Origin PROVINCE",
                 "Destination ID", "Destination NAME","Destination REGION","Destination PROVINCE",
                 "Trips NUMBER",'Origin POPULATION']

def make_time_formats(data, year_, month_, day_, month_name):
    dmonth = month_
    dday = datetime.date(year_, dmonth, day_).weekday()
    nday = days[dday]
    dweek = datetime.date(year_, dmonth, day_).isocalendar()[1]
    if nday in ['Sábado','Domingo']:
        tday = 1 
    else:
        tday = 0
    date_name = f"{nday}, {str(day_)} de {month_name} de {str(year_)}"
    fields = {'day_name': nday, 'day_number': dday, 'weekend': tday, 'week_number': dweek, 'month_number': dmonth, 'month_name': month_name,'year_number': year_, 'date_name': date_name}
    for field, var in fields.items():
        data[field] = var
    return data

def process_inemobility_format(data_directory, data_format):
    
    trips = []
    population = []
    
    if data_format == 'format 2':

        #Normalized Format
        for r, d, f in os.walk(data_directory):
            for f in [file for file in os.listdir(r) if os.path.isfile(os.path.join(r,file))]:
                if "Tabla 1.3" in f:
                    filepath = os.path.join(r,f)
                    year = int(filepath.split("\\")[-2].split(" ")[1])
                    nmonth = filepath.split("\\")[-2].split(" ")[0]
                    day = int(filepath.split("\\")[-1].split("_")[-1][0:2])
                    dmonth = months[nmonth]
                    df = make_time_formats(pd.read_excel(filepath), year, dmonth, day, nmonth)
                    trips.append(df)
                elif "Tabla 1.2" in f:
                    filepath = os.path.join(r,f)
                    year = int(filepath.split("\\")[-2].split(" ")[1])
                    nmonth = filepath.split("\\")[-2].split(" ")[0]
                    day = int(filepath.split("\\")[-1].split("_")[-1][0:2])
                    dmonth = months[nmonth]
                    df = make_time_formats(pd.read_excel(filepath), year, dmonth, day, nmonth)
                    population.append(df)
                else:
                    pass
    
    elif data_format == 'format 1':
        for r, d, f in os.walk(data_directory):
            for f in [file for file in os.listdir(r) if os.path.isfile(os.path.join(r,file))]:
                if "Flujos+15_O-D" in f:
                    filepath = os.path.join(r,f)
                    year = int(filepath.split("\\")[-3].split(" ")[1])
                    nmonth = filepath.split("\\")[-3].split(" ")[0]
                    day = int(filepath.split("\\")[-1].split("_")[-1][0:2])
                    dmonth = months[nmonth]
                    df = make_time_formats(pd.read_csv(filepath, sep = ";", encoding= 'latin-1'), year, dmonth, day, nmonth)
                    trips.append(df)
                elif "PobxCeldasOrigen" in f:
                    filepath = os.path.join(r,f)
                    year = int(filepath.split("\\")[-3].split(" ")[1])
                    nmonth = filepath.split("\\")[-3].split(" ")[0]
                    day = int(filepath.split("\\")[-1].split("_")[-1][0:2])
                    dmonth = months[nmonth]
                    df = make_time_formats(pd.read_csv(filepath, sep = ";", encoding= 'latin-1'), year, dmonth, day, nmonth)
                    population.append(df)
                else:
                    pass
    return pd.concat(trips),pd.concat(population)

def areas_description_dict(path_format2):
    df = pd.read_excel(path_format2)
    area_name = pd.Series(df['Nombre área de residencia'].values,index=df['Código área de residencia']).to_dict()
    region = pd.Series(df['Comunidad Autónoma de residencia'].values,index=df['Código área de residencia']).to_dict()
    province = pd.Series(df['Provincia de residencia'].values,index=df['Código área de residencia']).to_dict()
    return area_name, region, province

def make_regions_fields(data,areas_name,regions_name,provinces_name):
    data['CELDA_ORIGEN '] = data['CELDA_ORIGEN '].str.strip()
    data['CELDA_DESTINO '] = data['CELDA_DESTINO '].str.strip()
    data['Comunidad Autónoma de residencia'] = data['CELDA_ORIGEN '].replace(regions_name)
    data['Provincia de residencia'] = data['CELDA_ORIGEN '].replace(provinces_name)
    data['Nombre área de residencia'] = data['CELDA_ORIGEN '].replace(areas_name)
    data['Comunidad Autónoma de destino'] = data['CELDA_DESTINO '].replace(regions_name)
    data['Provincia de destino'] = data['CELDA_DESTINO '].replace(provinces_name)
    data['Nombre área de destino'] = data['CELDA_DESTINO '].replace(areas_name)
    return data

def population_per_date(data, data_format):
    pop_area = {}
    if data_format == 'format 2':
        for d in data['date_name'].unique():
            df = data[data['date_name'] == d]
            di = pd.Series(df['Población residente (A)'].values,index=df['Área']).to_dict()
            pop_area.update({d:di})
    elif data_format == 'format 1':
        for d in data['date_name'].unique():
            df = data[data['date_name'] == d]
            di = pd.Series(df['POB_RESID '].values,index=df['CELDA_ORIGEN ']).to_dict()
            pop_area.update({d:di})
    return pop_area

def population_stays_per_date(data):
    #Only needed for data format 1
    pop_stays_area = {}
    for d in data['date_name'].unique():
        df = data[data['date_name'] == d]
        di = pd.Series(df['POB_CASA '].values,index=df['CELDA_ORIGEN ']).to_dict()
        pop_stays_area.update({d:di})
    return pop_stays_area

def make_self_area_trips(data, population_stays_dict):
    
    for d in data['date_name'].unique():
        data.loc[data['date_name'] == d,'Población que permanece'] = data[data['date_name'] == d]['CELDA_ORIGEN '].replace(population_stays_dict[d])
        
    i = 0
    new = {}
    for d in data['date_name'].unique():
        df = data[data['date_name'] == d]
        for c in df['CELDA_ORIGEN '].unique():
            df2 = df[df['CELDA_ORIGEN '] == c]
            di = {'CELDA_ORIGEN ':c,
                  'NOMBRE_CELDA_ORIGEN ': df2['NOMBRE_CELDA_ORIGEN '].unique()[0],
                  'CELDA_DESTINO ': c,
                  'NOMBRE_CELDA_DESTINO ': df2['NOMBRE_CELDA_ORIGEN '].unique()[0],
                  'FLUJO ': df2['Población que permanece'].unique()[0],
                  'Unnamed: 5': np.nan,
                  'day_name': df2['day_name'].unique()[0],
                  'day_number': df2['day_number'].unique()[0],
                  'weekend': df2['weekend'].unique()[0],
                  'week_number': df2['week_number'].unique()[0],
                  'month_number': df2['month_number'].unique()[0],
                  'month_name': df2['month_name'].unique()[0],
                  'year_number': df2['year_number'].unique()[0],
                  'date_name': df2['date_name'].unique()[0],
                  'Población Origen': df2['Población Origen'].unique()[0]}
            i += 1
            new.update({i:di})
    return pd.DataFrame.from_dict(new,orient='index')  

def insert_area_population(data, population_dict, data_format):
    if data_format == "format 2":
        for d in data['date_name'].unique():
            data.loc[data['date_name'] == d,'Población Origen'] = data[data['date_name'] == d]['Código área de residencia'].replace(population_dict[d])
    elif data_format == "format 1":
        for d in data['date_name'].unique():
            data.loc[data['date_name'] == d,'Población Origen'] = data[data['date_name'] == d]['CELDA_ORIGEN '].replace(population_dict[d])
    return data

def force_integer(data):
    for c in data.columns:
        if data[c].dtype == 'float64':
            data[c].fillna(0,inplace = True)
            data[c] = data[c].astype('int64')
        else:
            pass
    return data

def percentage_trips(data):
    data["Trips AS PERCENTAGE POPULATION"] = round((data['Trips NUMBER'] * 100) / data['Origin POPULATION'],2)
    return data

processing_data = {}

format1_trips, format1_population = process_inemobility_format(data_directory, data_format= 'format 1')
format2_trips, format2_population = process_inemobility_format(data_directory, data_format= 'format 2')
pop_format1 = population_per_date(format1_population, data_format = 'format 1')
pop_format2 = population_per_date(format2_population, data_format = 'format 2')
format1_trips = insert_area_population(format1_trips, pop_format1, 'format 1')
format2_trips = insert_area_population(format2_trips, pop_format2, 'format 2')
pop_stays_format1 = population_stays_per_date(format1_population)
format1_selftrips = make_self_area_trips(format1_trips, pop_stays_format1)
area_name, region, province = areas_description_dict(sample_file)
format1_trips = make_regions_fields(format1_trips, area_name, region, province)
format1_selftrips = make_regions_fields(format1_selftrips, area_name, region, province)
format1_trips.rename(columns = data_columns, inplace = True)
format2_trips.rename(columns = data_columns, inplace = True)
format1_selftrips.rename(columns = data_columns, inplace = True)
format1_trips = format1_trips[final_columns]
format2_trips = format2_trips[final_columns]
format1_selftrips = format1_selftrips[final_columns]
format1_trips = format1_trips.loc[:,~format1_trips.columns.duplicated()]
format2_trips = format2_trips.loc[:,~format2_trips.columns.duplicated()]
format1_selftrips = format1_selftrips.loc[:,~format1_selftrips.columns.duplicated()]
format1_trips = force_integer(format1_trips)
format2_trips = force_integer(format2_trips)
format1_selftrips = force_integer(format1_selftrips)
format1_trips = percentage_trips(format1_trips)
format2_trips = percentage_trips(format2_trips)
format1_selftrips = percentage_trips(format1_selftrips)
final_data = pd.concat([format1_trips,format2_trips,format1_selftrips], axis = 0, ignore_index = True)
final_data.to_csv(r"C:\Users\ManuBenito\Documents\Walknet-DataLake\sources\spain\mobility\ine_mobility\level1\LEVEL1_INEMOBILITY_TRANSPORTZONE.csv",sep=";",index = False)