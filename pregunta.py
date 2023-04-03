"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""
import pandas as pd
import re

def ingest_data():
    #
    # Inserte su código aquí
    #

    # Obtener nombre de columnas según requerimiento
    with open("clusters_report.txt", "r") as archivo:
        lineas = archivo.readlines()
    columna = lineas[0].replace("de","de" + " "+'palabras clave')
    patron = re.compile(r"\s{2,}")
    lista_palabras = patron.split(columna)
    lista_palabras.pop(-1)
    lista_palabras = [str(elemento.replace(" ","_")) for elemento in lista_palabras]
    lista_palabras = [str(elemento.lower()) for elemento in lista_palabras]

    # Lectura de archivo .txt que permite modular el ancho de cada columna
    df = pd.read_fwf("clusters_report.txt", colspecs=[(3,6),(9,16),(25,29),(40,119)], header=None)
    df.drop(df.index[:3], inplace=True)
    df.reset_index(drop=True, inplace=True)
    df_complete = pd.DataFrame()
    for i in range(len(lista_palabras)-1):
        df_complete[lista_palabras[i]] = df[i]
    df_complete.dropna(inplace=True)
    df_complete.reset_index(drop=True, inplace=True)
    for i in range(len(lista_palabras)-2):
        df_complete[lista_palabras[i]] = df_complete[lista_palabras[i]].astype(int)
    df_complete[lista_palabras[2]] = df_complete[lista_palabras[2]].str.replace(',', '.').astype(float)

    # Modificación de df inicial, para ajustar a requerimientos de la pregunta
    item_clave = ' '.join([i for i in df[3]]).replace('control multi', 'control.multi')
    keys_words = [i.strip() for i in item_clave.split('.')]
    df_complete[lista_palabras[3]] = pd.concat([pd.Series(i) for i in keys_words]).reset_index(drop=True).str.replace(' ,', ',').replace(',',', ').str.strip('\n').str.replace('   ',' ').str.replace('  ',' ').str.replace('  ', ' ')
    return df_complete
