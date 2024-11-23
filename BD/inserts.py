import sqlite3
from sqlite3 import Error
import pandas as pd 
import os.path

# __file__ es una variable especial en Python que contiene la ruta completa del archivo Python que se está ejecutando.
# os.path.abspath() toma una ruta de archivo (en este caso __file__) y la convierte en una ruta absoluta.
# os.path.dirname() toma la ruta de la carpeta que contiene el archivo

# Lo que hace esta línea es obtener la ruta del directorio donde se encuentra el archivo Python actual, no el archivo mismo.
BASE_DIR = os.path.dirname(os.path.abspath(__file__)) 

# os.path.join() crea un directorio completo con los datos que se le ingresan.
# Por lo tanto, las siguientes constantes son strings que toman los archivos del nombre
# especificado, siempre y cuando compartan el mismo directorio con este script.
DB_PATH = os.path.join(BASE_DIR, "delitos_2023.db") 
DS_PATH = os.path.join(BASE_DIR, "delitos_2023.csv")

df = pd.read_csv(DS_PATH)

try:
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
except Error:
    print(Error)

# Diccionarios para las funciones lambda. Probablemente se pueden colocar en otro .py e importarlos desde ahí.
dia_a_numero = {
    "LUN": 1,
    "MAR": 2,
    "MIE": 3,
    "JUE": 4,
    "VIE": 5,
    "SAB": 6,
    "DOM": 7
}
mes_a_numero = {
    "enero": 1,
    "febrero": 2,
    "marzo": 3,
    "abril": 4,
    "mayo": 5,
    "junio": 6,
    "julio": 7,
    "agosto": 8,
    "septiembre": 9,
    "octubre": 10,
    "noviembre": 11,
    "diciembre": 12
}
tipo_delito_a_numero = {
    "Muertes por siniestros viales": 1,
    "Lesiones por siniestros viales": 2,
    "Lesiones Dolosas": 3,
    "Amenazas": 4,
    "Hurto total": 5,
    "Robo total": 6,
    "Hurto automotor": 7,
    "Robo automotor": 8
}
barrios_a_numero = {
    'RETIRO': 1,
    'SAN NICOLAS': 2,
    'PUERTO MADERO': 3,
    'SAN TELMO': 4,
    'MONTSERRAT': 5,
    'CONSTITUCION': 6,
    'RECOLETA': 7,
    'BALVANERA': 8,
    'SAN CRISTOBAL': 9,
    'LA BOCA': 10,
    'BARRACAS': 11,
    'PARQUE PATRICIOS': 12,
    'NUEVA POMPEYA': 13,
    'ALMAGRO': 14,
    'BOEDO': 15,
    'CABALLITO': 16,
    'FLORES': 17,
    'PARQUE CHACABUCO': 18,
    'VILLA SOLDATI': 19,
    'VILLA RIACHUELO': 20,
    'VILLA LUGANO': 21,
    'LINIERS': 22,
    'MATADEROS': 23,
    'PARQUE AVELLANEDA': 24,
    'VILLA REAL': 25,
    'MONTE CASTRO': 26,
    'VERSALLES': 27,
    'FLORESTA': 28,
    'VELEZ SARSFIELD': 29,
    'VILLA LURO': 30,
    'VILLA GENERAL MITRE': 31,
    'VILLA DEVOTO': 32,
    'VILLA DEL PARQUE': 33,
    'VILLA SANTA RITA': 34,
    'COGHLAN': 35,
    'SAAVEDRA': 36,
    'VILLA URQUIZA': 37,
    'VILLA PUEYRREDON': 38,
    'NUÑEZ': 39,
    'BELGRANO': 40,
    'COLEGIALES': 41,
    'PALERMO': 42,
    'CHACARITA': 43,
    'VILLA CRESPO': 44,
    'LA PATERNAL': 45,
    'VILLA ORTUZAR': 46,
    'AGRONOMIA': 47,
    'PARQUE CHAS': 48
}

#           0       1       2       3       4        5          6           7           8
columnas = ["anio", "mes", "dia", "fecha", "franja", "subtipo", "uso_arma", "uso_moto", "barrio"]

# Itera sobre el dataset por cada fila.
# Tardó entre 30 y 40 minutos, 157 mil registros no son joda...
for i in df.index:
    list = df[columnas].loc[[i]].values.flatten().tolist()

    # MES: STRING A ID (fecha_delito_mes_id)
    list[1] = (lambda mes: mes_a_numero.get(mes, None))(list[1])

    # DIA: STRING A ID (fecha_delito_dia_id)
    list[2] = (lambda dia: dia_a_numero.get(dia, None))(list[2])

    # FECHA: ULTIMOS DOS CARACTERES DEL STRING A INT (fecha_delito_dia_numero)
    list[3] = int(list[3].split('-')[2])

    # SUBTIPO: STRING A ID (tipo_delito_id)
    list[5] = (lambda subtipo: tipo_delito_a_numero.get(subtipo, None))(list[5])

    # BARRIO: STRING A ID (barrio_id)
    list[8] = (lambda barrio: barrios_a_numero.get(barrio, None))(list[8])

    # Lista ordenada para que los datos se ingresen como aparecen las columnas en la BD. No se si es necesario.
    list_en_orden = [list[2], list[3], list[1], list[0], list[4], list[8], list[6], list[7], list[5]]

    cursor.execute("INSERT INTO Delitos (fecha_delito_dia_id, fecha_delito_dia_numero, fecha_delito_mes_id, fecha_delito_ano, franja_horaria, barrio_id, uso_arma, uso_moto, tipo_delito_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", list_en_orden)
    conn.commit()

cursor.close()