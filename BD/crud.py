import sqlite3
from sqlite3 import Error
import os.path

BASE_DIR = os.path.dirname(os.path.abspath(__file__)) 
DATABASE_FILE = os.path.join(BASE_DIR, "delitos_2023.db")


def sql_connection():
    try:
        db = sqlite3.connect(DATABASE_FILE)
        return db
    
    except Error:
        print(Error)


def select(db, id):
    try:
        cursor = db.cursor()
        cursor.execute("""SELECT Delitos.delito_id, Dias.nombre_dia, fecha_delito_dia_numero, Meses.nombre_mes, franja_horaria, Barrios.nombre_barrio, uso_arma, uso_moto, Tipo_Delitos.tipo_delito, info_adicional
                        FROM Delitos 
                        INNER JOIN Dias ON Delitos.fecha_delito_dia_id = Dias.fecha_delito_dia_id
                        INNER JOIN Meses ON Delitos.fecha_delito_mes_id = Meses.fecha_delito_mes_id
                        INNER JOIN Barrios ON Delitos.barrio_id = Barrios.barrio_id
                        INNER JOIN Tipo_Delitos ON Delitos.tipo_delito_id = Tipo_Delitos.tipo_delito_id
                        WHERE delito_id =;""", id)
        resultado = cursor.fetchone()
        cursor.close()
        return resultado
    
    except Error:
         print(Error)


def insert(db, list):
    try:
        cursor = db.cursor()
        cursor.execute("""INSERT INTO Delitos 
                          (fecha_delito_dia_id, fecha_delito_dia_numero, fecha_delito_mes_id, fecha_delito_ano, franja_horaria, barrio_id, uso_arma, uso_moto, tipo_delito_id, info_adicional) 
                          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", list)
        db.commit()
        cursor.close()
        print("Dato ingresado con éxito")

    except Error:
        print(Error)


def delete(db, id):
    try:
        cursor = db.cursor()
        cursor.execute("DELETE * FROM Delitos WHERE delito_id =", id)
        db.commit()
        cursor.close()
        print("Dato eliminado con éxito")
    
    except Error:
        print(Error)


def update(db, list, id):
    try:
        cursor = db.cursor()
        cursor.execute("UPDATE Delitos SET (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) WHERE delito_id = ?", list, id)
        db.commit()
        cursor.close()
        print("Dato modificado con éxito")

    except Error:
        print(Error)