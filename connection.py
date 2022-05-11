import mysql
import mysql.connector
from mysql.connector import Error

def get_connection():
    conn = mysql.connector.connect(host="localhost", user="divya", password="divya_123", port=3306,auth_plugin='mysql_native_password')
    cur = conn.cursor()
    return cur, conn