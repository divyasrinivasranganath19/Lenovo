import connection
cur, conn = connection.get_connection()

file_sql_query = "INSERT INTO mysql.app_files(File_Name,file_path,sys_creation_date,sys_update_date,DL_UPDATE,Application_ID,status) VALUES ('1234.csv','C:/user/Downlods',current_date(),null,'APPPY','121','PN')"
cur.execute(file_sql_query)
conn.commit()
print(cur.rowcount, "Record Inserted")
conn.close()