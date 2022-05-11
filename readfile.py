import logging
import os
import sys
import time
import csv
from datetime import date
from watchdog.observers import Observer
from watchdog.events import LoggingEventHandler, FileSystemEventHandler
from multiprocessing import Process
import connection

cur, conn = connection.get_connection()

class MyEventHandler (FileSystemEventHandler):
    def p_msg(text):
        proc = os.getpid()
        print("Hi {0} Has been created: {1} ".format(text, proc))
    def on_created(self, event):
        time.sleep(3)

        if event.src_path.lower().endswith('.csv'):

            if not event.is_directory:
                text = "Hi {0} Has been created! ".format(event.src_path)
                proc = Process(target=self.p_msg(), args=(text))
                proc.start()
                proc.join()
                print("YOu have Created an Event at- %s." % event.src_path)
                file_name= os.path.basename(event.src_path)
                fp = os.path.dirname(event.src_path)
                file_sql_query = "INSERT INTO mysql.app_files(File_Name,file_path,sys_creation_date,sys_update_date,DL_UPDATE,Application_ID,status,pid) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
                data=(file_name, fp, date.today(), date.today(),'APPPY','121','PN',os.getpid())
                cur.execute(file_sql_query, data)
                conn.commit()
                print(cur.rowcount, "Record Inserted")
                # conn.close()
                with open(event.src_path, 'r') as csv_f:
                    csv_read = csv.DictReader(csv_f)
    #
                    for line in csv_read:
                        print(line)
            else:
                print("Un-know FIle Is created")
        else:
            print("Folder is Created")
    # def
    # print(event)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')                #this code is for logging information
    path = r"C:\Users\Admin\Downloads"                              #folder to watch to listen
    event_handler = MyEventHandler()                                #Creating an Object of a class as per schedule
    observer = Observer()                                           #assigning an varible to function
    observer.schedule(event_handler, path, recursive=True)          #Pass the required input to call a scheduler
    observer.start()                                                #Starting scheduler
    try:                                            # It always try to do what ever is written if any exception
        while True:                                 # Infinite loop
            time.sleep(1)                           # sleep for 1 sec
    finally:                                        # this block will be called at the End
        observer.stop()                             # stoping observer
        observer.join()                             # take the join if any
        conn.close()