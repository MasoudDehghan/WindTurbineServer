import mysql.connector
from datetime import  datetime
from windTurbinData import WindTurbineData


class DatabaseHandler:

    def __init__(self,configs):
        try:
            self.probirdConnection = mysql.connector.connect(
                host = configs.get("DB_HOST").data,
                user = configs.get("DB_User").data,
                password = configs.get("DB_PWD").data,
                database = configs.get("DB_SCHEMA").data
            )   
        except:
            self.probirdConnection = None

    def insertFake(self):
        self.mycursor = self.probirdConnection.cursor()
        now = datetime.now()
        formatted_date = now.strftime('%Y-%m-%d %H:%M:%S')
        Wind_Turbine_ID = 1
        sql = """INSERT INTO Wind_Turbine_Data(Time_Stamp,Wind_Turbine_ID,Wind_Speed,RPM,Temperature,Rain,
                        Visibility,Status,Expected_Status,Sub_Status) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        val = (formatted_date,Wind_Turbine_ID,159,88,33,1230,2,"RXC",8,"JVF")
        self.mycursor.execute(sql,val)
        self.probirdConnection.commit()
        print(self.mycursor .rowcount, "fake record inserted.")

    def insert(self,tData):
        self.mycursor = self.probirdConnection.cursor()

        sql = """INSERT INTO Wind_Turbine_Data(Time_Stamp,Wind_Turbine_ID,Wind_Speed,RPM,Temperature,Rain,
                        Visibility,Status,Expected_Status,Sub_Status) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        val = (tData['Time_Stamp'],tData['Wind_Turbine_ID'],tData['Wind_Speed'],tData['RPM'],tData['Temperature'],
                    tData['Rain'],tData['Visibility'],tData['Status'],tData['Expected_Status'],tData['Expected_Status'])
        self.mycursor.execute(sql,val)
        self.probirdConnection.commit()
        #print(self.mycursor .rowcount, "record inserted.")

    def readData(self,lastID):
        self.readCursor = self.probirdConnection.cursor()
        sql = """SELECT ID,Time_Stamp,Wind_Turbine_ID,Wind_Speed,RPM,Temperature,Rain,
                        Visibility,Status,Expected_Status,Sub_Status FROM Wind_Turbine_Data WHERE ID > %s ORDER BY ID ASC"""
        id = (str(lastID),)
        self.readCursor.execute(sql,id)
        result = self.readCursor.fetchall()
        tDataList = [];
        for x in result:
            tData = WindTurbineData(x[0],x[1],x[2],x[3],x[4],x[5],x[6],x[7],x[8],x[9],x[10])
            tDataList.append(tData)
        return tDataList

    def __def__(self):
        self.probirdConnection.close()