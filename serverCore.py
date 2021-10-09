from kafka import KafkaConsumer
from json import loads
import json
from jproperties import Properties
from dbHandler import DatabaseHandler 
import logging
import sys
from mysql.connector.errors import DatabaseError

class ServerThread:
      def __init__(self):
        try:
            self.logger = logging.getLogger()
            self.logger.setLevel(logging.INFO)

            c_handler = logging.StreamHandler(sys.stdout)
            f_handler = logging.FileHandler('wturbine.log')

            c_format = logging.Formatter('%(asctime)s - %(message)s')
            f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            c_handler.setFormatter(c_format)
            f_handler.setFormatter(f_format)
            c_handler.setStream(sys.stdout)
            # Add handlers to the logger
            self.logger.addHandler(c_handler)
            self.logger.addHandler(f_handler)
            self.logger.info('WindTurbine Server is started')
            self.configFileName = "app-config.properties"
            self.config = Properties()
            self.producer = None
            
            with open(self.configFileName, 'r+b') as config_file:
                self.config.load(config_file)
        
            self.KAFKA_TOPIC = self.config.get("KAFKA_TOPIC").data           
            self.prodbHandler = DatabaseHandler(self.config)
            if self.prodbHandler.probirdConnection is None:
                 raise DatabaseError("Unable to connect to db")
            

        except DatabaseError as e:
            self.logger.error(f'{e}, program is terminated!')
            exit()
        except:
            self.logger.error(f'Error in Reading from Config File {self.configFileName}, program is terminated !!!')
            exit()
      def run(self):
        try:
            self.logger.info(f'Listening on Topic {self.KAFKA_TOPIC} in Kafka Server')
            self.consumer = KafkaConsumer(self.KAFKA_TOPIC,
                         value_deserializer=lambda x: 
                         loads(x.decode('utf-8')))
        except:
            self.logger.error("No Kafka Server is Available,program is terminated")
            exit()
        windbirdDataMap = {}
        try:
            for msg in self.consumer:
                msg =  msg.value
                tData = json.loads(msg)
                if tData['Wind_Turbine_ID'] in windbirdDataMap.keys():
                    if windbirdDataMap[tData['Wind_Turbine_ID']] < tData['ID']:
                        print('INSERT ID',tData['ID'],' FROM Turbine ',tData['Wind_Turbine_ID'])
                        self.prodbHandler.insert(tData)
                        windbirdDataMap[tData['Wind_Turbine_ID']] = tData['ID']
                    else:
                        print('IGNORE ID',tData['ID'],' FROM Turbine ',tData['Wind_Turbine_ID'])
                
                else:
                    print('XXXINSERT ID',tData['ID'],' FROM Turbine ',tData['Wind_Turbine_ID'])
                    self.prodbHandler.insert(tData)
                    windbirdDataMap[tData['Wind_Turbine_ID']] = tData['ID']
        except Exception as e:
            self.logger.error(f'{e}, program is terminated')
            exit(0)
        except KeyboardInterrupt as e:
            self.logger.error(f'Program is terminated by user')
            exit(0)    
        finally:
            if self.prodbHandler.probirdConnection != None:
                self.prodbHandler.probirdConnection.close()    
        


if __name__ =="__main__":
    serverThread = ServerThread()
    serverThread.run()