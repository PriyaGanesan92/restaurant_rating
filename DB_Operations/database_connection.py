from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from Log.Log import App_Logger
from datetime import datetime
class db_connection:

    def __init__(self):
        self.file = open("Log/File_Logs/Db_error.txt", 'a+')
        self.logger = App_Logger()


    def establish_db_connection(self):
        """
        This method is used to establish db connection with cassandra db
        DB which is present in cassandra is rating_prediction.

        Written by : Priya Ganesan
        Version : 1.0


        """
        try :

            cloud_config = {
                'secure_connect_bundle': 'Bundle_file//secure-connect-rating-prediction.zip'
            }
            auth_provider = PlainTextAuthProvider('ZPefWZKCbZjNWTWAsXxEHwrs', 'e-ewLfUSSs8puFQZadF80p,B2Et2WhT70YU91b-48ms_1frhKQQLokcGdsnBYhv7xg-zADZyS8y7mN,G.HleAQQfgN_NE,XcfLBWGe3ZWgzcsE,gmZ9Z1ntvSwjqEZlZ')
            cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
            self.session = cluster.connect('rating_prediction')
            return self.session


        except(Exception):
            self.logger.file_log(self.file, str(Exception))
            self.file.close()
            pass


    def log_insert_into_db(self,session,Log_Level,log_message):

        """
        This method is used to insert data into Log related tables

        :param Log_Level: It gives information whether log level is debug or info or error or Warning. According to that
        the insert happens to the respective table. This parameter gives details about the table
        :param log_message: This parameter contains the log information

        Written by : Priya Ganesan
        Version : 1.0

        """
        try:

            self.now = datetime.now()
            self.date = self.now.date()
            self.current_time = self.now.strftime("%H:%M:%S")
            #log related informations are inserted into database. Seperate tables are available for log levels like error,info, debug
            session.execute(
                """
                INSERT INTO """ + Log_Level + """ (id, current_date, current_time,log_message)
                VALUES (uuid(), %(date)s, %(time)s, %(message)s)
                """,
                {'date': self.date, 'time': self.current_time, 'message': log_message}
            )

        except Exception as e:
            pass
            self.logger.file_log(self.file, e)
            self.file.close()







