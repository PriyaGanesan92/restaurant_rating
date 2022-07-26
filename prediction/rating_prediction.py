import pandas as pd
import pickle
from DB_Operations.database_connection import db_connection
from prediction.prediction_preprocessing import prediction_preprocess
from Data_Preprocessing.Preprocessing import preprocessing
import bz2


class predict_rating :
    def __init__(self):
        self.connection = db_connection()
        self.file = open("Log/File_Logs/Db_error.txt", 'a+')
        self.prediction_preprocess = prediction_preprocess()
        self.preprocessing = preprocessing()

    def prediction_steps(self,session,inputlist):
        """
        This method contains the functions which is need to predict the input data given by the user
        :param session: database connectivity
        :param inputlist: input given by the user

        Written by : Priya Ganesan
        Version : 1.0

        """
        input_dataframe = self.create_dataframe_from_input_list(session,inputlist)
        self.insert_data_into_prediction_table(session,input_dataframe)
        input_dataframe = self.prediction_preprocess.mapping(session,input_dataframe)
        encoded_input_data = self.prediction_preprocess.one_hot_encoding_for_prediction_data(session,input_dataframe)
        X_train = pd.read_csv('X_train.csv')
        v = X_train.columns
        encoded_aligned_input_data = self.prediction_preprocess.creating_same_number_of_columns_in_input_data(session,X_train,encoded_input_data)
        return encoded_aligned_input_data



    def create_dataframe_from_input_list(self,session,inputlist):
        """
        We get the input given the user and based on the input we create a dataframe
        :param session: database connectivity
        :param inputlist: input list given by the user
        :return: dataframe input
        """
        try :

            self.connection.log_insert_into_db(session, 'DebugLog', 'Trying to start create dataframe from input list')
            #pandas is used to create dataframe
            input_dataframe = pd.DataFrame(columns=['online_order','book_table','phone','location','rest_type','dish_liked','cuisines','approx_cost(for two people)','listed_in(type)'])
            #input from the user is converted into list and the list is converted into dataframe usin the below code
            input_dataframe.loc[len(input_dataframe), :] = inputlist
            self.connection.log_insert_into_db(session, 'InfoLog', 'Creating dataframe from input list completed')
            return input_dataframe
        except Exception as ex :
            self.connection.log_insert_into_db(session, 'ErrorLog', str(ex))

    def insert_data_into_prediction_table(self,session,data):
        """
        This method is used to insert prediction data into cassandra db. 9 column values are getting inserted into db
        :param session: session for db connection
        :param useful_data: data to be inserted

        Written by : Priya Ganesan
        Version : 1.0
        """
        try :
            self.connection.log_insert_into_db(session, 'DebugLog','Trying to insert data into prediction data table')
            session.execute("truncate table prediction_data")
            query = "INSERT INTO prediction_data(id,online_order,book_table,phone_number,location,restaurant_subtype,dishes_liked,cuisines,approximate_cost,restaurant_type) VALUES(uuid(), ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            prepared = session.prepare(query)

            for index,row in data.iterrows():
                session.execute_async(prepared, (str(row[0]), str(row[1]), str(row[2]), str(row[3]), str(row[4]),
                                           str(row[5]), str(row[6]), str(row[7]), str(row[8])))

            self.connection.log_insert_into_db(session, 'InfoLog', 'Insert into prediction Data completed')

        except Exception as e:
            self.connection.log_insert_into_db(session,'ErrorLog', str(e))


    def prediction(self,session,input_data):
        """
        Prediction started for the data given by user
        :param session: Database connectivity
        :param input_data: data from the uesr
        :return: returns output to the ui page
        """
        try :
            self.connection.log_insert_into_db(session, 'DebugLog', 'Trying to start prediction')
            #prediction is made from the pickle file which is created while training the data
            file = bz2.BZ2File("RandomForestBinaryData", 'rb')
            loaded_model = pickle.load(file)
            #loaded_model = pickle.load(open('model_without_grid_search_cv.pkl', 'rb'))
            #prediction is made using the model file
            result = loaded_model.predict(input_data)
            #converting result to float
            result = round(float(result), 1)
            return result


        except Exception as ex :
            self.connection.log_insert_into_db(session, 'ErrorLog', str(ex))
            pass










