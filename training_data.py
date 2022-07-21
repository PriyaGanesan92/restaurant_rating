from datetime import datetime

import pandas as pd
import numpy as np
from DB_Operations.database_connection import db_connection
from Data_Preprocessing.Preprocessing import preprocessing
from model_builder import model_build
from Log.Log import App_Logger
import zipfile
import os

class train_data :

    def __init__(self):
        self.connection  = db_connection()
        self.preprocess = preprocessing()
        self.file = open("Log/File_Logs/Db_error.txt", 'a+')
        self.logger = App_Logger()
        self.model_build = model_build()





    def getting_restaurant_information(self,session,data):
        """
        This method is useful for populating information for user to select required options
        Written by : Priya Ganesan
        Version : 1.0

        """

        try:

            self.connection.log_insert_into_db(session, 'DebugLog', 'Trying to start gathering information related to restaurant')
            rest_sub_type_individual = data.assign(rest_type=data['rest_type'].str.split(',')).explode('rest_type')
            self.rest_sub_type_individual = rest_sub_type_individual['rest_type'].str.strip()
            self.rest_sub_type_individual = self.rest_sub_type_individual.dropna().unique()
            #creating file to write restaurant subtype information
            restaurant_subtype_file = open("Training_data_information/restaurant_sub_type.txt", 'w+')
            #creating file to write restaurant type information
            restaurant_type_file = open("Training_data_information/restaurant_type.txt", 'w+')
            #creating file to write location related information
            location_file = open("Training_data_information/location.txt", 'w+')
            #creating file to write cuisines realted information
            cuisines_file = open("Training_data_information/cuisines.txt", 'w+')
            #adding restaurant subtype information in the respective file
            for item in self.rest_sub_type_individual:
                    # write each item on a new line
                    restaurant_subtype_file.write("%s\n" % item)
            restaurant_subtype_file.close()
            #adding cuisines related information in the respective file
            cuisines = data.assign(cuisines=data['cuisines'].dropna().str.split(',')).explode('cuisines')
            self.cuisines = cuisines['cuisines'].str.strip()
            self.cuisines = self.cuisines.dropna().unique()
            for item in self.cuisines:
                # write each item on a new line
                cuisines_file.write("%s\n" % item)
            cuisines_file.close()
            #adding location related information in the respective file
            self.location = data['location'].dropna().str.strip().unique()
            for item in self.location:
                # write each item on a new line
                location_file.write("%s\n" % item)
            location_file.close()
            #adding restaurant type related information in the respective file
            self.rest_type = data['listed_in(type)'].dropna().str.strip().unique()
            for item in self.rest_type:
                # write each item on a new line
                restaurant_type_file.write("%s\n" % item)
            restaurant_type_file.close()
            self.connection.log_insert_into_db(session, 'Info','Gathering information related to restaurant completed')

        except Exception as ex:
            self.connection.log_insert_into_db(session, 'ErrorLog', str(ex))
            pass







    def train_data(self,session,path):
        try :
            """
                This method is used to train data. Data need to be in some format for the model building to done.
                Necessary steps are done in this function
                
                Written by : Priya Ganesan
                Version : 1.0
            """
            self.connection.log_insert_into_db(session, 'DebugLog','Train data started')
            data = self.get_data_from_path(session, path)
            useful_data = self.get_useful_data(session, data)
            useful_rating_data = self.drop_null_rating_row(session, useful_data)
            self.getting_restaurant_information(session,useful_rating_data)
            self.insert_data_into_db(session, useful_rating_data)
            useful_rating_data = self.preprocess.remove_comma_from_cost(session, useful_rating_data)
            useful_rating_data = self.preprocess.mapping_encoding(session, useful_rating_data)
            X_train, X_test, y_train, y_test = self.preprocess.train_test_split(session, useful_rating_data)
            X_train, X_test = self.preprocess.one_hot_encoding(session, X_train, X_test)
            X_train, X_test = self.preprocess.creating_same_number_of_columns_in_train_test(session, X_train, X_test)
            X_train, X_test = self.preprocess.KNNImputer(session, X_train, X_test)
            return X_train,X_test,y_train,y_test

        except Exception as ex :
            self.connection.log_insert_into_db(session, 'ErrorLog', str(ex))




    def get_data_from_path(self,session,path):
        """
        This method is used to load data from the path. The training file is available in the path
        :param path: path of the file is present in the parameter

        Written by : Priya Ganesan
        Version : 1.0
        """
        try:
            self.connection.log_insert_into_db(session,'DebugLog', 'Trying to load training data into pandas dataframe')
            with zipfile.ZipFile(path, 'r') as zip_ref:
                zip_ref.extractall('/training_file/zip_extract')
            # Training data is read using pandas
            data = pd.read_csv('/training_file/zip_extract/zomato.csv')
            print(data)
            os.remove('/training_file/zip_extract/zomato.csv')
            self.connection.log_insert_into_db(session,'InfoLog','Loaded training data into pandas dataframe')
            return data
        except Exception as e:
            self.connection.log_insert_into_db(session,'ErrorLog', str(e))

    def get_useful_data(self,session,data):
        """
        The zomato dataset contains totally 17 columns. We are droping the columns which are not required and
        keeping only the required columns

        Written by : Priya Ganesan
        Version : 1.0
        """
        try :
            self.data = data
            self.columns = ['url', 'address', 'name', 'reviews_list', 'menu_item', 'listed_in(city)', 'votes']
            self.connection.log_insert_into_db(session,'DebugLog', 'Trying to extract useful columns from the available columns')
            #The columns which are not useful for training purpose is being dropped. Only useful columns are moved to next step
            self.useful_data = self.data.drop(labels= self.columns,axis=1)
            self.connection.log_insert_into_db(session,'InfoLog', 'Successfully extracted useful columns')
            return self.useful_data

        except Exception as e:
            self.connection.log_insert_into_db(session,'ErrorLog', str(e))


    def insert_data_into_db(self,session,useful_data):
        """
        This method is used to insert training data into cassandra db. 10 column values are getting inserted into db
        :param session: session for db connection
        :param useful_data: data to be inserted

        Written by : Priya Ganesan
        Version : 1.0
        """
        try :
            self.connection.log_insert_into_db(session, 'DebugLog','Trying to insert data into Training data table')
            session.execute("truncate table training_data")
            #training data is the table to load training related table
            query = "INSERT INTO training_data(id,online_order,book_table,rating,phone_number,location,restaurant_subtype,dishes_liked,cuisines,approximate_cost,restaurant_type) VALUES(uuid(), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            prepared = session.prepare(query)
            before = datetime.now()
            for index,row in useful_data.iterrows():
                #execute asynchronously to improve the speed of data insert. This is fast compared to execute statement
                session.execute_async(prepared, (str(row[0]), str(row[1]), str(row[2]), str(row[3]), str(row[4]),
                                           str(row[5]), str(row[6]), str(row[7]), str(row[8]), str(row[9])))

            self.connection.log_insert_into_db(session, 'InfoLog', 'Insert into Training Data completed')
            print("Elapsed: {}".format(datetime.now() - before))
        except Exception as e:
            self.connection.log_insert_into_db(session,'ErrorLog', str(e))



    def drop_null_rating_row(self,session,useful_data):

        """This method drops rows which has null values for rating column.
         The model is trained with the rating column and the prediction is done for rating column
         So we don't want the rows which is not having the null rating values

         Written by : Priya Ganesan
         version : 1.0"""
        try:
            self.connection.log_insert_into_db(session, 'DebugLog', 'Trying to delete row which has null value for rating column')
            useful_data.loc[useful_data['rate'] == 'NEW','rate'] = np.nan
            #Rate is the target column. We cannot do imputation in the target column as it will mislead the training and the prediction might be wrong
            useful_rating_data = useful_data[useful_data['rate'].isnull() == False]
            #Some of the value are '-', we are droping even that value as it will not be useful in model building
            useful_rating_data = useful_rating_data[useful_rating_data['rate'] != '-']
            self.connection.log_insert_into_db(session, 'InfoLog', 'Delete null value rating row completed')
            return useful_rating_data



        except Exception as e :
            self.connection.log_insert_into_db(session, 'ErrorLog', str(e))



