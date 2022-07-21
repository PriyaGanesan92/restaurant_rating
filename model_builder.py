from DB_Operations.database_connection import db_connection
from Log.Log import App_Logger
from sklearn.ensemble import RandomForestRegressor
import pickle
import bz2

class model_build :

    def __init__(self):
        self.connection = db_connection()
        self.file = open("Log/File_Logs/Db_error.txt", 'a+')
        self.logger = App_Logger()



    def build_model(self,session,X_train,X_test,y_train,y_test):
        """
        This method is used to build random forest model. Since there is only 40000+ records only single model which is efficient is built.
        Clustering of data is not done as the number of records is low
        :param session: database connectivity
        :param X_train: Training features
        :param X_test: Test Features
        :param y_train: Training features
        :param y_test: Test target

        Written by : Priya Ganesan
        Version : 1.0
        """
        try :
            self.connection.log_insert_into_db(session, 'DebugLog', 'Trying to start to build model')
            #Model is build using the below step
            random_regressor_without_grid_search_cv = RandomForestRegressor(n_jobs=-1)
            random_regressor_without_grid_search_cv.fit(X_train,y_train)
            score1 = random_regressor_without_grid_search_cv.score(X_test,y_test)
            #the model is save to a pickle file so it can be used while predicting result for the input data
            #pickle.dump(random_regressor_without_grid_search_cv, open('model_without_grid_search_cv.pkl', 'wb'))
            ofile = bz2.BZ2File("RandomForestBinaryData", 'wb')
            pickle.dump(random_regressor_without_grid_search_cv, ofile)
            self.connection.log_insert_into_db(session, 'InfoLog', 'Building model completed')
            return(score1)


        except Exception as ex :
            self.connection.log_insert_into_db(session, 'ErrorLog', str(e))










