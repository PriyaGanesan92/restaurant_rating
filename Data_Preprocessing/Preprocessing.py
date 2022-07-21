from DB_Operations.database_connection import db_connection
from sklearn.model_selection import train_test_split
import pandas as pd
from sklearn.impute import KNNImputer
from Log.Log import App_Logger



class preprocessing:
    def __init__(self):
        self.connection = db_connection()
        self.file = open("Log/File_Logs/Db_error.txt", 'a+')
        self.logger = App_Logger()


    def remove_comma_from_cost(self, session, useful_rating_data):
        """
        Cost column value is given as 1,500 type. Conversion into float and removing the comma is done in this method
        :param session: database connectivity
        :param useful_rating_data: training data

        Written by : Priya Ganesan
        Version : 1.0

        """
        try:
            self.connection.log_insert_into_db(session, 'DebugLog', 'Trying to do preprocessing step in cost column')
            #comma string is replaced by empty. After executing the following statement, 1,000 is converted to 1000
            useful_rating_data['approx_cost(for two people)'] = useful_rating_data[
                'approx_cost(for two people)'].str.replace(",", "")
            #converting the column to float since it will be easy for computation while creating model
            useful_rating_data['approx_cost(for two people)'] = useful_rating_data[
                'approx_cost(for two people)'].astype(float)
            useful_rating_data = useful_rating_data.reset_index(drop=True)
            self.connection.log_insert_into_db(session, 'InfoLog', 'Preprocessing step in cost column completed')
            return useful_rating_data

        except Exception as e:
            self.connection.log_insert_into_db(session, 'ErrorLog', str(e))

    def train_test_split(self, session, useful_rating_data):
        """
        This method is used to split the data into train and test. This will be useful in calculating the accuracy and test the model
        :param session: database connectivity
        :param useful_rating_data: training data

        Written by : Priya Ganesan
        Version : 1.0

        """
        try:
            self.connection.log_insert_into_db(session, 'DebugLog', 'Trying to split train and test data')
            X = useful_rating_data.drop(columns=['rate'])
            y = useful_rating_data['rate']
            #80% of the data is taken for training and 20% for testing
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=200)
            self.connection.log_insert_into_db(session, 'InfoLog', 'Train , Test split completed successfully')
            return X_train, X_test, y_train, y_test


        except Exception as e:
            self.connection.log_insert_into_db(session, 'ErrorLog', str(e))

    def one_hot_encoding(self, session, X_train, X_test):
        """
        There are number of categorical columns in this dataset. Conversion of categorical columns into numerical values is necessary
        :param session: database connectivity
        :param X_train: Training features
        :param X_test: Testing features
        :param y_train: Training target
        :param y_test: Testing target
        """

        try:
            self.connection.log_insert_into_db(session, 'DebugLog', 'One hot encoding part started')
            #List of columns which are categorical are passed
            column_list = ['location', 'rest_type', 'cuisines', 'listed_in(type)']
            X_train,X_test = self.one_hot_encoding_conversion(session, X_train, X_test,column_list)
            self.connection.log_insert_into_db(session, 'InfoLog', 'One hot encoding part completed successfully')
            return X_train,X_test

        except Exception as e:
            self.connection.log_insert_into_db(session, 'ErrorLog', str(e))

    def one_hot_encoding_conversion(self, session, X_train, X_test, column_list):

        try:
            self.connection.log_insert_into_db(session, 'DebugLog', 'One hot encoding conversion started')
            for column in column_list:
                #pandas get dummies method is used to create dummies for the data. Number of columns increase while doing this method
                X_train = pd.concat([X_train.drop(column, 1), X_train[column].str.get_dummies(sep=",")], 1)
                X_test = pd.concat([X_test.drop(column, 1), X_test[column].str.get_dummies(sep=",")], 1)
            self.connection.log_insert_into_db(session, 'InfoLog', 'One hot encoding conversion completed successfully')
            return X_train,X_test

        except Exception as e:

            self.connection.log_insert_into_db(session, 'ErrorLog', str(e))


    def creating_same_number_of_columns_in_train_test(self,session,X_train,X_test):
        """
        Since we are doing one hot encoding there might be mismatch in the train and test data.
        So we are performing some operations to make sure there is same number of columns in train and test data

        Written by : Priya Ganesan
        Version : 1.0

        """
        try :
            self.connection.log_insert_into_db(session, 'DebugLog', 'Matching equal number of train and test columns started')
            #removing extra space present in the train data column name
            X_train.columns = X_train.columns.str.strip()
            #removing extra space present in the test data column name
            X_test.columns = X_test.columns.str.strip()
            #Sometimes columns can be duplicated. So duplicate columns are removed in this step
            X_train = X_train.loc[:, ~X_train.columns.duplicated()]
            X_test = X_test.loc[:, ~X_test.columns.duplicated()]
            #If the column present only in the train and not in the test, then that columns are mapped to zero in the test
            missing_cols = set(X_train.columns) - set(X_test.columns)
            for c in missing_cols:
                X_test[c] = 0
            #If the column present only in the test, then that columns are removed to match the number of columns in the train data and test data
            missing_cols_train = set(X_test.columns) - set(X_train.columns)

            if len(missing_cols_train) > 0:
                for value in missing_cols_train:
                    X_test = X_test.drop(columns=[value])
            #to arrange train data and test data in the same order
            X_test = X_test[X_train.columns]
            #train data is stored to act as a reference to the input data from prediction
            X_train.to_csv('X_train.csv',index=False)
            self.connection.log_insert_into_db(session, 'InfoLog','Matching equal number of train and test columns completed')

            return X_train, X_test

        except Exception as e:
            self.connection.log_insert_into_db(session, 'ErrorLog', str(e))

    def mapping_encoding(self, session, useful_rating_data):
        """
        For few columns encoding can be done directly by mapping values. Those type of encoding is done in this method
        :param X_train: Training data
        :param X_test: Testing data

        """
        try :
            self.connection.log_insert_into_db(session, 'DebugLog','Trying to start mapping_encoding')
            #rate is present as 4.1/5. After executing the below statement it will be stored as 4.1
            useful_rating_data['rate'] = useful_rating_data['rate'].str[0:3]
            #online_order is mapped to numerical number
            useful_rating_data['online_order'] = useful_rating_data['online_order'].map({'Yes': 1, 'No': 0})
            #book_table column is mapped to numeric value
            useful_rating_data['book_table'] = useful_rating_data['book_table'].map({'Yes': 1, 'No': 0})
            useful_rating_data['phone'] = useful_rating_data['phone'].fillna(0)
            #if phone number is present then it is mapped to 1, or else it is mapped to 0
            useful_rating_data['phone'] = useful_rating_data['phone'].apply(lambda x: 1 if x != 0 else x)
            self.creating_dish_liked_list(session,useful_rating_data)
            useful_rating_data['dish_liked'] = useful_rating_data['dish_liked'].fillna(0)
            useful_rating_data['dish_liked'] = useful_rating_data['dish_liked'].apply(lambda x: 1 if x != 0 else x)
            self.connection.log_insert_into_db(session, 'InfoLog', 'Mapping encoding completed successfully')
            return useful_rating_data
        except Exception as e :
            self.connection.log_insert_into_db(session, 'ErrorLog', str(e))


    def creating_dish_liked_list(self,session,useful_rating_data):
        try :
            """
            User gives input for recommended dish for their restaurant. We are taking some of the top liked dishes from the
            training data. If the recommended dish is present in the data, we are mapping it to 1 or else we will map to 0.
            This information is useful while choosing the restaurant. For example if we are ordering food from a particular restaurant,
            the first thing which we see is recommended dish. If we have our liked dish, then we will go ahead and order the food in that
            particular restaurant. So this information is important in rating.
            
            Written by: Priya Ganesan
            Version : 1.0
            """
            self.connection.log_insert_into_db(session, 'DebugLog', 'Trying to create dish liked file')
            #since dish_liked column is comma seperated, we are trying to take individual dishes
            useful_rating_data = useful_rating_data.assign(dish_liked=useful_rating_data['dish_liked'].str.split(',')).explode('dish_liked')
            #removing the extra space present in the dish
            useful_rating_data['dish_liked'] = useful_rating_data['dish_liked'].str.strip()
            #converting to upper case as it will be easy to compare the user input to the list of the dish liked
            useful_rating_data['dish_liked'] = useful_rating_data['dish_liked'].str.upper()
            dish_liked_counts = useful_rating_data['dish_liked'].value_counts().rename('liked_dish_count')
            df1 = pd.DataFrame(dish_liked_counts)
            #taking the top dish liked and the value counts. Dishes whose value count > 0 is chosen in the list
            df2 = df1[df1['liked_dish_count'] > 50]
            dish_liked_list = list(df2.index)
            dish_liked_file = open("Training_data_information/dish_liked.txt", 'w+')
            for item in dish_liked_list:
                    # write each item on a new line
                    dish_liked_file.write("%s\n" % item)
            dish_liked_file.close()

            self.connection.log_insert_into_db(session, 'InfoLog', 'dish liked file successfully created')


        except Exception as ex:
            self.connection.log_insert_into_db(session, 'ErrorLog', str(ex))



    def KNNImputer(self,session,X_train,X_test):
        """
        The dataset is having null values. This method imputes null values
        Written by : Priya Ganesan
        Version : 1.0
        """
        try :
            self.connection.log_insert_into_db(session, 'DebugLog','Trying to start imputation')
            #giving the value of k =2 for performing knn
            imputer = KNNImputer(n_neighbors=2)
            impute = imputer.fit(X_train)
            #the imputation done for train is also followed for the test data
            X_train = impute.transform(X_train)
            X_test = impute.transform(X_test)
            return X_train,X_test

        except Exception as e :
            self.connection.log_insert_into_db(session, 'ErrorLog', str(e))




