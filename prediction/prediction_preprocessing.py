from DB_Operations.database_connection import db_connection
import pandas as pd

class prediction_preprocess :

    def __init__(self):
        self.connection = db_connection()
        self.file = open("Log/File_Logs/Db_error.txt", 'a+')


    def mapping(self,session,data):
        """
        This method is used to map the input data given by the user
        :param session: Database connectivity
        :param data: data to which mapping needs to be done

        """
        try :
            self.connection.log_insert_into_db(session, 'DebugLog', 'Trying to start mapping for input data')
            data['online_order'] = data['online_order'].map({'Yes': 1, 'No': 0})
            data['book_table'] = data['book_table'].map({'Yes': 1, 'No': 0})
            data['phone'] = data['phone'].map({'Yes':1,'No':0})
            dish_liked_input = data['dish_liked'].to_string(index=False)
            #checking if the recommended dish is present in dis_liked file. If it is present then mapped to 1 else mapped to 0
            with open('Training_data_information/dish_liked.txt') as f:
                dish_liked = f.read().splitlines()
                f.close()
            if len(dish_liked_input) == 0:
                data['dish_liked'] = 0
            if dish_liked_input.upper() in dish_liked :
                data['dish_liked'] = 1
            else :
                data['dish_liked'] = 0
            self.connection.log_insert_into_db(session, 'InfoLog', 'Mapping of input data is completed')
            return data

        except Exception as ex :
            self.connection.log_insert_into_db(session, 'ErrorLog', str(ex))


    def one_hot_encoding_for_prediction_data(self, session, input_data):
        try:
            self.connection.log_insert_into_db(session, 'DebugLog', 'Trying to start one hot encoding process')
            #restaurant type and cuisines column are list, it is converted into string for encoding
            input_data['rest_type'] = [','.join(map(str, l)) for l in input_data['rest_type']]
            input_data['cuisines'] = [','.join(map(str, l)) for l in input_data['cuisines']]
            column_list = ['location', 'rest_type', 'cuisines', 'listed_in(type)']
            for column in column_list:
                input_data = pd.concat([input_data.drop(column, 1), input_data[column].str.get_dummies(sep=",")], 1)
            print(len(input_data.columns))
            self.connection.log_insert_into_db(session, 'InfoLog', 'One hot encoding process going on')

            return input_data

        except Exception as ex:
            self.connection.log_insert_into_db(session, 'ErrorLog', str(ex))


    def creating_same_number_of_columns_in_input_data(self,session,X_train,input_data):
        """
        Since we are doing one hot encoding there might be mismatch in the train and input data.
        So we are performing some operations to make sure there is same number of columns in train and test data

        """
        try :
            self.connection.log_insert_into_db(session, 'DebugLog', 'Matching equal number of train and test columns started')

            input_data.columns = input_data.columns.str.strip()
            #if there is any duplicate values, those values are removed from input data
            input_data = input_data.loc[:, ~input_data.columns.duplicated()]
            missing_cols = set(X_train.columns) - set(input_data.columns)
            for c in missing_cols:
                input_data[c] = 0
            #checking if there is any extra column that are not present in training data is present. If so, that is removed
            missing_cols_train = set(input_data.columns) - set(X_train.columns)
            if len(missing_cols_train) > 0:
                for value in missing_cols_train:
                    input_data = input_data.drop(columns=[value])
            input_data = input_data[X_train.columns]
            print(len(X_train.columns))
            print(len(input_data.columns))
            self.connection.log_insert_into_db(session, 'InfoLog','Matching equal number of train and input columns completed')

            return input_data

        except Exception as e:
            self.connection.log_insert_into_db(session, 'ErrorLog', str(e))




