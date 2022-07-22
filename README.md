# restaurant_rating_prediction

This project is used to create an application which takes input from the user and the model predicts the rating. Training of the application is done using zomato dataset.

The training data is available in the project folder trainingfolder/zomato.csv.zip

Training data is available in the zip format and the code extracts and pass it to the model

This project aims to help user to note the things when a new restaurant is opened.

Training data is taken, extracted, there are many categorical columns present in the dataset like location, cuisines, dish_liked etc. In order to change the categorical 
columns to numerical columns one hot encoding is used. Then the data is passed to train. Random Forest Regressor is used to train this project

When the user navigates to prediction, few information is taken as input and the prediction is made according to that

Cassandra db is used in this application. Training data gets inserted into training_data table and prediction data gets inserted to prediction_data table

There is a requirements.txt file available to install the required files.

After downloading the projeect, do pip install requirements.txt and you are good to go for the application to run in local

The application is also deployed in google cloud run and the link is https://priyasganesan-92-qwq4mtspda-uc.a.run.app

High Level design document, Low Level design document also available to get the details of this project

