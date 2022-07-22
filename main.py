from wsgiref import simple_server
from training_data import train_data

from flask import Flask, render_template, request,url_for,redirect
from flask_cors import CORS, cross_origin
import flask_monitoringdashboard as dashboard
from DB_Operations.database_connection import db_connection

from prediction.rating_prediction import predict_rating
from model_builder import model_build

import os




app = Flask(__name__)
dashboard.bind(app)
CORS(app)


@app.route('/')
@cross_origin()

def index_page():

    return render_template('index.html')

@app.route('/loading',methods=['GET','POST'])
@cross_origin()
def loading():
    return render_template("loading.html")


@app.route('/predict',methods=['GET', 'POST'])
@cross_origin()

def prediction_page():
    with open('Training_data_information/location.txt') as f:
        location_list = f.read().splitlines()
        f.close()
    with open('Training_data_information/restaurant_type.txt') as f:
        restaurant_type = f.read().splitlines()
        f.close()
    with open('Training_data_information/restaurant_sub_type.txt') as f:
        restaurant_subtype = f.read().splitlines()
        f.close()
    with open('Training_data_information/cuisines.txt') as f:
        cuisines_list = f.read().splitlines()
        f.close()
    cost_list = []
    for i in range(100,1000,100):
        cost_list.append(i)
    for i in range(1000,5500,500):
        cost_list.append(i)


    return render_template('prediction.html',location_list=location_list,restaurant_type=restaurant_type,restaurant_subtype=restaurant_subtype,cuisines_list=cuisines_list,cost_list=cost_list)

@app.route('/predictionload',methods = ['GET','POST'])
@cross_origin()

def prediction_load():
    online_order = request.form['online_order']
    book_table = request.form['book_table']
    phone_number = request.form['phone_number']
    location_list = request.form['location_list']
    restaurant_type = request.form['restaurant_type']
    restaurant_subtype = request.form.getlist('restaurant_subtype')
    cuisines_list = request.form.getlist('cuisines_list')
    cost = request.form['cost_list']
    recommended_dish = request.form['dish']
    formlist = []
    formlist.extend((online_order,book_table,phone_number,location_list,restaurant_type,restaurant_subtype,cuisines_list,cost,recommended_dish))
    form_file = open("form_file.txt", 'w+')
    for item in formlist:
        # write each item on a new line
        form_file.write("%s\n" % item)
    form_file.close()
    return render_template('predictionloading.html')



@app.route('/prediction',methods=['GET', 'POST'])
@cross_origin()

def prediction():

    with open('form_file.txt') as f:
        input_list = f.read().splitlines()
        f.close()

    db = db_connection()
    session = db.establish_db_connection()
    prediction_rating = predict_rating()
    encoded_aligned_data = prediction_rating.prediction_steps(session,input_list)

    result = prediction_rating.prediction(session,encoded_aligned_data)
    print(result)
    return render_template('result.html', value=result)



@app.route('/train',methods=['GET', 'POST'])
@cross_origin()

def training():
    db = db_connection()
    session = db.establish_db_connection()
    path = 'training_file/zomato.csv.zip'
    training_data = train_data()
    X_train,X_test,y_train,y_test = training_data.train_data(session, path)
    model = model_build()
    accuracy = model.build_model(session, X_train, X_test, y_train, y_test)
    return render_template('training_result.html',accuracy=accuracy)



port = int(os.getenv("PORT",5000))

if __name__ == '__main__':

    host = '0.0.0.0'
    httpd = simple_server.make_server(host, port, app)
    print("Serving on %s %d" % (host, port))
    httpd.serve_forever()

















