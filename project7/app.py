from flask import Flask
from flask import request
from flask  import jsonify
import pandas as pd
from modules.insurace_predict import InsurancePredict

app = Flask(__name__)

@app.route("/")
def home():
    return "API Modelling"

@app.route("/predict", methods=["POST"])
def predict():
    data = request.get_json()
    df = pd.DataFrame(data, index=[0])
    predict_code = InsurancePredictS.runModel(df, typed='single')
    
    result_predict = 'Interested' if predict_code == 1 else 'Not Interested'


    return jsonify({
        "status":"Predicted",
        "predict_code" : predict_code,
        "result" : result_predict,
        "data":data
    })