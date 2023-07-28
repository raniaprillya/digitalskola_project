import os
import pickle
import time

import pandas as pd
import numpy as np


from modules.insurance_pre import InsurancePre

class InsurancePredict():
    def __init__(self):
        pass

    def runModel(data, typed='multi'):
        path = "/home/rani/project7/modules/packages/"
        model = pickle.load(open(path + 'model_InsuranceRecommendation.pkl', 'rb'))
        col_p = pickle.load(open(path + 'columnPreparation.pkl', 'rb'))
        col_m = pickle.load(open(path + 'columnModelling.pkl', 'rb'))

        X = data[col_p]
        colEncoder, colpOneHotEncoder, colStandarScaler = InsurancePre().colPreparation()
        for col in X.columns:
            prep = pickle.load(open(path + 'prep' + col + '.pkl', 'rb'))
            if col in colpOneHotEncoder:
                dfTemp = pd.DataFrame(prep.transform(X[[col]]).toarray())
                X = pd.concat([X.drop(col, axis=1), dfTemp], axis=1)
            else:
                dfTemp = pd.DataFrame(prep.transform(X[[col]]))
                X = pd.concat([X.drop(col, axis=1), dfTemp], axis=1)
        X.columns = col_m
        
        if typed == 'multi':
            y = model.predict(X)
            return y
        
        elif typed == 'single':
            y = model.predict(X)[0]
            if y == 0:
                return 0
            else:
                return 1
        else:
            return False
