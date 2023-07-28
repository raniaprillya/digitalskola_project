class InsurancePre():
    def __init__(self):
        pass

    def colPreparation(self):
        labelEncoder = ['Gender','Driving_License','Previously_Insured','Vehicle_Damage']
        oneHotEncoder = ['Vehicle_Age','Region_Code','Policy_Sales_Channel']
        scallingStandar = ['Age','Annual_Premium','Vintage']
        
        return labelEncoder, oneHotEncoder, scallingStandar