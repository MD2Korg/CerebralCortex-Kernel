import numpy as np

""" dummy classes """
class DummyAvailabilityModel:
    def __init__(self):
        pass
    
    def fit(self, X, y):
        pass
    
    def predict(self, X):
        return 1
    
class DummyStressModel:
    def __init__(self):
        pass
    
    def fit(self, X, y):
        pass
    
    def predict(self, X):
        return 1
    
""" dummy preprocessing functions """
def preprocess_data(raw_data):
    processed_data = raw_data
    return processed_data