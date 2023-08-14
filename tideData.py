# @author: John McManus
# @date 03/11/2023
# © 2023. This work is licensed under a CC BY-NC-SA 4.0 license

# Define the tideData storage class

class tideData:
    # @parameter dateTime: a datetime object that contains the date and time 
    # @parameter prediction: the predicted water level
    # @parameter measured: the measured water level
    # @parameter surge: the surge value
    
    def __init__(self, dateTime, prediction, measured, surge):
        self._dateTime = dateTime
        self._prediction = prediction
        self._measured = measured
        self._surge = surge
        self._residual = 0.0
        
    def __repr__(self):
        return("%s, %f, %f, %f, %f \n" % (self._dateTime, self._prediction, self._measured, self._surge, self._residual))

# Define the stormData storage class

class stormData:
    # @parameter startDate: a datetime object that contains the start date and time of the storm
    # @parameter endDate: a datetime object that contains the end date and time of the storm
    # @parameter SEPI: the SEPI calculated for the storm
    
    def __init__(self, startDate, endDate, SEPI):
        self._startDate = startDate
        self._endDate = endDate
        self._SEPI = SEPI
        self._duration = self._endDate - startDate
        self._timeFromPrevious = 0.0
        
    def __repr__(self):
        return("%s, %s, %f, %s, %s \n" % (self._startDate, self._endDate, self._SEPI, self._duration,self._timeFromPrevious))