# @author: John McManus
# @date 03/27/2023
# © 2023. This work is licensed under a CC BY-NC-SA 4.0 license
# Program to calculate the storm and SEPI data using annual surge thresholds


# Import the datetime, timedelta, and date classes
from datetime import datetime, timedelta, date

# Import the timeData and stormData Abstract Data Types
from tideData import tideData, stormData

# importing the statistics class
import statistics

def main():
   
   # A boolean variable used to turn print statements used during debugging on and off
   DEBUG = False
   
   # The list of station names
   stationNM = ["Portland", "Boston", "Montauk", "The Battery", "Sandy Hook", "Sewells Pt", "Wilmington NC", 
               "Charleston SC", "Fernandina Beach", "Key West", "Newport", "Atlantic City-A", "Atlantic City-B"]
   
   # For each station in the list
   for element in stationNM:
      # Open the input files
      # File containing the water levels and surge data
      inFile = open(element + "-Corrected.csv", "r")
      
      print("Processing %s station" % element)
      
      # File containing the storm threshold
      inFile2 = open(element + "-MHW.csv", "r")
      
      # File containing the Surge threshold
      inFile3 = open(element + "AnnualSurge.csv", "r")       
      
      # Open the output files
      outFile3 = open(element + "FinalStorm.csv", "w")
      outFile4 = open(element + "Annual SEPI.csv", "w")
      
      # Read the storm threshold input file and store the values in a list
      thresholdList = list()
      
      header1 = inFile2.readline().strip()
      for line in inFile2:
         line = line.strip()
         elements = line.split(",")
         temp = Threshold(int(elements[0]), float(elements[1]))
         if DEBUG:
            print(temp)
         thresholdList.append(temp)
         
      # Read the Surge threshold input file and store the values in a list
      surgeThresholdList = list()
         
      header2 = inFile3.readline().strip()
      for line in inFile3:
         line = line.strip()
         elements = line.split(",")
         temp = Threshold(int(elements[0]), float(elements[3]))
         if DEBUG:
            print(temp)
         surgeThresholdList.append(temp)      
      
      # if DEBUG = True: Open the temp files to write data
      if DEBUG:
         outFile = open(element + "TEMP.csv", "w")
         outFile2 = open(element + "TEMPStorm.csv", "w")  
      
      # Read the input file headers from the raw data file
      header1 = inFile.readline().strip()
      header2 = inFile.readline().strip()
      
      print(header1)
      print(header2)
      
      # Initialize the lists to store the preliminay data for processing
      dataList = []
      surgeList = []
      preStormList = []
      stormList = []
      
      for line in inFile:
         line = line.strip()
         line = line.split(",")
         if len(line) != 4:
            print("Missing data element in the file",  line)
         
         # get the first element (month-date-year) from the line
         dateTime = line[0].split()
         dateComponents = dateTime[0].split("-")
         
         # Get the time from the line of data
         if len(dateTime) < 2:
            print("Short Date Time", dateTime)
         else:
            timeFix = dateTime[1].rstrip(",")
            timeFix = timeFix.split(":")
            for i in range (len(timeFix)):
               timeFix[i].rstrip(",")
            
            # create the date time object
            rawDateTime = datetime(int(dateComponents[0]), int(dateComponents[1]), int(dateComponents[2]), int(timeFix[0]), int(timeFix[1])) 
            
            prediction = float(line[1])
            
            if line[2] != "  None":
               measured = float(line[2])
            else:
               measured = 0.0
            
            if line[3] != "":
               surge = float(line[3])
            else:
               surge = 0.0 
               
            currentLine = tideData(rawDateTime, prediction, measured, surge)
            dataList.append(currentLine)
      
      print(dataList[0]._dateTime.year, dataList[0]._surge)
      
      if DEBUG:
         print("the date - time list has %s elements" % len(dataList))
      
      # Set the threshold index to 0 value. 
      thresholdIndex = 0
      
      for i in range(len(dataList)):
         if dataList[i]._dateTime.year > thresholdList[thresholdIndex].year:
            thresholdIndex = thresholdIndex + 1
            if DEBUG:
               print("Data Year: %s, Threshold Year: %s" % (dataList[i]._dateTime.year, thresholdList[thresholdIndex].year))
               
         # if the surge is > the surge threshold and the verified (measured) value is > 0
         # compute the residual as verfied * surge
         if dataList[i]._surge > surgeThresholdList[thresholdIndex].threshold and dataList[i]._measured > thresholdList[thresholdIndex].threshold:
            dataList[i]._residual =  dataList[i]._measured * dataList[i]._surge
            #add the Data to the prestorm list for processing
            preStormList.append(dataList[i])
            
         # Write the processed data to the TEMP file
         if DEBUG:
            outFile.write(str(dataList[i]))
      
      # Clear the dataList
      dataList = []
      
      # Process the data to identify storms
      # DELTA: A constant that represents 12 hours 
      DELTA = timedelta(hours = 12)
      print("The time constant (hours) used to determine if points are in the same storm %s" % DELTA)
      
      # Set the loop counter to 1 so we can compare to the previous data point 
      i = 1
      
      # Loop through the prestorm data
      # set start to the first data point that contains a storm.
      start = preStormList[i-1]._dateTime
      sepi = preStormList[i-1]._residual
      preStormList[i-1]._timeFromPrevious = 0
      
      # While there is data in the prestorm list
      while i < len(preStormList):
         loop = False
         
         # While the data points are from the same storm
         while i < len(preStormList) and (preStormList[i]._dateTime - preStormList[i-1]._dateTime) < DELTA:
            sepi = sepi + preStormList[i]._residual
            i = i + 1
            loop = True
         if not loop:
            sepi = preStormList[i-1]._residual
         
         # Set the end of the storm and create a storm object
         end = preStormList[i-1]._dateTime
         temp = stormData(start, end, sepi) 
         stormList.append(temp)
         if DEBUG:
            outFile2.write(str(temp))
         sepi = 0
         if i < len(preStormList):
            start = preStormList[i]._dateTime
            i = i + 1
      
      # Write the storm Sepi Data to the output3 file
      outFile3.write("Start Date, Storm SEPI, Time From Previous Storm \n") 
      outFile3.write("%s, %f, %s \n" % (stormList[0]._startDate, stormList[0]._SEPI, stormList[0]._timeFromPrevious))
      for i in range (1, len(stormList)):
         stormList[i]._timeFromPrevious = stormList[i]._startDate - stormList[i-1]._endDate
         # Extract the days component from the time delta object
         days = stormList[i]._timeFromPrevious.days
         seconds = stormList[i]._timeFromPrevious.seconds
         partialDay = seconds/86400
         days = days + partialDay
         outFile3.write("%s, %f, %f \n" % (stormList[i]._startDate, stormList[i]._SEPI, days))
         
      # Write the number of storms in a year and the annual SEPI (sum of the storm Sepi)
      outFile4.write("Year, Number of Storms, Total SEPI \n")
      currentYear = stormList[0]._startDate.year
      stormCount = 0
      annualSEPI = 0
      for i in range (0, len(stormList)):
         if stormList[i]._startDate.year == currentYear:
            stormCount = stormCount + 1
            annualSEPI = annualSEPI + stormList[i]._SEPI
         else:
            outFile4.write("%s, %d, %f \n" % (currentYear, stormCount, annualSEPI))
            currentYear = stormList[i]._startDate.year
            stormCount = 1
            annualSEPI = stormList[i]._SEPI
      outFile4.write("%s, %d, %f \n" % (currentYear, stormCount, annualSEPI))
      
      # Close the input files   
      inFile.close()
      outFile3.close()
      outFile4.close()
      
      if DEBUG:
         outFile.close()
         outFile2.close()
      print("Station Complete")
   print("Analysis Complete")
   
# define the threshold storage class
# @parameter year: The year the data is from
# @parameter threshold: The annual storm threshold value
class Threshold():
   def __init__(self, year, threshold):
      self.year = year
      self.threshold = threshold
       
   def __repr__(self):
      return("%s, %f" % (self.year, self.threshold))   

# Call the Main Function
main()