# @author: John McManus
# @date 03/11/2023
# © 2023. This work is licensed under a CC BY-NC-SA 4.0 license
# Program to compute the Annual Surge values for multiple stations using processed NOAA Data

# Import the datetime, timedelta, and date classes
from datetime import datetime, timedelta, date

# importing the statistics class
import statistics

def main():
   # A boolean variable used to turn print statements used during debugging on and off
   DEBUG = False
   
   stationNM = ["Portland", "Boston", "Montauk", "The Battery", "Sandy Hook", "Sewells Pt", "Wilmington NC", 
               "Charleston SC", "Fernandina Beach", "Key West", "Newport", "Atlantic City-A", "Atlantic City-B"]
   
   # For each station in the list
   for element in stationNM:
      # Open the input files
      # File containing the water levels and surge data
      inFile = open(element + "-Corrected.csv", "r")
      
      print("Processing %s station" % element)
            
      # Open the output files
      outFile = open(element + "AnnualSurge.csv", "w")
      outFile.write("currentYear, averageSurge, standardDevSurge, surgeThreshold \n")       
      
      # Read the input file headers from the raw data file
      header1 = inFile.readline().strip()
      header2 = inFile.readline().strip()
      
      print(header1)
      print(header2)
      
      surgeList = []
      annualSurgeList = []

      # Is this the first year?
      start = True
      
      for line in inFile:
         line = line.strip()
         line = line.split(",")
         if len(line) != 4:
            print("Ouch",  line)
         #print(line)
         
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
            currentYear = int(dateComponents[0])
 
            if start:
               pastYear = currentYear
               start = False
            
            if pastYear != currentYear:
               # compute the annual average and standard deviation
              
               averageSurge = sum(surgeList) / len(surgeList)
               print("%d:The average of the surge values = %.5f" % (currentYear, averageSurge))
               
               standardDevSurge = statistics.stdev(surgeList)
               print("The Standard deviation of the surge values = %.5f" % standardDevSurge)
               
               # Calculate the surge threshold
               surgeThreshold = (standardDevSurge * 2) + averageSurge
               print("The surge threshold = %.5f" % surgeThreshold)
               
               # Print the Annual Average surge, StDDev, Threshold
               outFile.write("%s, %f, %f, %f\n" % (pastYear, averageSurge, standardDevSurge, surgeThreshold))
               # currentYear = currentYear + 1
               surgeList = []
               pastYear = currentYear
            
            # Read the dredicted value
            prediction = float(line[1])
            
            if line[2] != "  None":
               measured = float(line[2])
            else:
               measured = 0.0
            
            if line[3] != "":
               surge = float(line[3])
               surgeList.append(surge)
            else:
               surge = 0.0
               
      averageSurge = sum(surgeList) / len(surgeList)
      print("%d:The average of the surge values = %.5f" % (currentYear, averageSurge))
      
      standardDevSurge = statistics.stdev(surgeList)
      print("The Standard deviation of the surge values = %.5f" % standardDevSurge)
      
      # Calculate the surge threshold
      surgeThreshold = (standardDevSurge * 2) + averageSurge
      print("The surge threshold = %.5f" % surgeThreshold)
      
      # Print the Annual Average surge, StDDev, Threshold
      outFile.write("%s, %f, %f, %f\n" % (pastYear, averageSurge, standardDevSurge, surgeThreshold))


# Call the Main Function
main()