# @author: John McManus
# @date 03/11/2023
# © 2023. This work is licensed under a CC BY-NC-SA 4.0 license
# Program to correct each station's MSL to the proper value.
# Data pulled from NOAA web services defaults to the current epoch.

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
      
      print("Processing %s station" % element)
      
      # Open the input files
      # Open the station's current epoch MSL file
      inFile2 = open(element + "-Base-MSL.csv", "r")
      header1 = inFile2.readline().strip()
      data = inFile2.readline().strip()
      dataList = data.split(",")
      baseMSL = float(dataList[3])
      print (baseMSL)
      
      # Open the station's Annual MSL data file
      inFile3 = open(element + "-MSL.csv", "r")
      MSL_List = list()
      
      # Read the file header
      header1 = inFile3.readline().strip()
      for line in (inFile3):
         data = line.split(",")
         MSL_List.append(float(data[1]))
      
      if DEBUG:
         print("MSL contains %d elements" % len(MSL_List))
         print(MSL_List[1])
      
      # File containing the water levels and surge data
      inFile = open(element + ".csv", "r")
      
      # Read the input file headers from the raw data file
      header1 = inFile.readline()
      header2 = inFile.readline()
      
      # Open the output file
      outFile = open(element + "-Corrected.csv", "w")
      outFile.write("%s"% header1) 
      outFile.write("Date, Prediction-Corrected (m), Measured(m) , Surge\n")
      
      if DEBUG:
         print(header1)
         print(header2)
      
      # Is this the first year?
      start = True
      
      # Initialize the loop counter to 0 for the annual correction values
      inc = 0
      
      for line in inFile:
         line = line.strip()
         line = line.split(",")
         if len(line) != 4:
            print("Ouch",  line)
         
         # get the first element (month-date-year) from the line
         dateTime = line[0].split()
         if DEBUG:
            print("The date is %s" % dateTime)
         
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
               # Move to the next correction values
               inc = inc + 1
               pastYear = currentYear
            
            # Read the predicted value
            prediction = float(line[1]) - baseMSL + MSL_List[inc]
            
            if line[2] != " None":
               measured = float(line[2])
            else:
               measured = " None"
            
            if line[3] != "":
               surge = measured - prediction 
            else:
               surge = ""
         
         printDate = line[0]
         # Print the date, corrected Prediction, measured, surge
         outFile.write("%s, %f, %s, %s\n" % (printDate, prediction, str(measured), str(surge)))
      # close the output file
      outFile.close()
   

# Call the Main Function
main()