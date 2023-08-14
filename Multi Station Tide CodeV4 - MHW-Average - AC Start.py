# @author: John McManus
# © 2023. This work is licensed under a CC BY-NC-SA 4.0 license
# MultiStation Tide Data analysis
# Program to query the NOAA Data source to get monthly mean tidal datums for Atlantic City from 1922 to 1969
# No Data available for this station in 1970

import requests
import time

DEBUG = False

def main():
    # Station ID, start year for data, and the station name
    stationID = [8534720]
    stationYR = [1922]
    stationNM = ["Atlantic City"]
    stationEnd = 1970
    
    # initialize the request string with format specifiers to accept
    # the start date, the end date, and the station ID
    requestStringTides = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?product=monthly_mean&application=RMC_Research&begin_date=%d0101&end_date=%d1231&datum=STND&station=%d&time_zone=GMT&units=metric&format=json"
    
    
    for sID in range (len(stationID)):
        startYear = stationYR[sID]
    
        # set the filename
        fileName = stationNM[sID] + "A-MHW.csv"
        
        outFile = open(fileName, "w")
        outFile.write("Year, Average MHW(m) \n")        

        # Loop throught the years of data, stopping at the stationEnd value
        for i in range(startYear, stationEnd):
            
            # Build the url using string formatting 
            urlTides = (requestStringTides % (i, i, stationID[sID]))
            if DEBUG:
                print(urlTides)
            
            # Post the requests
            responseTides = requests.post(urlTides)
            
            # decode the replies
            dataTides = responseTides.json()
            if DEBUG:
                print(dataTides)
            
            numTidesPoints = len(dataTides["data"])
            
            if DEBUG:
                print("numTidesPoints = ", numTidesPoints)            
            
            # Initialize the loop counters
            m = 0
            total = 0
            count = 0
            year = int(dataTides["data"][0]["year"])
            
            for j in range (numTidesPoints):
                if dataTides["data"][j]["MHW"] != "":
                    total = total + float(dataTides["data"][j]["MHW"])
                    count = count + 1
            if count > 0:
                average = total / count
                if count != 12:
                    print("%d months data %d" % (count, i))
            else:
                print("Cannot calculate average for year %d" % year)
                average = 0.0
                
            outFile.write("%d, %s \n" % (year, average))
            
        print("%s is complete" % fileName)
        outFile.close()
    
# Call the main function
main()