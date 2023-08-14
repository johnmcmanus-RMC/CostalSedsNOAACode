# @author: John McManus
# © 2023. This work is licensed under a CC BY-NC-SA 4.0 license
# MultiStation Tide Data analysis
# Program to query the NOAA Data source to get monthly mean tidal datums
#

# Import the requests library and the time library
import requests
import time

DEBUG = False

def main():
    stationID = [8418150, 8443970, 8510560, 8518750, 8531680, 8638610, 8658120, 8665530, 8720030, 8724580, 8452660]
    stationYR = [1912, 1921, 1947, 1926, 1932, 1927, 1935, 1922, 1938, 1926, 1938]
    stationNM = ["Portland", "Boston", "Montauk", "The Battery", "Sandy Hook", "Sewells Pt", "Wilmington NC", 
                 "Charleston SC", "Fernandina Beach", "Key West", "Newport"]
    
    # initialize the monthly mean request string with format specifiers to accept
    # the start date, the end date, and the station ID. Data is relative to the Station Datum
    requestStringTides = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?product=monthly_mean&application=RMC_Research&begin_date=%d0101&end_date=%d1231&datum=STND&station=%d&time_zone=GMT&units=metric&format=json"
    
    # for each station ID
    for sID in range (len(stationID)):
        startYear = stationYR[sID]
    
        # set the filename
        fileName = stationNM[sID] + "-MSL.csv"
        
        print("Starting %s" % stationNM[sID])
        
        outFile = open(fileName, "w")
        outFile.write("Year, Average MSL(m) \n")        

        # Loop throught the years of data, ending in 2022
        for i in range(startYear, 2023):
            
            # Build the url using string formatting 
            urlTides = (requestStringTides % (i, i, stationID[sID]))
            if DEBUG:
                print(urlTides)
            
            # Send the requests
            responseTides = requests.post(urlTides)
            
            # decode the replies
            dataTides = responseTides.json()
            if DEBUG:
                print(dataTides)
            
            numTidesPoints = len(dataTides["data"])
            
            if DEBUG:
                print("Number of Data Points = ", numTidesPoints)            
            
            # Initialize the loop counters
            m = 0
            total = 0
            count = 0
            year = int(dataTides["data"][0]["year"])
            
            for j in range (numTidesPoints):
                if dataTides["data"][j]["MSL"] != "":
                    total = total + float(dataTides["data"][j]["MSL"])
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