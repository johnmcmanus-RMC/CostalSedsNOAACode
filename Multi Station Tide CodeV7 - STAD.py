# @Author: John McManus
# © 2023. This work is licensed under a CC BY-NC-SA 4.0 license
# MultiStation Tide Data analysis
# Program to query the NOAA Data source to get Station Datum (STAD) tidal data
#
import requests
import time

DEBUG = False

def main():
    
    # Lists with the NOAA station IDs, start year for the data for each station, and station names
    # Potention refactoring: build a station class and read the data from a text or .CSV file
    stationID = [8418150, 8443970, 8510560, 8518750, 8531680, 8638610, 8658120, 8665530, 8720030, 8724580, 8452660]
    stationYR = [1912, 1921, 1947, 1926, 1932, 1927, 1935, 1922, 1938, 1926, 1938]
    stationNM = ["Portland", "Boston", "Montauk", "The Battery", "Sandy Hook", "Sewells Pt", "Wilmington NC", 
                 "Charleston SC", "Fernandina Beach", "Key West", "Newport"]
    
    # initialize the request strings with format specifiers to accept
    # the start date, the end date, and the station ID
    # requestStringTides is for predicted data
    # requestStringWater is for actual data
    
    requestStringTides ="https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?product=predictions&application=RMC.RESEARCH.APP&begin_date=%d0101&end_date=%d1231&datum=STND&station=%d&time_zone=GMT&units=metric&interval=h&format=json"
    
    requestStringWater = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?product=hourly_height&application=RMC.RESEARCH.APP&begin_date=%d0101&end_date=%d1231&datum=STND&station=%d&time_zone=GMT&units=metric&format=json"
    
    # Loop through the station IDs
    for sID in range (len(stationID)):
        startYear = stationYR[sID]
    
        # set the filenames
        fileName = stationNM[sID] + ".csv"
        fileName2 = stationNM[sID] + "-missing.csv"
        fileName3 = stationNM[sID] + "-missing.txt"
        
        # Open the output files
        outFile = open(fileName, "w")
        txtFile = open(fileName2, "w")
        missing = open(fileName3, "w")
        
        txtFile.write("Year, # of Verified points, # of Predicted points, # of Missing points\n")
        
        # Loop throught the years of data
        for i in range(startYear, 2023):
            
            # Build the url using string formatting 
            urlTides = (requestStringTides % (i, i, stationID[sID]))
            urlWater = (requestStringWater % (i, i, stationID[sID]))
            if DEBUG:
                print(urlTides)
                print(urlWater)
            
            # Post the requests to the two web services
            responseTides = requests.post(urlTides)
            responseWater = requests.post(urlWater)
            
            # decode the replies
            dataTides = responseTides.json()
            dataWater = responseWater.json()
            
            numWaterPoints = len(dataWater["data"])
            numTidesPoints = len(dataTides["predictions"])

            if numWaterPoints != numTidesPoints:
                print("Oh my, mismatched data year: %d Water: %d Tides: %d" % (i, numWaterPoints, numTidesPoints))
            
            if DEBUG:
                print("numWaterPoints = ", numWaterPoints)
                print("numTidesrPoints = ", numTidesPoints)            
            
            # Write the file header if this is the first year of data
            if i == startYear:
                outFile.write("id: %s, name: %s, lat: %s, lon: %s \n" % 
                        (dataWater["metadata"]["id"], dataWater["metadata"]["name"], dataWater["metadata"]["lat"], dataWater["metadata"]["lon"]))
                outFile.write("Date, Prediction (m), Measured(m) , Surge\n")
                
            # Initialize the loop counters
            m = 0
            n = 0
            countMissing = 0
            
            # While there is data to be process in either list
            while m < numWaterPoints and n < numTidesPoints:
                
                # if no V data is in the water dataset, set the value to "None"
                if dataWater["data"][m]["v"] == "":
                    dataWater["data"][m]["v"] = None
                    
                    # Increment the missing data counter and write the time to the file
                    countMissing = countMissing + 1
                    missing.write("%s, \n" % (dataTides["predictions"][m]["t"]))
    
                # if no V data is in the tides dataset, set the value to "None"
                if dataTides["predictions"][n]["v"] == "":
                    dataTides["predictions"][n]["v"] = None
                    
                    # Increment the missing data counter and write the time to the file                    
                    countMissing = countMissing + 1
                    missing.write("%s, \n" % (dataTides["predictions"][n]["t"]))
                
                # If the actual data time is > the predicted data time
                if dataWater["data"][m]["t"] > dataTides["predictions"][n]["t"]:
                    outFile.write("%s, %s, %s, %s\n" % (dataTides["predictions"][n]["t"], dataTides["predictions"][n]["v"], "None", ""))
                    missing.write("%s, \n" % (dataTides["predictions"][n]["t"]))
                    n = n + 1
                    
                # If the actual data time is > the predicted data time                
                elif dataWater["data"][m]["t"] < dataTides["predictions"][n]["t"]:
                    outFile.write("%s, %s, %s, %s\n" % (dataWater["data"][m]["t"], "None", dataWater["data"][m]["v"], ""))
                    missing.write("%s, \n" % (dataTides["predictions"][m]["t"]))
                    m = m + 1
                else:
                    if (dataWater["data"][m]["v"] != None) and (dataTides["predictions"][n]["v"] != None):
                        result = float(dataWater["data"][m]["v"]) - float(dataTides["predictions"][n]["v"])
                        if result <= 0:
                            #result = ""
                            result = str(result)
                        else:
                            result = str(result)
                    else:
                        result = ""
                        
                    outFile.write("%s, %s, %s, %s\n" % (dataWater["data"][m]["t"], dataTides["predictions"][n]["v"], dataWater["data"][m]["v"], result))
                    m = m + 1
                    n = n + 1
            
            # While there is data in the dataWater (verified data) list
            while m < numWaterPoints:
                outFile.write("%s, %s, %s, %s\n" % (dataWater["data"][m]["t"], None, dataWater["data"][m]["v"], ""))
                missing.write("%s, \n" % (dataTides["predictions"][m]["t"]))
                m = m + 1

            # While there is data in the dataTides (predicted data) list
            while n < numTidesPoints:
                outFile.write("%s, %s, %s, %s\n" % (dataTides["predictions"][n]["t"], dataTides["predictions"][n]["v"], None, ""))
                missing.write("%s, \n" % (dataTides["predictions"][n]["t"]))
                n = n + 1
                
            txtFile.write("%s, %d, %d, %d\n" % (i, numWaterPoints, numTidesPoints, countMissing))
            
        print("File %s is ready" % fileName)
        # Close the output files
        outFile.close()
        txtFile.close()
        missing.close()
    
# Call the main function
main()