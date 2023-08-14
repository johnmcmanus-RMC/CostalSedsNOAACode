# @Author: John McManus
# © 2023. This work is licensed under a CC BY-NC-SA 4.0 license
# MultiStation Tide Data analysis
# Program to query the NOAA Data source to get Station Datum (STAD) tidal data
# This program is specific to the Atlantic City station, from 1971 - 2022
# The Atlantic City station is missing data in 1970

# import the request module and the time module
import requests
import time

# Constant to switch debugging print statements on/off
DEBUG = False

def main():
    
    # Lists with the NOAA station ID, start year for the data, and station name
    stationID = [8534720]
    stationYR = [1922]
    stationNM = ["Atlantic City"]
    
    # initialize the request string with format specifiers to accept
    # the start date, the end date, and the station ID
    requestStringTides ="https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?product=predictions&application=RMC.RESEARCH.APP&begin_date=%d0101&end_date=%d1231&datum=STND&station=%d&time_zone=GMT&units=metric&interval=h&format=json"
    
    requestStringWater = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?product=hourly_height&application=RMC.RESEARCH.APP&begin_date=%d0101&end_date=%d1231&datum=STND&station=%d&time_zone=GMT&units=metric&format=json"
    
    for sID in range (len(stationID)):
        startYear = stationYR[sID]
    
        # set the filename
        fileName = stationNM[sID] + "-A.csv"
        fileName2 = stationNM[sID] + "-missing-A.csv"
        fileName3 = stationNM[sID] + "-missing-A.txt"
        
        
        outFile = open(fileName, "w")
        txtFile = open(fileName2, "w")
        missing = open(fileName3, "w")
        
        txtFile.write("Year, # of Verified points, # of Predicted points, # of Missing points\n")
        
        # Loop throught the years of data
        for i in range(startYear, 1970):
            
            # Build the url using string formatting 
            urlTides = (requestStringTides % (i, i, stationID[sID]))
            urlWater = (requestStringWater % (i, i, stationID[sID]))
            if DEBUG:
                print(urlTides)
                print(urlWater)
            
            # Send the requests
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
            
            if i == startYear:
                outFile.write("id: %s, name: %s, lat: %s, lon: %s \n" % 
                        (dataWater["metadata"]["id"], dataWater["metadata"]["name"], dataWater["metadata"]["lat"], dataWater["metadata"]["lon"]))
                outFile.write("Date, Prediction (m), Measured(m) , Surge\n")
                
            # Initialize the loop counters
            m = 0
            n = 0
            countMissing = 0
            
            while m < numWaterPoints and n < numTidesPoints:
                # if no V data is in the water dataset, set the value to "None"
                if dataWater["data"][m]["v"] == "":
                    dataWater["data"][m]["v"] = None
                    countMissing = countMissing + 1
                    missing.write("%s, \n" % (dataTides["predictions"][m]["t"]))
    
                # if no V data is in the tides dataset, set the value to "None"
                if dataTides["predictions"][n]["v"] == "":
                    dataTides["predictions"][n]["v"] = None
                    countMissing = countMissing + 1
                    missing.write("%s, \n" % (dataTides["predictions"][n]["t"]))
                
                if dataWater["data"][m]["t"] > dataTides["predictions"][n]["t"]:
                    outFile.write("%s, %s, %s, %s\n" % (dataTides["predictions"][n]["t"], dataTides["predictions"][n]["v"], "None", ""))
                    missing.write("%s, \n" % (dataTides["predictions"][n]["t"]))
                    n = n + 1
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

            while m < numWaterPoints:
                outFile.write("%s, %s, %s, %s\n" % (dataWater["data"][m]["t"], None, dataWater["data"][m]["v"], ""))
                missing.write("%s, \n" % (dataTides["predictions"][m]["t"]))
                m = m + 1

            while n < numTidesPoints:
                outFile.write("%s, %s, %s, %s\n" % (dataTides["predictions"][n]["t"], dataTides["predictions"][n]["v"], None, ""))
                missing.write("%s, \n" % (dataTides["predictions"][n]["t"]))
                n = n + 1
                
            txtFile.write("%s, %d, %d, %d\n" % (i, numWaterPoints, numTidesPoints, countMissing))
            #time.sleep(0.001)
            
        print("File %s is ready" % fileName)
        outFile.close()
        txtFile.close()
        missing.close()
    
# Call the main function
main()