# @author: John McManus
# © 2023. This work is licensed under a CC BY-NC-SA 4.0 license
# MultiStation Tide Data analysis
# Program to query the NOAA Data source to get monthly mean tidal datums
# "https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations/%d/datums.json?units=metric&format=json"

# Import the requests library 
import requests


DEBUG = False

def main():
    # NOAA Station ID numbers
    stationID = [8418150, 8443970, 8510560, 8518750, 8531680, 8638610, 8658120, 8665530, 8720030, 8724580, 8452660, 8534720]

    # NOAA Station ID numbers    
    stationNM = ["Portland", "Boston", "Montauk", "The Battery", "Sandy Hook", "Sewells Pt", "Wilmington NC", 
                 "Charleston SC", "Fernandina Beach", "Key West", "Newport", "Atlantic City"]
    
    # initialize the request string with format specifiers to accept
    # the start date, the end date, and the station ID
    requestStringTides = "https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations/%d/datums.json?units=metric&format=json"
    
    # for each station ID
    for sID in range (len(stationID)):
    
        # set the filename using the station name
        fileName = stationNM[sID] + "-Base-MSL.csv"
        
        print("Starting %s" % stationNM[sID])
        
        outFile = open(fileName, "w")
        outFile.write("Station, Epoch, Datum, Base MSL(m) \n")        

        # Build the webservice request url using string formatting 
        urlTides = (requestStringTides % (stationID[sID]))
        
        # Send the get request
        responseTides = requests.get(urlTides)
        
        if DEBUG:
            print(urlTides)
            print(responseTides)

        # decode the JSON replies
        dataTides = responseTides.json()
        if DEBUG:
            print(dataTides)
            
        print(stationNM[sID], dataTides["epoch"], dataTides["datums"][5]["name"], dataTides["datums"][5]["value"])

        # Write the data to the output file 
        outFile.write("%s, %s, %s, %s \n" % (stationNM[sID], dataTides["epoch"], dataTides["datums"][5]["name"], dataTides["datums"][5]["value"]))
            
        print("%s is complete" % fileName)
        outFile.close()
    
# Call the main function
main()