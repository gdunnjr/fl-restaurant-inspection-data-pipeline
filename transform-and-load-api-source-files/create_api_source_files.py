import csv
import geocode
import os

# global variable to use for unique id in output file
g_counter = 0
g_api_key = os.environ['G_API_KEY']

# Accept an address and use google goecode api to get geocode
def getGeoCode(businessAddress):
	
	address = businessAddress.replace('#','').replace('&','').replace('?','')
	geocode_result = geocode.get_google_results(address, g_api_key, return_full_response=False)
            
	# If we're over the API limit, backoff for a while and try again later.
	if geocode_result['status'] == 'OVER_QUERY_LIMIT':
		print("Hit Query Limit! Sleeping for 30 minutes.")
		time.sleep(1800) # sleep for 30 minutes
		geocoded = False
	else:
		if geocode_result['status'] != 'OK':
			print("Error geocoding {}: {}".format(address, geocode_result['status']))
		print("Geocoded: {}: {}".format(address, geocode_result['status']))
            
	return geocode_result       

# Process a row from the input file - get geocode and write out desired columns                
def processInspection(inspRow,ff_writer):
    global g_counter

    businessAddress = inspRow['BusinessName']+ ' ' + inspRow['LocationAddress'] + ' ' +	inspRow['LocationCity'] + ' ' +	inspRow['LocationZip']            
    geo_results = getGeoCode(businessAddress)
    lat = geo_results['latitude']
    lng = geo_results['longitude']
    sum_writer.writerow([g_counter, inspRow['BusinessName'], inspRow['InspectionDate'], inspRow['InspectionDisposition'], inspRow['LocationAddress'], \
        inspRow['LocationCity'], inspRow['LocationZip'], inspRow['CountyName'], \
        lat, lng, inspRow['LicenseID'], inspRow['InspectionVisitID'], inspRow['LicenseNo'], inspRow['NumTotalViolations'], \
         inspRow['NumHighViolations'],inspRow['NumIntermediateViolations'], inspRow['NumBasicViolations'] \
        ])
    g_counter = g_counter + 1
    return

# Open the files
sum_file = open('failed_first_inspection_stage_v2.csv',mode='w')
sum_writer = csv.writer(sum_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
sum_writer.writerow(['Id', 'Name', 'Date', 'Violation', 'Address', 'City', 'Zip' , 'CountyName', 'Lat','Lng','LicenseId', \
'InspectionVisitID','LicenseNo','NumTotalViolations','NumHighViolations','NumIntermediateViolations','NumBasicViolations'])
 
# Open input file and process each row
with open('../export-from-athena/pass_and_failures_last_30_export.csv', mode='r') as csv_file:
    csv_reader = csv.DictReader(csv_file)
    for row in csv_reader:
        processInspection(row, sum_writer)
       
