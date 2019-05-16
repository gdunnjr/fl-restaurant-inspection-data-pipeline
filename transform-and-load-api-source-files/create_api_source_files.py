import csv
import geocode
import os

# global variable to use for unique id in output file
g_counter = 0
g_api_key = os.environ['G_API_KEY']
inputFile = '../export-from-athena/pass_and_failures_last_30_export.csv'
detailOutputFile = 'restaurant_inspections_detail_stage.csv'
summaryOutputFile = 'restaurant_inspections_summary_stage.csv'

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
def processInspection(inspRow, sum_writer, det_writer):
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
    det_writer.writerow([g_counter, inspRow['BusinessName'], inspRow['InspectionDate'], inspRow['InspectionDisposition'], inspRow['LocationAddress'], \
        inspRow['LocationCity'], inspRow['LocationZip'], inspRow['CountyName'], \
        lat, lng, inspRow['LicenseID'], inspRow['InspectionVisitID'], inspRow['LicenseNo'], inspRow['NumTotalViolations'], \
        inspRow['NumHighViolations'],inspRow['NumIntermediateViolations'], inspRow['NumBasicViolations'], \
        inspRow['COLW'],inspRow['COLX'],inspRow['COLY'],inspRow['COLZ'],inspRow['COLAA'],inspRow['COLAB'],inspRow['COLAC'],inspRow['COLAD'], \
        inspRow['COLAE'],inspRow['COLAF'],inspRow['COLAG'],inspRow['COLAH'],inspRow['COLAI'],inspRow['COLAJ'],inspRow['COLAK'],inspRow['COLAL'], \
        inspRow['COLAM'],inspRow['COLAN'],inspRow['COLAO'],inspRow['COLAP'],inspRow['COLAQ'],inspRow['COLAR'],inspRow['COLAS'],inspRow['COLAT'], \
        inspRow['COLAU'],inspRow['COLAV'],inspRow['COLAW'],inspRow['COLAX'],inspRow['COLAY'],inspRow['COLAZ'],inspRow['COLBA'],inspRow['COLBB'], \
        inspRow['COLBC'],inspRow['COLBD'],inspRow['COLBE'],inspRow['COLBF'],inspRow['COLBG'],inspRow['COLBH'],inspRow['COLBI'],inspRow['COLBJ'], \
        inspRow['COLBK'],inspRow['COLBL'],inspRow['COLBM'],inspRow['COLBN'],inspRow['COLBO'],inspRow['COLBP'],inspRow['COLBQ'],inspRow['COLBR'], \
        inspRow['COLBS'],inspRow['COLBT'],inspRow['COLBU'],inspRow['COLBV'],inspRow['COLBW'],inspRow['COLBX'],inspRow['COLBY'],inspRow['COLBZ'], \
        inspRow['COLCA'],inspRow['COLCB']])
    g_counter = g_counter + 1
    return

# Open summary file and write header
sum_file = open(summaryOutputFile,mode='w')
sum_writer = csv.writer(sum_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
sum_writer.writerow(['Id', 'Name', 'Date', 'Violation', 'Address', 'City', 'Zip' , 'CountyName', 'Lat','Lng','LicenseId', \
'InspectionVisitID','LicenseNo','NumTotalViolations','NumHighViolations','NumIntermediateViolations','NumBasicViolations'])

# Open summary file and write header 
det_file = open(detailOutputFile,mode='w')
det_writer = csv.writer(det_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
det_writer.writerow(['Id', 'Name', 'Date', 'Violation', 'Address', 'City', 'Zip' , 'CountyName', 'Lat','Lng','LicenseId', \
'InspectionVisitID','LicenseNo','NumTotalViolations','NumHighViolations','NumIntermediateViolations','NumBasicViolations', \
'COLW','COLX','COLY','COLZ','COLAA','COLAB','COLAC','COLAD', \
'COLAE','COLAF','COLAG','COLAH','COLAI','COLAJ','COLAK','COLAL', \
'COLAM','COLAN','COLAO','COLAP','COLAQ','COLAR','COLAS','COLAT', \
'COLAU','COLAV','COLAW','COLAX','COLAY','COLAZ','COLBA','COLBB', \
'COLBC','COLBD','COLBE','COLBF','COLBG','COLBH','COLBI','COLBJ', \
'COLBK','COLBL','COLBM','COLBN','COLBO','COLBP','COLBQ','COLBR', \
'COLBS','COLBT','COLBU','COLBV','COLBW','COLBX','COLBY','COLBZ', \
'COLCA','COLCB'])
 
# Open input file and process each row
with open(inputFile, mode='r') as csv_file:
    csv_reader = csv.DictReader(csv_file)
    for row in csv_reader:
        processInspection(row, sum_writer, det_writer)
       
