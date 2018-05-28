import argparse
import configparser
import requests
import pyprind
import pymysql.cursors

def get_server_connection(serverUrl, username, password):
    server = requests.Session()
    server.auth = (username, password)
    adapter = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100)
    server.mount(serverUrl, adapter)
    return server

def get_content(server, datanURL, dataIndex):
    response = server.get(datanURL).json()
    return {org['id']:org['displayName'] for org in response[dataIndex]}

def get_metadata(server, metadataURL, elementURL, dataset):
	meta = server.get(metadataURL.format(dataset)).json()
	
	#Get datasetname, organisation_units, data_elements, category_options
	dataset_name = meta['dataSets'][0]['name']
	organisation_units = [dhiscode for facility in meta['dataSets'][0]['organisationUnits'] for dhiscode in facility.values()]
	data_elements = [element.get('id') for element in meta.get('dataElements')]
	category_options = [options.get('id') for options in meta.get('categoryOptions')]

	#Remove elements that have date values (don't allow aggregation)
	[data_elements.remove(element) for element in data_elements if server.get(elementURL.format(element)).json().get('valueType') == 'DATE']

	return organisation_units, data_elements, category_options, dataset_name

def get_data_urls(organisation_units, data_elements, category_options, dataset_name, analyticsURL, period):
    _orgchunk = 100
    _orgchunks = [organisation_units[i:i + _orgchunk] for i in range(0, len(organisation_units), _orgchunk)]
    urls = [analyticsURL.format(period, ';'.join(chunk), ';'.join(data_elements), ';'.join(category_options)) for chunk in _orgchunks]
    return urls

def get_data(server, dataUrls, category):
	data = []
	percentage = pyprind.ProgPercent(len(dataUrls)) #Progress bar
	for dataUrl in dataUrls:
		response = server.get(dataUrl).json()
		rows = response['rows']
		if not rows:
			continue  #if no rows, go to next URL

		metanames = response['metaData']['items'] #dictionary of codes
		hierarchy = response['metaData']['ouHierarchy'] #dictionary of orgs

		#Create a reference for county, subcounty & facility names
		cleanhierarchy = {}
		for facility in hierarchy:
			#facility would have 4 parents - skip non facilities
			if len(hierarchy[facility].split('/')) != 4:
				continue

			countrycode, countycode, subcountycode, wardcode = hierarchy[facility].split('/')

			facilityname = metanames[facility]['name']
			county = metanames[countycode]['name']
			subcounty = metanames[subcountycode]['name']
			#Clean dictionary hierarchy of orgs
			cleanhierarchy[facility] = [county, subcounty, facilityname]

		#Process the rows returned (dx = data_element, co = category, ou = organisation_unit, qty = quantity)
		for row in rows:
			dx, co, pe, ou, qty = row
			dxname = metanames[dx]['name']
			coname = metanames[co]['name']

			#skip rows for non-facility (check above)
			if ou not in cleanhierarchy:
				continue

			county, subcounty, facilityname = cleanhierarchy[ou]
			try:
				qty = float(qty)
			except (ValueError, TypeError):
				pass
			#(Dimension = Drug/Regimen and Category includes Beginning Balance)
			data.append([ou, pe, dx, category[co], qty])
			

		 #Update progress bar
		percentage.update()
	return data

def get_db_connection(cfg):
	dbcfg = {
		'user': str(cfg["username"]),
		'password': str(cfg["password"]),
		'host': str(cfg["hostname"]),
		'port': int(cfg["port"]),
		'database': str(cfg["dbname"]),
		'charset': 'utf8mb4',
		'cursorclass': pymysql.cursors.Cursor
	}
	return pymysql.connect(**dbcfg)


def process_data(dbconn, data, dataset):
	#Bulk insert data into tbl_order
	cursor = dbconn.cursor()
	cursor.execute('TRUNCATE tbl_order')
	cursor.executemany('REPLACE INTO tbl_order (facility, period, dimension, category, value) VALUES (%s, %s, %s, %s, %s)', data)
	dbconn.commit() 
	#Run cleanup stored procedures
	cursor.callproc('proc_save_'+dataset)
	dbconn.commit() 
	cursor.callproc('proc_save_'+dataset+'_item')
	dbconn.commit() 
	#Close cursor and connection
	cursor.close()
	dbconn.close()

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='DHIS Reader')
	parser.add_argument('-c','--content', help='Content to fetch', required=True, choices=['county', 'subcounty', 'datasets', 'metadata'])
	parser.add_argument('-ds','--dataset', help='Dataset to fetch', default='cdrr', choices=['maps', 'cdrr'])
	parser.add_argument('-p','--period', help='Period to fetch', default='LAST_MONTH', choices=['THIS_MONTH', 'LAST_MONTH', 'LAST_3_MONTHS', 'LAST_6_MONTHS', 'LAST_12_MONTHS'])
	args = vars(parser.parse_args())

	#Get configuration
	cfg = configparser.ConfigParser()
	cfg.read('config/properties.ini')
	dhis_url = cfg['dhis']['url']
	dhis_username = cfg['dhis']['username']
	dhis_password = cfg['dhis']['password']

	#Get parameters
	content = args['content']

	#Get server connection
	serverObj = get_server_connection(dhis_url, dhis_username, dhis_password)

	#Get content
	if(content == 'metadata'):
		dataset = args['dataset']
		organisation_units, data_elements, category_options, dataset_name = get_metadata(serverObj, cfg['urls'][content], cfg['urls']['element'], cfg['datasets'][dataset])
		dataUrls = get_data_urls(organisation_units, data_elements, category_options, dataset_name, cfg['urls']['analytics'], args['period'])
		data = get_data(serverObj, dataUrls, cfg['category'])
		dbconn = get_db_connection(cfg['database'])
		process_data(dbconn, data, dataset)
	else:
		print get_content(serverObj, cfg['urls'][content], cfg['indices'][content])
