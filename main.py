import argparse
import configparser
import requests
import pyprind
import pymysql.cursors
import warnings

def get_server_connection(serverUrl, username, password):
    server = requests.Session()
    server.auth = (username, password)
    adapter = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100)
    server.mount(serverUrl, adapter)
    return server

def get_content(server, dataURL, dataIndex):
    response = server.get(dataURL).json()
    return {org['id']:org['name'] for org in response[dataIndex]}

def get_org_content(server, dataURL, dataIndex, category):
	orgs = []
	response = server.get(dataURL).json()
	if response and dataIndex in response:
		for org in response[dataIndex]:
			if 'code' in org:
				longitude = ''
				latitude = ''
				if 'coordinates' in org and org['coordinates'] != '#N/A' and org['coordinates'] != '':
					coordinates = org['coordinates'].replace('[', '').replace(']', '').split(',')
					longitude = coordinates[0]
					latitude = coordinates[1]
				orgs.append([org['code'], org['name'], category, org['id'], longitude, latitude])
	return orgs

def get_metadata(server, metadataURL, elementURL, dataset):
	meta = server.get(metadataURL.format(dataset)).json()
	
	#Get datasetname, data_elements, category_options
	dataset_name = meta['dataSets'][0]['name']
	data_elements = [element.get('id') for element in meta.get('dataElements')]
	category_options = [options.get('id') for options in meta.get('categoryOptions')]

	#Remove elements that have date values (don't allow aggregation)
	[data_elements.remove(element) for element in data_elements if server.get(elementURL.format(element)).json().get('valueType') == 'DATE']

	return data_elements, category_options, dataset_name

def get_period_dates(period_str):
	import datetime
	from dateutil.relativedelta import relativedelta

	period_dates = []
	period_list = {'LAST_MONTH': 2, 'LAST_3_MONTHS': 4, 'LAST_6_MONTHS': 7, 'LAST_9_MONTHS': 10, 'LAST_12_MONTHS': 13}
	i = 1
	d = datetime.date.today()

	while i < period_list[period_str]:
		d2 = d - relativedelta(months=i)
		i += 1
		period_dates.append(d2.strftime('%Y%m'))

	return period_dates

def get_period_fulldates(period_str):
	import datetime
	from dateutil.relativedelta import relativedelta

	period_dates = []
	period_list = {'LAST_MONTH': 2, 'LAST_3_MONTHS': 4, 'LAST_6_MONTHS': 7, 'LAST_9_MONTHS': 10, 'LAST_12_MONTHS': 13}
	i = 1
	d = datetime.date.today()

	while i < period_list[period_str]:
		d2 = d - relativedelta(months=i)
		i += 1
		period_dates.append(d2.strftime('%Y-%m-01'))

	return period_dates

def get_data_urls(organisation_units, data_elements, category_options, dataset_name, datavaluesURL, period_str):
	urls = []
	_orgchunk = 100
	_orgchunks = [organisation_units[i:i + _orgchunk] for i in range(0, len(organisation_units), _orgchunk)]
	for period_date in get_period_dates(period_str):
		if dataset_name in ['rk2yudsNrm5', 'ZddkIXm6FDw']:
			urls.append([datavaluesURL.format(dataset_name, period_date, '&orgUnitGroup='.join(chunk)) for chunk in _orgchunks])
		else:
			urls.append([datavaluesURL.format(dataset_name, period_date, '&orgUnit='.join(chunk)) for chunk in _orgchunks])
	return urls

def get_data(server, dataUrls, category, dataset, dbconn):
	data = []
	category_keys = [str(a) for a in category.keys()]
	percentage = pyprind.ProgPercent(len(dataUrls)) #Progress bar
	for dataUrl in dataUrls:
		response = server.get(dataUrl).json()
		if 'dataValues' in response.keys():
			values = response['dataValues']
			for value in values:
				categoryOptionCombo = value['categoryOptionCombo'].lower()
				if 'value' in value.keys() and categoryOptionCombo in category_keys:
					data.append([value['orgUnit'], value['period'], value['dataElement'], category[categoryOptionCombo], value['value']])
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
		'cursorclass': pymysql.cursors.DictCursor
	}
	return pymysql.connect(**dbcfg)

def process_data(dbconn, data, dataset):
	table = cfg['tables'][dataset]
	try:
		cursor = dbconn.cursor()
		if len(data) != 0:
			#Bulk insert data into tbl_order
			cursor.execute('TRUNCATE tbl_order')
			cursor.executemany('REPLACE INTO tbl_order (facility, period, dimension, category, value) VALUES (%s, %s, %s, %s, %s)', data)
			dbconn.commit() 
			#Run cleanup stored procedures
			cursor.callproc('proc_save_'+table, [dataset])
			dbconn.commit() 
			cursor.callproc('proc_save_'+table+'_item')
			dbconn.commit()
	except Exception, e:
		print e

	#Close cursor and connection
	cursor.close()

def save_orgs(dbconn, data, parent_mfl = None):
	cursor = dbconn.cursor()
	for org in data:
		if parent_mfl == None:
			org.append(org[0])
		else:
			org.append(parent_mfl)
			if org[0] == parent_mfl:
				org[2] = 'central'
		#Run proc	
		try:
			cursor.callproc('proc_save_facility_dhis', org)
			dbconn.commit() 
		except Exception, e:
			print org
			print e
	#Close cursor
	cursor.close()

def save_dsh_data(dbconn, full_date):
	try:
		with warnings.catch_warnings():
			warnings.simplefilter("ignore")
			cursor = dbconn.cursor()
			cursor.callproc('proc_save_dsh_tables', [full_date])
			dbconn.commit() 
	except Exception, e:
		print e

	#Close cursor and connection
	cursor.close()

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='DHIS Reader')
	parser.add_argument('-c','--content', help='Content to fetch', required=True, choices=['county', 'subcounty', 'datasets', 'metadata', 'central', 'standalone'])
	parser.add_argument('-ds','--dataset', help='Dataset to fetch', default='D-CDRR', choices=['D-CDRR', 'F-CDRR', 'D-MAPS', 'F-MAPS'])
	parser.add_argument('-p','--period', help='Period to fetch', default='LAST_MONTH', choices=['LAST_MONTH', 'LAST_3_MONTHS', 'LAST_6_MONTHS', 'LAST_9_MONTHS', 'LAST_12_MONTHS'])
	parser.add_argument('-dsb','--dashboard', help='Push data to dashboard tables', default='No', choices=['Yes', 'No'])
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

	#Get database connection
	dbconn = get_db_connection(cfg['database'])

	#Get content
	if(content == 'metadata'):
		dataset = args['dataset']
		#Get ordering site org_units
		if dataset in ['D-CDRR', 'D-MAPS']:
			organisation_units = [dhiscode for dhiscode in get_content(serverObj, cfg['urls']['central_grp'], cfg['indices']['central_grp'])]
			data_elements, category_options, dataset_name = get_metadata(serverObj, cfg['urls'][content], cfg['urls']['element'], cfg['datasets'][dataset])
			dataUrls = get_data_urls(organisation_units, data_elements, category_options, cfg['datasets'][dataset], cfg['urls']['datavaluesgrp'], args['period'])
		else:
			organisation_units = [dhiscode for dhiscode in get_content(serverObj, cfg['urls']['facility'], cfg['indices']['facility'])]
			data_elements, category_options, dataset_name = get_metadata(serverObj, cfg['urls'][content], cfg['urls']['element'], cfg['datasets'][dataset])
			dataUrls = get_data_urls(organisation_units, data_elements, category_options, cfg['datasets'][dataset], cfg['urls']['datavalues'], args['period'])
		#Get and save data
		for dataUrl in dataUrls:
			data = get_data(serverObj, dataUrl, cfg['category'], dataset, dbconn)
			process_data(dbconn, data, dataset)
		#Run when final D-MAPS runs
		for full_date in get_period_fulldates(args['period']):
			if args['dashboard'] == 'Yes':
				save_dsh_data(dbconn, full_date)
	else:
		if content in ['county', 'subcounty', 'datasets']:
			print get_content(serverObj, cfg['urls'][content], cfg['indices'][content])
		elif content in ['central']:
			#Get central sites
			central_sites = get_org_content(serverObj, cfg['urls'][content], cfg['indices'][content], content)
			save_orgs(dbconn, central_sites)
			central_ids = [site[3] for site in central_sites]
			#Get central site groups
			response = serverObj.get(cfg['urls']['central_grp']).json()
			central_grps = response[cfg['indices']['central_grp']]
			percentage = pyprind.ProgPercent(len(central_grps)) #Progress bar
			for central_grp in central_grps:
				parent_mfl = None
				#Get satellites of central site groups
				satelliteURL = cfg['urls']['satellite'].format(central_grp['id'])
				satellites = get_org_content(serverObj, satelliteURL, cfg['indices']['satellite'], 'satellite')
				for satellite in satellites:
					if satellite[3] in central_ids:
						parent_mfl = satellite[0]
				save_orgs(dbconn, satellites, parent_mfl)
				#Update progress bar
				percentage.update()
		else: 
			orgs = get_org_content(serverObj, cfg['urls'][content], cfg['indices'][content], content)
			save_orgs(dbconn, orgs)