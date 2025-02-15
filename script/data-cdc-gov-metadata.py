import contextlib
import csv
from datetime import datetime
import io
import logging
from pathlib import Path
import re
import requests
from requests.exceptions import RequestException
import retriever as rt
import sys
import time
from urllib.parse import urlparse

# This script takes a list of homepages scraped from data.cdc.gov sitemaps and a list of downloaded datasets obtained from those homepages
# and builds spreadsheet-friendly documentation for those datasets.
#
# The outputs for the documentation are: the homepage_urls; the socrata_ids, obtained from the homepage_urls; the associated socrata metadata,
# including the dataset name and description; the downloaded dataset filename; the timestamp for when the dataset was downloaded; and a list
# of snapshot urls and their creation times for the homepages in the Internet Archive. 

#sitemap_data_fname = 'test_sitemap.csv'
sitemap_data_fname = 'cdc_data_sitemap_data.csv'
downloads_fname = 'cdc_data_downloads.csv'
log_fname = "dataset_doc.log"

header_text = '''This is supplementary information for a series of data.cdc.gov dataset snapshots taken in January 2025.
Methodology notes:        
In January 2025, the sitemaps.xml files for data.cdc.gov were used to come up with a list of dataset homepages served by that host.
From there, the datasets themselves were downloaded (download_filename, downloaded_ts). The socrata id was inferred from the dataset homepage's URL, and the downloaded filename became: socrata-id_download-timestamp-as-unix-epoch_name-downloaded-file-name.
To construct the rest of this documentation, socrata metadata was queried for the id. If found, the socrata name metadata became the dataset_name column, and the socrata_description became the description column.
All the socrata metadata is a single string in the additional metadata column. The socrata metadata was queried on Feb. 13. 2025. If the socrata metadata query was unsuccessful, the "UNKN_SOC_NAME" name was used.
The dataset homepages include metadata about the datasets themselves and change over time. The Internet Archive has snapshots of these homepages, and links to the snpshots are in the Internet Archive snapshots column.
The data are ordered by dataset name and may contain more than one dataset endpoint, identified by socrata id.

The dataset snapshots are available at: https://cdcdotgovarchive.org/CDC_datasets/.
The script that built this is here: https://github.com/YakShavingAsAService/data.cdc.gov-metadata '''

header_lines = header_text.splitlines()

# logging setup
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler = logging.FileHandler(log_fname, mode='w')
file_handler.setFormatter(formatter)
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.addHandler(stream_handler)

#logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', filename = log_fname, filemode = 'w')

logging.info("documenting datasets downloaded in %s" % downloads_fname)
logging.info("dataset homepages are in %s" % sitemap_data_fname)

def build_download_file_dict():
	"""
	this takes a file containing a list of downloaded filenames of format: SSSS-SSSS_TS.TS_XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
	and turns it into a dictionary entry with the socrata id (SSSS-SSSS) as the key and the value as a list of inner dictionaries that looks like:
	[ { 'filename': XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX, 'downloaded_ts': datetime_str(TS.TS) } ]
	for example, given the input: yt7u-eiyg_1736710755.963349_NCHS_-_Birth_Rates_for_Females_by_Age_Group__United_States_20250112.csv.gz
	this code adds the following key-value pair to results_dir:
	{ 'yt7u-eiyg' : [ { 'download_filename': NCHS_-_Birth_Rates_for_Females_by_Age_Group__United_States_20250112.csv.gz, 'download_ts': '2025-01-12 19:39:15 UTC' } } ]
	note: if there are multiple downloaded files with the same socrata_id -- that is, if it was downloaded multiple times -- then
	the dictionary value can have more than one inner dictionary in it.
	"""

	results_dict = {}
	pattern = r"^(?P<socrata_id>[a-zA-Z0-9]+-[a-zA-Z0-9]+)_(?P<timestamp>\d+\.\d+)_"
	with open(downloads_fname,'r') as downloadsf:
		while True:
			line = downloadsf.readline()
			line = line.rstrip()
			if not line: break

			match = re.search(pattern, line)
			if not match:
				logging.error("this downloaded file name %s cannot be mapped to a socrata_id?!? skipping it" % (line))
				pass
			else:
				socrata_id = match.group('socrata_id')
				ts = match.group('timestamp')
				# take this UNIX timestamp with fractional seconds and turn it
				# into a reasonable-looking UTC string
				ts_float = float(ts)
				utc_time = datetime.utcfromtimestamp(ts_float)
				download_ts = utc_time.strftime("%Y-%m-%d %H:%M:%S UTC")
				if socrata_id in results_dict.keys():
					logging.info("this socrata id %s appears more than once in %s?!?" % (socrata_id,downloads_fname))
					results_dict[ socrata_id ].append( { 'download_filename': line, 'download_ts': download_ts } )
				else:
					results_dict[ socrata_id ] = [ { 'download_filename': line, 'download_ts': download_ts } ]
	return results_dict

def get_next_homepage_url_line( inputf ):
	""""
	gets the next line in the sitemaps input file and parses it into a dict. 
	expected format: sitemaps_url_where_the_homepage_url_was_found,homepage_url
	the last component in a homepage_urls should be a socrata id, which looks like: xxxx-xxxx

	this function returns a dictionary that looks like:
	{ 'socrata_id': socrata_id, 'homepage_url': homepage_url }
	it is possible a socrata_id was not found, in whch case the dict looks
	like: { 'socrata_id':None, 'homepage_url': homepage_url }
	when the end of file is reached, returns None	
	"""
	line = inputf.readline()
	line = line.rstrip()
	if not line: return None

	(sitemap_url,homepage_url) = line.split(',')
	parsed_url = urlparse(homepage_url)
	path = parsed_url.path
	socrata_id = Path(path).name

	if not is_socrata_id(socrata_id):
		logging.error("could not find a socrata id in %s ???" % (homepage_url))
		return { 'socrata_id': None, 'homepage_url': homepage_url }

	return { 'socrata_id': socrata_id, 'homepage_url': homepage_url }

def get_internet_archive_snapshots( homepage_url ):
	"""
	return a list of saved snapshots of the homepage url in the internet archive.
	"""
	memento_url = "http://web.archive.org/web/timemap/link/%s" % homepage_url

	try:
		response = requests.get( memento_url )
		if response.status_code == 200:
			return response.text
		else:
			logging.error("could not lookup IA snapshots using this url %s: %s" % (memento_url,str(response)))
	except Exception as e:
		print(f"error %s occurred using this url: %s " % (e,memento_url))

	return ""

def get_socrata_data( socrata_id ):
	"""
	look up this socrata id and retieve the associated socrata metadata.
	if no name found in the metadata, use "UNKN_SOC_NAME" instead.
	if we didn't get any metadata back, return any API stdout messages in that field instead.
	returns a dictionary with two keys: 'name' and 'socrata_meta'
	"""
	ret = {}
	ret['soc_name'] = "UNKN_SOC_NAME"
	ret['soc_homepage'] = ""
	ret['soc_description'] = ""
	# find the socrata metadata associated with this socrata id
	stdout_buffer = io.StringIO()
	with contextlib.redirect_stdout(stdout_buffer):
		socrata_meta = rt.find_socrata_dataset_by_id(socrata_id)
	socrata_stdout = stdout_buffer.getvalue()
	if socrata_stdout != "":
		logging.info("socrata call to %s returned this string to stdout: %s " % (socrata_id, socrata_stdout))

	# did we not get socrata metadata back at all? maybe there's some explanatory stdout from the api. capture that.
	if not socrata_meta:
		logging.error("could not find matching socrata metadata for socrata id %s, but got this: %s??" % (socrata_id,socrata_stdout))
		ret['socrata_meta'] = socrata_stdout
	# or maybe we got socrata metadata back, but it came with an error message?
	elif 'error' in socrata_meta.keys():
		logging.error('socrata API call with id %s gave me an error: %s??' % (socrata_id,str(socrata_meta)))
		if 'name' in socrata_meta.keys():
			ret['soc_name'] = socrata_meta['name']
		ret['socrata_meta'] = str(socrata_meta)
	# or maybe we got a normal response that looks something like this?
	# {'name': 'dhds_dataset', 'id': '9vgf-r2z6', 'description': '', 'datatype': 'tabular', 'keywords': ['socrata'], 'domain': 'data.cdc.gov', 'homepage': 'https://data.cdc.gov/dataset/dhds_dataset/9vgf-r2z6'}
	else:
		if 'name' in socrata_meta.keys():
			ret['soc_name'] = socrata_meta['name']
		if 'homepage' in socrata_meta.keys():
			ret['soc_homepage'] = socrata_meta['homepage']
		if 'description' in socrata_meta.keys():
			ret['soc_description'] = socrata_meta['description']
		ret['socrata_meta'] = str(socrata_meta)

	logging.debug("matched socrata id %s to socrata meta data: %s " % (socrata_id,ret['socrata_meta']))
	return ret

def process_leftover_download_files(parsed_download_file_dict,download_files_processed):
	"""
	some datasets in the download file might not have corresponding entry in the datasets homepages in the sitemaps file.
	process these "leftover" files separately
	"""
	ret = []
	for socrata_id in parsed_download_file_dict.keys():
		if socrata_id not in download_files_processed:
			logging.error("download file %s not in dataset home page file. going to process what we can still get now." % socrata_id)
			values = {}
			values['socrata_id'] = socrata_id
			values['homepage'] = ""
			values.update( get_socrata_data( socrata_id ) )
			# we don't have the homepage because there wasn't an entry in the homepages in the sitemaps file.
			# but if we can get one from socrata_meta, use that instead to pull IA snapshots
			values['internet_archive_snapshots'] = ""
			if 'soc_homepage' in values.keys():
				values['internet_archive_snapshots'] = get_internet_archive_snapshots( values['soc_homepage'] )
				values["homepage"] = values["soc_homepage"]
			download_file_dict_list = parsed_download_file_dict[socrata_id]
			for download_file_dict in download_file_dict_list: 
				values['dataset_snapshot_filename'] =  download_file_dict['download_filename']
				values['dataset_snapshot_dl_ts'] =  download_file_dict['download_ts']

			# add the values dictionary to the ret list 
			ret.append( values )
	return ret


def is_socrata_id(input_string):
	"""
	is this string formatted like a socrata id? returns True or False.
	"""

	pattern = r"^[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}$"
    
	if re.match(pattern, input_string):
		return True
	else:
		return False



results_dict = {}
parsed_download_file_dict = build_download_file_dict()
download_files_processed = {} # keep track of what we matched so we can process unmatched ones separately

with open(sitemap_data_fname,'r') as inputf:

	logging.info("getting dataset homepages out of the sitemaps file: %s" % sitemap_data_fname)
	while True:

		parsed_homepage_url = get_next_homepage_url_line( inputf )
		print("looking at homepage %s from the sitemaps file" % parsed_homepage_url)
		logging.debug("looking at homepage %s from the sitemaps file" % parsed_homepage_url)
		if not parsed_homepage_url:
			logging.info("finished looking at dataset homepages from the sitemaps file %s" % sitemap_data_fname)
			break

		homepage_url = parsed_homepage_url['homepage_url']
		socrata_id = parsed_homepage_url['socrata_id']

		# not a dataset home page? just skip.
		if not socrata_id:
			logging.info("could not find a socrata id in %s; assuming this is not a dataset homepage and skipping to the next entry" % parsed_homepage_url)
			continue

		values = {}
		values['socrata_id'] = socrata_id
		values['homepage'] = homepage_url

		# find the IA snapshots of this homepage
		values['internet_archive_snapshots'] = get_internet_archive_snapshots( homepage_url )

		# find the socrata metadata associated with the socrata id
		values.update( get_socrata_data( socrata_id ) )

		# lastly, look for the matching downloaded dataset file
		download_file_dict_list = []
		if not socrata_id in parsed_download_file_dict.keys():
			logging.info("could not find a download file with a matching socrata id %s in %s???" % (socrata_id,downloads_fname))
			values['dataset_snapshot_filename'] =  ""
			values['dataset_snapshot_dl_ts'] =  ""
		else:
			download_file_dict_list = parsed_download_file_dict[ socrata_id ]
			logging.debug("matched homepage %s to download file(s) %s " % (parsed_homepage_url['homepage_url'],download_file_dict_list))
			download_files_processed[ socrata_id ] = True 	# keep track of these so we can process the leftovers later 

			for download_file_dict in download_file_dict_list: 
				values['dataset_snapshot_filename'] =  download_file_dict['download_filename']
				values['dataset_snapshot_dl_ts'] =  download_file_dict['download_ts']

		# add the values dictionary to the results_dict
		if values['soc_name'] in results_dict.keys():
			results_dict[ values['soc_name'] ].append( values )
		else:
			results_dict[ values['soc_name'] ] = [  values ]

		time.sleep(10)

# there might be some files in the download list that weren't in the homepage list in the sitemaps file
# go get the matching data for them and add to the results_dict
leftover_downloads_list = process_leftover_download_files(parsed_download_file_dict,download_files_processed)
for leftover_downloads in leftover_downloads_list:
	if leftover_downloads['soc_name'] in results_dict.keys():
		results_dict[ leftover_downloads['soc_name'] ].append( leftover_downloads )
	else:
		results_dict[ leftover_downloads['soc_name'] ] = [  leftover_downloads ]


# output the results
with open('dataset_documentation.csv', mode='w', newline='') as outputf:
	csv_writer = csv.writer(outputf)
	for hl in header_lines:
		csv_writer.writerow([hl])
	csv_writer.writerow(["dataset name","socrata id","download filename","downloaded ts","dataset homepage","description","Internet Archive snapshots","addtl socrata metadata"])

	# for every dataset name ...
	for name in sorted( results_dict.keys() ):

		# ... ordered by socrata id ...
		sorted_datasets = sorted(results_dict[name], key=lambda x: x['socrata_id'])

		# ... print out a row with the dataset's information
		for ds in sorted_datasets:
			csv_writer.writerow([name,ds['socrata_id'],ds['dataset_snapshot_filename'], \
				ds['dataset_snapshot_dl_ts'], ds['homepage'], ds['soc_description'], ds['internet_archive_snapshots'],
				ds['socrata_meta']])


