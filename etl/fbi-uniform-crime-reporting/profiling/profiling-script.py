import pandas as pd
import os, pickle, subprocess

match_tokens = [' of ' , 'police', 'county', 'sheriff', 'department', 'public', 'safety', 'dept', 'city', 'agency', 'office', 'parish']
mapping = {}

mapping_file_name = 'mapping.pickle'
if not os.path.isfile(mapping_file_name):
    with open('icpsr-county-agency-mapping-data/data.csv','r') as handle:
        # mapping_data as pandas data_frame
        mapping_data = pd.read_csv(handle, usecols=['STATE', 'COUNTY', 'AGENCY'])
        mapping_data.columns = ['state', 'county', 'agency']
        for index, row in mapping_data.iterrows():
            state = row.get('state').upper()
            agency = row.get('agency').lower()
            for token in match_tokens:
                agency = agency.replace(token, '').strip()
            county = row.get('county').lower()
            key = state + '-' + agency
            mapping[key] = county
    with open(mapping_file_name, 'wb') as handle:
        pickle.dump(mapping, handle, protocol=pickle.HIGHEST_PROTOCOL)
else:
    with open(mapping_file_name, 'rb') as handle:
        mapping = pickle.load(handle)

fbi_data_dir = 'fbi-crime-data/'
for my_file in os.listdir(fbi_data_dir):
    with open(fbi_data_dir+my_file, 'r') as handle:
        # fbi_data as pandas data_frame
        fbi_data = pd.read_csv(handle, usecols=['Agency', 'State'])
        fbi_data.columns = ['agency', 'state']
    agency_list = list(map(lambda x: x.lower(), fbi_data.get('agency')))
    state = fbi_data.get('state')[0].upper()
    result = []
    count = 0
    for agency in agency_list:
        for token in match_tokens:
            agency = agency.replace(token, '').strip()
        key = state + '-' + agency
        if not key in mapping:
            result.append(key)
            count += 1
    print(state, len(agency_list), ' #unmatched:', count)
    if state == '':
        for item in result:
            print(item)
        input('################')
        subprocess.call('clear')
