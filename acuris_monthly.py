import pandas as pd
import json
import re
#import sys

#a = sys.argv[1]


jsonObj = pd.read_json(path_or_buf=f'/home/ec2-user/acuris/data/monthly/06_full/indiv.jsonl', lines=True)

busObj = pd.read_json(path_or_buf=f'/home/ec2-user/acuris/data/monthly/06_full/corp.jsonl', lines=True)


ind = pd.DataFrame(columns=['WATCHLIST_ID', 'LAST_NAME', 'FIRST_NAME',
                               'WATCHLIST_NAME', 'WATCHLIST_ALIAS', 'ALTERNATIVE_SPELLING',
                               'LOW_QUALITY_ALIASES', 'WATCHLIST_CATEGORY', 'SUB_CATEGORY',
                               'WATCHLIST_POSITION', 'DATE_OF_BIRTH', 'DOB_WATCHLIST',
                               'WATCHLIST_COUNTRY_OF_BIRTH', 'PLACE_OF_BIRTH',
                               'DECEASED_FLAG', 'COUNTRIES', 'WATCHLIST_NATIONALITY',
                               'WATCHLIST_GENDER', 'COMPANIES', 'KEYWORDS', 'EXTERNAL_SOURCES',
                               'WATCHLIST_IDENTIFICATION_NUMBER', 'PASSPORTS', 'LINKED_TO', 'FURTHER_INFORMATION',
                               'PEP_ROLES', 'WATCHLIST_IDENTIFICATION_TYPE', 'LOCATIONS_COUNTRY', 'WATCHLIST_SOURCE',
                               'NAME_OF_WATCHLIST',
                               'WATCHPERSON_BIRTH_ADDRESS', 'WATCHPERSON_RESIDENTIAL_ADDRESS',
                               'TT_IS_DELETED', 'WATCHLIST_INDIVIDUAL_CORPORATE_TYPE'])

corp = pd.DataFrame()

# INDIVIDUAL FILE EXTRACTS
ind = ind.assign(WATCHLIST_ID=jsonObj['qrCode'], LAST_NAME=jsonObj['lastName'],
                       FIRST_NAME=jsonObj['firstName'],
                       WATCHLIST_NAME=jsonObj['firstName'] + ' ' + jsonObj['middleName'] + ' ' + jsonObj['lastName'],
                       WATCHLIST_ALIAS=jsonObj['aliases'], ALTERNATIVE_SPELLING=jsonObj['aliases'],
                       LOW_QUALITY_ALIASES=jsonObj['aliases'],
                       WATCHLIST_CATEGORY=jsonObj['poiEntries'],
                       DATE_OF_BIRTH=jsonObj['datesOfBirthIso'],
                       DOB_WATCHLIST=jsonObj['datesOfBirthIso'],
                       WATCHLIST_COUNTRY_OF_BIRTH=jsonObj['addresses'],
                       PLACE_OF_BIRTH=jsonObj['addresses'],
                       WATCHLIST_SOURCE='Acuris', NAME_OF_WATCHLIST='Acuris_Watchlist',
                       WATCHPERSON_BIRTH_ADDRESS=jsonObj['addresses'],
                       WATCHPERSON_RESIDENTIAL_ADDRESS=jsonObj['addresses'],
                       LOCATIONS_COUNTRY=jsonObj['addresses'],
                       DECEASED_FLAG=jsonObj['isDeceased'], EXTERNAL_SOURCES=jsonObj['evidences'],
                       COUNTRIES=jsonObj['nationalitiesIsoCodes'],
                       WATCHLIST_NATIONALITY=jsonObj['nationalitiesIsoCodes'],
                       WATCHLIST_GENDER=jsonObj['gender'], FURTHER_INFO=jsonObj['profileImages'],
                       WATCHLIST_IDENTIFICATION_TYPE=jsonObj['identifiers'],
                       WATCHLIST_IDENTIFICATION_NUMBER=jsonObj['identifiers'],
                       WATCHLIST_POSITION=jsonObj['pepEntries'],
                       TT_IS_DELETED=jsonObj['isDeleted'], KEYWORDS=jsonObj['evidences'],
                       WATCHLIST_INDIVIDUAL_CORPORATE_TYPE='I', SUB_CATEGORY=jsonObj['datasets'],
                       PEP_ROLES=jsonObj['pepEntries'], COMPANIES=jsonObj['businessLinks'],
                       PASSPORTS=jsonObj['identifiers'], LINKED_TO=jsonObj['individualLinks'])


# BUSINESS FILE EXTRACT
corp = corp.assign(WATCHLIST_ID=busObj['qrCode'], WATCHLIST_INDIVIDUAL_CORPORATE_TYPE='C', LAST_NAME='NULL',
                   FIRST_NAME=busObj['name'], WATCHLIST_NAME=busObj['name'],
                   WATCHLIST_SOURCE='Acuris', NAME_OF_WATCHLIST='Acuris_Watchlist', TT_IS_DELETED=busObj['isDeleted'],
                   WATCHLIST_ALIAS=busObj['aliases'], SUB_CATEGORY=busObj['datasets'],
                   WATCHLIST_CATEGORY=busObj['poiEntries'], FURTHER_INFO=busObj['profileImages'],
                   LINKED_TO=busObj['individualLinks'], COMPANIES=busObj['businessLinks'],
                   WATCHLIST_COUNTRY_OF_BIRTH=busObj['addresses'],
                   WATCHPERSON_RESIDENTIAL_ADDRESS=busObj['addresses'],
                   WATCHLIST_IDENTIFICATION_NUMBER=busObj['identifiers'],
                   WATCHLIST_IDENTIFICATION_TYPE=busObj['identifiers'],
                   KEYWORDS=busObj['evidences'], WATCHLIST_POSITION=busObj['poiEntries'],
                   PLACE_OF_BIRTH=busObj['addresses'], EXTERNAL_SOURCES=busObj['evidences'],
                   WATCHPERSON_BIRTH_ADDRESS=busObj['addresses'],
                   WATCHLIST_NATIONALITY=busObj['addresses'], LOCATIONS_COUNTRY=busObj['addresses'],
                   COUNTRIES=busObj['addresses'])


# DATE_OF_BIRTH
ind['DATE_OF_BIRTH'] = ind['DATE_OF_BIRTH'].apply(lambda x: ', '.join(str(dt) for dt in x) if x else '')

# DOB_WATCHLIST
ind['DOB_WATCHLIST'] = ind['DOB_WATCHLIST'].apply(lambda x: ', '.join(str(dt) for dt in x) if x else '')


# SUB_CATEGORY
ind['SUB_CATEGORY'] = ind['SUB_CATEGORY'].astype(str).str.replace('[\[\]]', '', regex=True).str.replace("'", "")

corp['SUB_CATEGORY'] = corp['SUB_CATEGORY'].astype(str).str.replace('[\[\]]', '', regex=True).str.replace("'", "")


# FURTHER_INFO
ind['FURTHER_INFO'] = ind['FURTHER_INFO'].astype(str).str.replace('[\[\]]', '', regex=True).str.replace("'", "")

corp['FURTHER_INFO'] = corp['FURTHER_INFO'].astype(str).str.replace('[\[\]]', '', regex=True).str.replace("'", "")


# function to extract the countryIsoCode
#def extract_country_iso_code(json_list, address_type):
#    country_iso_codes = list(set(obj['countryIsoCode'].strip() for obj in json_list if obj.get('addressType') == address_type))
#    return ', '.join(country_iso_codes) if country_iso_codes else None

def extract_country_iso_code(json_list, address_type):
    country_iso_codes = []
    for obj in json_list:
        if obj.get('addressType') == address_type:
            if 'countryIsoCode' in obj:
                try:
                    iso_code = obj['countryIsoCode'].strip()
                    country_iso_codes.append(iso_code)
                except AttributeError:
                    pass
    return ', '.join(country_iso_codes) if country_iso_codes else None


# Function to extract particular key value from single json
def extract_value(json_list, key):
    values = [obj[key] for obj in json_list if key in obj and obj[key] != ""]
    values_string = ", ".join(values)
    return values_string


# To extract value from json of arrays - pep_roles
def pep_roles(json_dict, key):
    value = None
    if json_dict is not None:
        if key in json_dict:
            value = json_dict[key]
    return value


# Function to pick only one key-value pair
def extract_val(json_list, key, category_key, category_value):
    values = [obj[key] for obj in json_list if category_key in obj and obj[category_key] == category_value]
    return values


# Function to omit a key-value pair
def omit_value(json_list, key, category_key, category_value):
    values = [obj[key] for obj in json_list if category_key in obj and obj[category_key] != category_value and obj[key] != ""]
    return values


# Function to extract particular key value from double nested JSON
def nested_json(json_list, nest1, nest2):
    values = []
    for obj in json_list:
        if nest1 in obj:
            nested_obj = obj[nest1]
            if isinstance(nested_obj, list):
                for nested_item in nested_obj:
                    if nest2 in nested_item:
                        values.append(nested_item[nest2])
    return ', '.join(values)


# WATCHLIST_NATIONALITY
ind['WATCHLIST_NATIONALITY'] = ind['WATCHLIST_NATIONALITY'].astype(str).str.replace('[\[\]]', '',
                                                                                          regex=True).str.replace("'", "")
corp['WATCHLIST_NATIONALITY'] = corp['WATCHLIST_NATIONALITY'].apply(
    lambda x: extract_country_iso_code(x, 'Operating'))


# COUNTRIES
ind['COUNTRIES'] = ind['COUNTRIES'].astype(str).str.replace('[\[\]]', '', regex=True).str.replace("'", "")

corp['COUNTRIES'] = corp['COUNTRIES'].apply(
    lambda x: extract_country_iso_code(x, 'Operating'))


# PEP_ROLES
ind['PEP_ROLES'] = ind['PEP_ROLES'].apply(lambda x: pep_roles(x, 'pepEntries').get('pepTier') if pep_roles(x, 'pepEntries') else None)


# WATCHLIST_IDENTIFICATION_TYPE
ind['WATCHLIST_IDENTIFICATION_TYPE'] = ind['WATCHLIST_IDENTIFICATION_TYPE'].apply(lambda x: omit_value(x, 'category', 'category', 'Occupation'))

corp['WATCHLIST_IDENTIFICATION_TYPE'] = corp['WATCHLIST_IDENTIFICATION_TYPE'].apply(lambda x: omit_value(x, 'category', 'category', 'Occupation'))


# WATCHLIST_IDENTIFICATION_NUMBER
ind['WATCHLIST_IDENTIFICATION_NUMBER'] = ind['WATCHLIST_IDENTIFICATION_NUMBER'].apply(lambda x: omit_value(x, 'value', 'category', 'Occupation'))

corp['WATCHLIST_IDENTIFICATION_NUMBER'] = corp['WATCHLIST_IDENTIFICATION_NUMBER'].apply(lambda x: omit_value(x, 'value', 'category', 'Occupation'))


# KEYWORDS
ind['KEYWORDS'] = ind['KEYWORDS'].apply(
    lambda x: extract_value(x, 'keywords'))

corp['KEYWORDS'] = corp['KEYWORDS'].apply(
    lambda x: extract_value(x, 'keywords'))


# EXTERNAL_SOURCES
ind['EXTERNAL_SOURCES'] = ind['EXTERNAL_SOURCES'].apply(
    lambda x: extract_value(x, 'originalUrl'))

corp['EXTERNAL_SOURCES'] = corp['EXTERNAL_SOURCES'].apply(
    lambda x: extract_value(x, 'originalUrl'))


# WATCHLIST_CATEGORY
ind['WATCHLIST_CATEGORY'] = ind['WATCHLIST_CATEGORY'].apply(
    lambda x: extract_value(x, 'category'))

corp['WATCHLIST_CATEGORY'] = corp['WATCHLIST_CATEGORY'].apply(
    lambda x: extract_value(x, 'category'))

# COMPANIES
ind['COMPANIES'] = ind['COMPANIES'].apply(
    lambda x: extract_value(x, 'qrCode'))

corp['COMPANIES'] = corp['COMPANIES'].apply(
    lambda x: extract_value(x, 'qrCode'))


# LINKED_TO
ind['LINKED_TO'] = ind['LINKED_TO'].apply(
    lambda x: extract_value(x, 'qrCode'))

corp['LINKED_TO'] = corp['LINKED_TO'].apply(
    lambda x: extract_value(x, 'qrCode'))


# PASSPORTS
ind['PASSPORTS'] = ind['PASSPORTS'].apply(lambda x: extract_val(x, 'value', 'category', 'Passport Number'))


# Define a function to extract the position value from the JSON string
def extract_position(json_str):
    try:
        data = json.loads(json_str)
        if "pepEntries" in data:
            pep_entries = data["pepEntries"]
            if "current" in pep_entries:
                current = pep_entries["current"]
                if isinstance(current, list) and len(current) > 0:
                    position = current[0].get("position")
                    return position
    except (json.JSONDecodeError, AttributeError):
        pass
    return None


# Apply the function to the column with JSON strings
'''ind['PEP_ROLES'] = ind['PEP_ROLES'].apply(
    lambda x: extract_value(x, 'pepTier'))'''


# Function to concatenate 'firstName', 'middleName', and 'lastName' values from JSON objects
def aliases(json_list):
    full_names = []
    for obj in json_list:
        first_name = obj.get('firstName', '').strip()
        middle_name = obj.get('middleName', '').strip()
        last_name = obj.get('lastName', '').strip()
        names = [name for name in [first_name, middle_name, last_name] if name]
        if names:
            full_names.append(' '.join(names))
    return ', '.join(full_names)


# WATCHLIST_ALIAS
ind['WATCHLIST_ALIAS'] = ind['WATCHLIST_ALIAS'].apply(aliases)

corp['WATCHLIST_ALIAS'] = corp['WATCHLIST_ALIAS'].apply(
    lambda x: extract_value(x, 'alias'))


# Function to concatenate 'firstName', 'middleName', and 'lastName' for alias type
def alt_spell(json_list, type):
    full_names = []
    for obj in json_list:
        if obj.get('type') == type:
            first_name = obj.get('firstName', '').strip()
            middle_name = obj.get('middleName', '').strip()
            last_name = obj.get('lastName', '').strip()
            names = [name for name in [first_name, middle_name, last_name] if name]
            if names:
                full_names.append(' '.join(names))
    return ', '.join(full_names)


# ALTERNATIVE_SPELLING
ind['ALTERNATIVE_SPELLING'] = ind['ALTERNATIVE_SPELLING'].apply(
    lambda x: alt_spell(x, 'Name Spelling Variation'))


# LOW_QUALITY_ALIASES
ind['LOW_QUALITY_ALIASES'] = ind['LOW_QUALITY_ALIASES'].apply(
    lambda x: alt_spell(x, 'Fake Name'))


# WATCHLIST_COUNTRY_OF_BIRTH
ind['WATCHLIST_COUNTRY_OF_BIRTH'] = ind['WATCHLIST_COUNTRY_OF_BIRTH'].apply(
    lambda x: extract_country_iso_code(x, 'Place of birth'))

corp['WATCHLIST_COUNTRY_OF_BIRTH'] = corp['WATCHLIST_COUNTRY_OF_BIRTH'].apply(
    lambda x: extract_country_iso_code(x, 'Registered'))

# LOCATIONS_COUNTRY
ind['LOCATIONS_COUNTRY'] = ind['LOCATIONS_COUNTRY'].apply(lambda x: extract_country_iso_code(x, 'Residential'))

corp['LOCATIONS_COUNTRY'] = corp['LOCATIONS_COUNTRY'].apply(lambda x: extract_country_iso_code(x, 'Operating'))


# addresses
def addresses(json_list, address_type):
    concatenated_values = []
    for data in json_list:
        if isinstance(data, dict) and data.get('addressType') == address_type:
            values = [value for key, value in data.items() if key != 'addressType' and value != '']
            concatenated_values.append(', '.join(values))
    return ', '.join(concatenated_values) if concatenated_values else None


# PLACE_OF_BIRTH
ind['PLACE_OF_BIRTH'] = ind['PLACE_OF_BIRTH'].apply(lambda x: addresses(x, 'Place of birth'))

corp['PLACE_OF_BIRTH'] = corp['PLACE_OF_BIRTH'].apply(lambda x: addresses(x, 'Registered'))


# WATCHPERSON_BIRTH_ADDRESS
ind['WATCHPERSON_BIRTH_ADDRESS'] = ind['WATCHPERSON_BIRTH_ADDRESS'].apply(
    lambda x: addresses(x, 'Place of birth'))

corp['WATCHPERSON_BIRTH_ADDRESS'] = corp['WATCHPERSON_BIRTH_ADDRESS'].apply(
    lambda x: addresses(x, 'Registered'))


# WATCHPERSON_RESIDENTIAL_ADDRESS
ind['WATCHPERSON_RESIDENTIAL_ADDRESS'] = ind['WATCHPERSON_RESIDENTIAL_ADDRESS'].apply(
    lambda x: addresses(x, 'Residential'))

corp['WATCHPERSON_RESIDENTIAL_ADDRESS'] = corp['WATCHPERSON_RESIDENTIAL_ADDRESS'].apply(
    lambda x: addresses(x, 'Operating'))


# WATCHLIST_POSITION
ind['WATCHLIST_POSITION'] = ind['WATCHLIST_POSITION'].apply(lambda x: nested_json(x, 'positions', 'position') if x else '')

corp['WATCHLIST_POSITION'] = corp['WATCHLIST_POSITION'].apply(lambda x: nested_json(x, 'positions', 'position') if x else '')

# CONCAT INDIVIDUAL AND BUSINESS FILES
finale = pd.concat([ind, corp], ignore_index=True)

finale.to_csv(f'/home/ec2-user/acuris/data/monthly/06_full/finale.csv', index=False)

