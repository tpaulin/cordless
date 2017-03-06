# Import all libraries needed for the tutorial
import time

import pandas as pd
import itertools

import json

from bs4 import BeautifulSoup, NavigableString
import requests
import re

import pprint

import logging
log = logging.getLogger(__name__)

# Define config, input and output file
use_iso_3 = False

# Input files:
# - eu contains eu projects and data
# - countries contains country metadata, e.g, name nad ISO-2, ISO-3 abreviations
target_directory = 'out/'
source_directory = 'data/'
web_directory = 'data/web/'

h2020_projects_url = 'http://cordis.europa.eu/data/cordis-h2020projects.csv'
h2020_organizations_url = 'http://cordis.europa.eu/data/cordis-h2020organizations.csv'

cordis_h2020projects_csv = source_directory + 'cordis-h2020projects.csv'
cordis_h2020organizations_csv = source_directory + 'cordis-h2020companies.csv'
all_countries_csv = source_directory + 'all_countries.csv'
eu_project_file_json = source_directory + 'h2020_projects.json'

# dist for converting ISO-2 to ISO-3 country abriviations
transform_country_code = {"BD": "BGD", "BE": "BEL", "BF": "BFA", "BG": "BGR", "BA": "BIH", "BB": "BRB", "WF": "WLF", "BL": "BLM", "BM": "BMU", "BN": "BRN", "BO": "BOL", "BH": "BHR", "BI": "BDI", "BJ": "BEN", "BT": "BTN", "JM": "JAM", "BV": "BVT", "BW": "BWA", "WS": "WSM", "BQ": "BES", "BR": "BRA", "BS": "BHS", "JE": "JEY", "BY": "BLR", "BZ": "BLZ", "RU": "RUS", "RW": "RWA", "RS": "SRB", "TL": "TLS", "RE": "REU", "TM": "TKM", "TJ": "TJK", "RO": "ROU", "TK": "TKL", "GW": "GNB", "GU": "GUM", "GT": "GTM", "GS": "SGS", "GR": "GRC", "GQ": "GNQ", "GP": "GLP", "JP": "JPN", "GY": "GUY", "GG": "GGY", "GF": "GUF", "GE": "GEO", "GD": "GRD", "GB": "GBR", "GA": "GAB", "SV": "SLV", "GN": "GIN", "GM": "GMB", "GL": "GRL", "GI": "GIB", "GH": "GHA", "OM": "OMN", "TN": "TUN", "JO": "JOR", "HR": "HRV", "HT": "HTI", "HU": "HUN", "HK": "HKG", "HN": "HND", "HM": "HMD", "VE": "VEN", "PR": "PRI", "PS": "PSE", "PW": "PLW", "PT": "PRT", "SJ": "SJM", "PY": "PRY", "IQ": "IRQ", "PA": "PAN", "PF": "PYF", "PG": "PNG", "PE": "PER", "PK": "PAK", "PH": "PHL", "PN": "PCN", "PL": "POL", "PM": "SPM", "ZM": "ZMB", "EH": "ESH", "EE": "EST", "EG": "EGY", "ZA": "ZAF", "EC": "ECU", "IT": "ITA", "VN": "VNM", "SB": "SLB", "ET": "ETH", "SO": "SOM", "ZW": "ZWE", "SA": "SAU", "ES": "ESP", "ER": "ERI", "ME": "MNE", "MD": "MDA", "MG": "MDG", "MF": "MAF", "MA": "MAR", "MC": "MCO", "UZ": "UZB", "MM": "MMR", "ML": "MLI", "MO": "MAC", "MN": "MNG", "MH": "MHL", "MK": "MKD", "MU": "MUS", "MT": "MLT", "MW": "MWI", "MV": "MDV", "MQ": "MTQ", "MP": "MNP", "MS": "MSR", "MR": "MRT", "IM": "IMN", "UG": "UGA", "TZ": "TZA", "MY": "MYS", "MX": "MEX", "IL": "ISR", "FR": "FRA", "IO": "IOT", "SH": "SHN", "FI": "FIN", "FJ": "FJI", "FK": "FLK", "FM": "FSM", "FO": "FRO", "NI": "NIC", "NL": "NLD", "NO": "NOR", "NA": "NAM", "VU": "VUT", "NC": "NCL", "NE": "NER", "NF": "NFK", "NG": "NGA", "NZ": "NZL", "NP": "NPL", "NR": "NRU", "NU": "NIU", "CK": "COK", "XK": "XKX", "CI": "CIV", "CH": "CHE", "CO": "COL", "CN": "CHN", "CM": "CMR", "CL": "CHL", "CC": "CCK", "CA": "CAN", "CG": "COG", "CF": "CAF", "CD": "COD", "CZ": "CZE", "CY": "CYP", "CX": "CXR", "CR": "CRI", "CW": "CUW", "CV": "CPV", "CU": "CUB", "SZ": "SWZ", "SY": "SYR", "SX": "SXM", "KG": "KGZ", "KE": "KEN", "SS": "SSD", "SR": "SUR", "KI": "KIR", "KH": "KHM", "KN": "KNA", "KM": "COM", "ST": "STP", "SK": "SVK", "KR": "KOR", "SI": "SVN", "KP": "PRK", "KW": "KWT", "SN": "SEN", "SM": "SMR", "SL": "SLE", "SC": "SYC", "KZ": "KAZ", "KY": "CYM", "SG": "SGP", "SE": "SWE", "SD": "SDN", "DO": "DOM", "DM": "DMA", "DJ": "DJI", "DK": "DNK", "VG": "VGB", "DE": "DEU", "YE": "YEM", "DZ": "DZA", "US": "USA", "UY": "URY", "YT": "MYT", "UM": "UMI", "LB": "LBN", "LC": "LCA", "LA": "LAO", "TV": "TUV", "TW": "TWN", "TT": "TTO", "TR": "TUR", "LK": "LKA", "LI": "LIE", "LV": "LVA", "TO": "TON", "LT": "LTU", "LU": "LUX", "LR": "LBR", "LS": "LSO", "TH": "THA", "TF": "ATF", "TG": "TGO", "TD": "TCD", "TC": "TCA", "LY": "LBY", "VA": "VAT", "VC": "VCT", "AE": "ARE", "AD": "AND", "AG": "ATG", "AF": "AFG", "AI": "AIA", "VI": "VIR", "IS": "ISL", "IR": "IRN", "AM": "ARM", "AL": "ALB", "AO": "AGO", "AQ": "ATA", "AS": "ASM", "AR": "ARG", "AU": "AUS", "AT": "AUT", "AW": "ABW", "IN": "IND", "AX": "ALA", "AZ": "AZE", "IE": "IRL", "ID": "IDN", "UA": "UKR", "QA": "QAT", "MZ": "MOZ"}
# EU members:
eu_countries = ('AUT', 'BEL', 'BGR', 'HRV', 'CYP', 'CZE', 'DNK', 'EST', 'FIN', 'FRA', 'DEU', 'GRC', 'HUN', 'IRL', 'ITA', 'LVA', 'LTU', 'LUX', 'MLT', 'NLD', 'POL', 'PRT', 'ROU', 'SVK', 'SVN', 'ESP', 'SWE', 'GBR')

csv_out_file = open(target_directory + 'combinations_all.csv', 'w')
dot_out_file = open(target_directory + 'combinations_all.dot', 'w')
json_out_file = open(target_directory + 'combinations_all.json', 'w')
countries_out_file = open(target_directory + 'country_data.json', 'w')
country_links_out = open(target_directory + 'country_inter_connection.json', 'w')


# -- Start processing -- #
def get_h2020_files():
    def download_url_to(source_url, destination_file):
        import requests
        try:
            with open(destination_file, 'wb') as f:
                response = requests.get(source_url, stream=True)
                if not response.ok:
                    log.critical('Could not download: ', response)
                for block in response.iter_content(1024):
                    f.write(block)
                f.flush()
        except Exception as e:
            log.critical('Could not retrieve ' + str(source_url) + ' due  to ' + str(e))
            quit()
    for source_url, target_file in zip([h2020_projects_url, h2020_organizations_url],
                                       [cordis_h2020projects_csv, cordis_h2020organizations_csv]):
        download_url_to(source_url, target_file)
get_h2020_files()


def load_h2020_files():
    pd_country_meta_data = pd.read_csv(all_countries_csv)
    pd_eu_projects = pd.read_csv(cordis_h2020projects_csv, delimiter=';', quotechar='"')
    pd_eu_organisations = pd.read_csv(cordis_h2020organizations_csv, delimiter=';', quotechar='"')
    return pd_country_meta_data, pd_eu_projects, pd_eu_organisations

project_count = dict()
country_labels = dict()


def get_project_as_soup(rcn):
    standard_rul = 'http://cordis.europa.eu/project/rcn/{rcn}_en.html'.format(rcn=rcn)
    standard_file = '{rcn}_en.html'.format(rcn=rcn)

    result = None
    try:
        result = open(web_directory + standard_file)
    except FileNotFoundError:
        print('Local file for {rcn} not found, attempting to download '.format(rcn=standard_file))

        try:
            result = requests.get(standard_rul)
            if result.status_code == 200:
                result = result.text
                with open(web_directory + standard_file, 'w') as e:
                    e.write(result)
                    e.flush()
                time.sleep(0.5)

            else:
                raise FileNotFoundError('Could not get file', result.status_code, standard_rul)
        except:
            raise FileNotFoundError('Could not get file', standard_rul)

    return BeautifulSoup(result, 'html.parser')


def get_coordinator(source):
    coordinator = source.find('div', {'class': 'coordinator'})
    if coordinator:
        coordinator = get_metadata(coordinator)
        return coordinator
    else:
        return None


def get_metadata(sub_soup):
    this_dict = dict()
    name = sub_soup.find('div', {'class': 'name'}).get_text()
    name = name.replace('Participation ended', ' ').replace('\n', ' ').replace('"', '').strip()
    this_dict['name'] = name
    this_dict['country'] = sub_soup.find('div', {'class': 'country'}).get_text()

    p_address = sub_soup.find('div', {'class': 'optional'})
    if p_address:
        inner_text = [element.strip() for element in p_address if isinstance(element, NavigableString) and element.strip() != '']
        this_dict['address'] = ', '.join(inner_text)

    p_contribution = sub_soup.find('p', {'class': 'partipant-contribution'}).get_text()
    # print(p_contribution)
    this_dict['contribution'] = re.findall("\d+\,*\d*", p_contribution.strip().replace(' ', ''))[0]

    return this_dict


def get_participants(source):
    participants_div = source.find('div', {'class': 'participants'})
    participants_div = participants_div.find_all('div', {'class': 'participant'})
    participants = [get_metadata(participant) for participant in participants_div]
    return participants


def load_project_file(file_name):
    try:
        with open(source_directory + file_name, 'r') as fp:
            projects = json.load(fp)
        return projects

    except:
        print('Project file ({file_name}) not found, forcing reload.'.format(file_name=file_name))
        return False


def extract_meta_data(pk):
    project = dict()

    try:
        project['rcn'] = str(pk['rcn'])  # Make sure that rcn is string...

        # Extract data from the web source:
        soup = get_project_as_soup(project['rcn'])
        # Make sure that the names match
        project['acronym'] = soup.find('div', {'class': 'header'}).find('h1').get_text()
        if project['acronym'] != pk['acronym']:
            print('Error: Acronyms do not match', project['acronym'], pk['acronym'], 'using web acronym')
        # Get the coordinator:
        project['coordinator'] = get_coordinator(soup)
        try:
            project['participants'] = get_participants(soup)
            project['participant_count'] = 1 + len(project['participants'])
        except:
            project['participants'] = 'None'
            project['participant_count'] = 1

        # Extract data from the csv file:
        string_variables = ['rcn', 'reference', 'status', 'programme', 'topics', 'frameworkProgramme', 'title',
                            'objective', 'call', 'coordinator', 'coordinatorCountry']

        project['rcn'] = str(pk['rcn'])
        project['reference'] = pk.get('reference', 'null')
        project['status'] = pk.get('status', 'null')
        project['programme'] = pk.get('programme', 'null')
        project['topics'] = pk.get('topics', 'null')
        project['frameworkProgramme'] = pk.get('frameworkProgramme', 'null')
        project['title'] = pk.get('title', 'null')
        project['objective'] = pk.get('objective', 'null')
        project['call'] = pk.get('call', 'null')
        project['programme_coordinator'] = pk.get('coordinator', 'null')
        project['programme_coordinator_country'] = pk.get('coordinatorCountry', 'null')

        ############################
        # Fix this to get stuff out of the webpage?!
        ############################
        project['startDate'] = pk.get('startDate', 'null') if not pd.isnull(pk.get('startDate', 'null')) else 'null'
        project['endDate'] = pk.get('endDate', 'null') if not pd.isnull(pk.get('endDate', 'null')) else 'null'
        project['projectUrl'] = pk.get('projectUrl', 'null') if not pd.isnull(pk.get('projectUrl')) else 'null'
        project['ecMaxContribution'] = pk.get('ecMaxContribution', 'null') if not pd.isnull(
            pk.get('ecMaxContribution')) else 'null'
        project['fundingScheme'] = pk.get('fundingScheme', 'null') if not pd.isnull(pk.get('fundingScheme')) else 'null'

        project['totalCost'] = float(pk.get('totalCost', '0').replace(',', '.'))

        return project

    except FileNotFoundError:
        return None


def convert_to_json(store_to_file=True, file_name='project_meta_data.json'):
    pd_country_meta_data, pd_eu_projects, pd_eu_organisations = load_h2020_files()

    projects = dict()

    # Remove special characters
    cln = list(pd_eu_projects.columns)
    cln[0] = 'rcn'
    pd_eu_projects.columns = cln

    cln = list(pd_eu_organisations.columns)
    cln[0] = 'rcn'
    cln[2] = 'acronym'

    pd_eu_organisations.columns = cln

    for index, pk in pd_eu_organisations.iterrows():
        index += 1
        try:
            project = extract_meta_data(pk)
            projects[project['rcn']] = project
        except FileNotFoundError:
            print('Could not process ', pk['rcn'])
        except TypeError:
            print('TypeError: Could not process ', pk['rcn'])

    for index, pk in pd_eu_projects.iterrows():
        index += 1
        try:
            project = extract_meta_data(pk)
            projects[project['rcn']] = project
        except FileNotFoundError:
            print('FileNotFoundError: Could not process ', pk['rcn'])
        except TypeError:
            print('TypeError: Could not process ', pk['rcn'])

    if store_to_file:
        # We store the data as a json array and a raw dict dump so we
        # can import the array into mongo and reload the file
        [print('PROJ', projects[e]) for e in projects]
        with open(source_directory + 'json_' + file_name, 'w') as fp:
            json.dump([{'_id': e, 'project': projects[e]} for e in projects], fp, indent=True, allow_nan=False)

        with open(source_directory + file_name, 'w') as fp:
            json.dump(projects, fp, indent=True)

    return projects


def load_project_meta_data_from_json(force_update=False, store_to_file=True, file_name='project_meta_data.json'):
    """
    Loads the projects as dict
    :param force_update:
    :param store_to_file:
    :param file_name:
    :return:
    """
    projects = load_project_file(file_name)
    if not projects:
        force_update = True

    if force_update:
        projects = convert_to_json()

    return projects


def generate_country_and_company_metadata(project_data, force_reload=False):
    ###
    # TODO: As this is trivial, make it a query
    # Iterate over the project to extract:
    # - per country participation (as in number of projects
    # - get their country lable
    # - get their iso-3 code (for d3.js)
    ###

    pd_country_meta_data, pd_eu_projects, pd_eu_organisations = load_h2020_files()

    def get_information(participant, acronym, rcn):
        # Check if we can identify the participant country otherwise fallback to Unknown:
        print(participant)

        row = pd_country_meta_data.loc[participant['country'] == pd_country_meta_data['name']]
        if row.empty:
            cnt_iso_3 = 'Unknown'
            cnt_iso_2 = 'Unknown'
            print('Could not look up ', participant['country'])
        else:
            if row['alpha-3'].empty:
                print('Alhpa-3 is missing for ', participant['country'])
                cnt_iso_3 = row['alpha-3'].item()
            else:
                cnt_iso_3 = row['alpha-3'].item()
            if row['alpha-3'].empty:
                print('Alhpa-2 is missing for ', participant['country'])
                cnt_iso_2 = row['alpha-2'].item()
            else:
                cnt_iso_2 = row['alpha-2'].item()

            # print('looked_up', participant['country'], 'as', cnt_iso_2, 'and', cnt_iso_3)

        # Update country information:
        country_data.setdefault(cnt_iso_3, dict())
        country_data[cnt_iso_3].setdefault('project_participation', 0)
        country_data[cnt_iso_3].setdefault('contribution', 0)

        # This makes is number of company involvments per county
        # TODO: consider making this something else
        country_data[cnt_iso_3]['project_participation'] += 1
        country_data[cnt_iso_3]['contribution'] += int(float(participant['contribution'].replace(',', '.')))
        country_data[cnt_iso_3]['name'] = participant['country']

        if cnt_iso_3 in eu_countries:
            country_data[cnt_iso_3]['fillKey'] = 'EU'
        else:
            country_data[cnt_iso_3]['fillKey'] = 'nonEU'

        # TODO: add comparison between existing and new information
        participant['name'] = str(participant['name']).replace('"', '')
        if company_data.get(participant['name']):
            company_data[participant['name']]['contribution'] += int(float(participant['contribution'].replace(',', '.')))
            company_data[participant['name']]['project_participation'] += 1
            company_data[participant['name']]['projects'][acronym] = dict()
            company_data[participant['name']]['projects'][acronym]['acronym'] = acronym
            company_data[participant['name']]['projects'][acronym]['rcn'] = rcn
        else:
            company_data[participant['name']] = dict()
            company_data[participant['name']]['name'] = participant['name']  # Might be redundant, unless key is changed into an id
            company_data[participant['name']]['address'] = participant['address']
            company_data[participant['name']]['country'] = participant['country']
            company_data[participant['name']]['contribution'] = int(float(participant['contribution'].replace(',', '.')))
            if participant['country'] == 'Namibia':
                company_data[participant['name']]['country_iso2'] ='NA'
            else:
                company_data[participant['name']]['country_iso2'] = cnt_iso_2
            company_data[participant['name']]['country_iso3'] = cnt_iso_3
            company_data[participant['name']]['project_participation'] = 1
            company_data[participant['name']]['projects'] = dict()
            company_data[participant['name']]['projects'][acronym] = dict()
            company_data[participant['name']]['projects'][acronym]['acronym'] = acronym
            company_data[participant['name']]['projects'][acronym]['rcn'] = rcn


    try:
        with open(source_directory + 'country_meta_data.json', 'r') as fp:
            country_data = json.load(fp)

        with open(source_directory + 'company_meta_data.json', 'r') as fp:
            company_data = json.load(fp)
    except FileNotFoundError:
        force_reload = True

    if force_reload:
        country_data = dict()
        company_data = dict()
        index = 0
        for index, pk in pd_eu_projects.iterrows():
            #if index < 30:
            if True:
                index += 1
                if project_data.get(str(pk['rcn'])) or project_data.get(pk['rcn']):

                    coordinator = project_data[str(pk['rcn'])]['coordinator']
                    get_information(coordinator, pk['acronym'], str(pk['rcn']))

                    if project_data[str(pk['rcn'])]['participants'] != 'None':
                        participants = project_data[str(pk['rcn'])]['participants']
                        for participant in participants:
                            get_information(participant, pk['acronym'], str(pk['rcn']))


                else:
                    print('Project with rcn not found:', str(pk['rcn']))

        with open(source_directory + 'json_country_meta_data.json', 'w') as fp:
            json.dump([{'_id': e, 'country': country_data[e]} for e in country_data], fp, indent=True, allow_nan=False)
        with open(source_directory + 'country_meta_data.json', 'w') as fp:
            json.dump(country_data, fp, indent=True)

        with open(source_directory + 'company_meta_data.json', 'w') as fp:
            json.dump(company_data, fp, indent=True)
        with open(source_directory + 'json_company_meta_data.json', 'w') as fp:
            def check_dict(dic):
                # print(dic)
                return json.dumps({'_id': dic['name'], 'company': dic})
            # dd = [check_dict(company_data[e]) for e in company_data if check_dict(company_data[e]) is not None]
            # [company_data[e].pop('partners') for e in company_data if company_data[e].get('partners')]
            json.dump([{'_id': e, 'company': company_data[e]} for e in company_data], fp, indent=True, allow_nan=True)
            # json.dump([e for e in dd], fp, indent=True, allow_nan=False)

    return [country_data, company_data]


def generate_country_and_company_relations(project_data, company_data):
    company_links = dict()
    country_links = dict()
    pd_country_meta_data, pd_eu_projects = load_h2020_files()

    for index, pk in pd_eu_projects.iterrows():
        # For some reason, subjects are participants and participants are project leeds???
        #if index < 30:
        if True:
            project_subjects = project_data[str(pk['rcn'])]['participants']
            project_participants = project_data[str(pk['rcn'])]['coordinator']

            if project_subjects != 'None':  # If there are no subjects there is no collaboration...
                # Merge subjects and participant(s) into one line. For now we don't care about the difference
                project_subjects.extend([project_participants])
                project_subjects_as_list = [e['name'] for e in project_subjects]

                for company_pair in list(itertools.product(project_subjects_as_list, project_subjects_as_list)):
                    if company_pair[0] != company_pair[1]:
                        # TODO: Consider whether e should be reordered!
                        ordered = company_pair
                        company_links.setdefault(
                            (ordered[0], ordered[1]), 0)
                        company_links[
                            (ordered[0], ordered[1])] += 1
                        company_data[company_pair[0]].setdefault('partners', [])
                        company_data[company_pair[0]]['partners'].extend([company_pair[1]])

                    if company_data[company_pair[0]]['country_iso3'] != company_data[company_pair[1]]['country_iso3']:
                        country_links.setdefault(
                            (company_data[ordered[0]]['country_iso3'], company_data[ordered[1]]['country_iso3']), 0)
                        country_links[
                            (company_data[ordered[0]]['country_iso3'], company_data[ordered[1]]['country_iso3'])] += 1
            else:
                company_data[project_participants['name']].setdefault('partners', [])

    with open(source_directory + 'company_meta_data.json', 'w') as fp:
        json.dump(company_data, fp, indent=True)

    # Generate origin destination file for d3.js (datamaps) (interaction between countries)
    # format is json:
    # origin
    # destination
    # stokeWidth (should be normalized logorithmitically)
    # strokeColor (match it to strokeWidth?)
    print('Generating country links as json')
    country_links_out.write('[\n')
    is_first = True
    for e in country_links:
        if (e[0] in eu_countries and e[1] in eu_countries) and (e[0] != e[1]):  # Filter for eu countries only
            if is_first:
                is_first = False
            else:
                country_links_out.write(',')
            country_links_out.write(json.dumps({'origin': e[0], 'destination': e[1], 'strokeWidth': 0.3, 'strokeColor': 'lightblue'}))
            country_links_out.write('\n')
    country_links_out.write(']\n')

    # Filter and generate origin destination file for d3.js circo graph (interaction between companies)
    print('Generating company links as json')
    threshold = 1
    list_of_accepted_partners = dict()
    list_of_child_partners = dict()

    for company in company_data:
        # and (project_count[src_node]['country'] == 'AT' or project_count[src_node]['country'] == 'DE'):
        #if company_data[company]['project_participation'] > threshold and (company_data[company].get('partners') and len(company_data[company].get('partners')) > 0) and company_data[company]['country_iso2'] == 'AT':

        if 'synyo' in company.lower() or (company_data[company].get('partners') and any('synyo' in e.lower() for e in company_data[company].get('partners'))):
            print(company)
            list_of_accepted_partners[company] = True
        else:
            list_of_accepted_partners[company] = False

    srt = sorted(company_data, key=lambda k: company_data[k]['country'], reverse=False)
    srt = sorted(company_data, key=lambda k: company_data[k]['project_participation'], reverse=False)

    is_first_element = True
    json_out_file.write('[\n')
    for company in srt:
        country_prefix = company_data[company]['country']

        if list_of_accepted_partners[company]:
            this_string = ''
            if is_first_element:
                is_first_element = False
            else:
                this_string += ','
            this_string += '{"name":"' + str(country_prefix) + ' - ' + str(company).replace('"', ' ').replace("'", ' ' ) + '","size":10,"imports":['

            any_acceptable = False
            is_first_partner = True
            for dst_node in company_data[company]['partners']:
                dst_country = company_data[dst_node]['country']

                if is_first_partner and list_of_accepted_partners[dst_node]:
                    this_string += '"' + str(dst_country).replace('"', ' ').replace("'", ' ' ) + ' - ' + str(dst_node).replace('"', ' ').replace("'", ' ' ) + '"'
                    is_first_partner = False
                    any_acceptable =True
                elif list_of_accepted_partners[dst_node]:
                    this_string += ',"' + str(dst_country).replace('"', ' ').replace("'", ' ' ) + ' - ' + str(dst_node).replace('"', ' ').replace("'", ' ' ) + '"'
                    any_acceptable = True

                csv_out_file.write('"'+str(company).replace('"', ' ').replace("'", ' ') + '"' + ',' + '"' + str(dst_node).replace('"', ' ').replace("'", ' ') + '"' + ';\n')
                dot_out_file.write('"'+str(company).replace('"', ' ').replace("'", ' ') + '"' + '->' + '"' + str(dst_node).replace('"', ' ').replace("'", ' ') + '"' + ';\n')
                csv_out_file.flush()
                dot_out_file.flush()

            this_string += ']}\n'

            if any_acceptable:
                json_out_file.write(this_string)
                json_out_file.flush()
    counter = 0
    delimiter = '\t'
    for country in ['']:
        srt = sorted(company_data, key=lambda k: company_data[k]['project_participation'], reverse=True)
        srt = sorted(company_data, key=lambda k: company_data[k]['contribution'], reverse=True)
        f_out = open(target_directory + 'per_company_project_count_{iso}.csv'.format(iso=country), 'w')
        p_out = open(target_directory + 'per_company_project_contribution_{iso}.csv'.format(iso=country), 'w')
        f_out.write('id'+delimiter+'value\n')
        p_out.write('id'+delimiter+'value\n')
        for company in srt:
            if counter < 30:
                f_out.write('"' + company + '"' + delimiter + str(int(float(company_data[company]['project_participation']))) + '\n')
                p_out.write('"' + company + '"' + delimiter + str(int(float(company_data[company]['contribution']))) + '\n')
                counter += 1

        f_out.flush()
        f_out.close()
        p_out.flush()
        p_out.close()

    json_out_file.write('\n]')


def get_data():
    pass

if __name__ == '__main__':
    print('Running')
    project_data = load_project_meta_data_from_json(force_update=False)
    country_data, company_data = generate_country_and_company_metadata(project_data, force_reload=True)
    generate_country_and_company_relations(project_data, company_data)

    pprint.pprint(type(project_data))
