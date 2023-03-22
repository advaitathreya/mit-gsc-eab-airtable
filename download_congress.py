import urllib3
import xmltodict
import pandas as pd

congress_session = 118

url = r'https://clerk.house.gov/xml/lists/MemberData.xml'
fileName = 'data/Congress/MemberData.xml'
with urllib3.PoolManager() as http:
    r = http.request('GET', url)
    with open(fileName, 'wb') as fout:
        fout.write(r.data)

with open(fileName, 'r') as f:
    xml_data = xmltodict.parse(f.read())

congress_session_downloaded = int(xml_data['MemberData']['title-info']['congress-num'])
assert congress_session_downloaded == congress_session

m = []
for elem in xml_data['MemberData']['committees']['committee']:
    new = {'type': elem['@type'],
           'code': elem['@comcode'],
           'name': elem['committee-fullname'],
          }
    m.append(new)
    
    try:
        subcommittees = elem['subcommittee']
    except KeyError:
        subcommittees = []
        
    for subelem in subcommittees:
        new = {'type': 'subcommittee',
               'parent': elem['@comcode'],
               'code': subelem['@subcomcode'],
               'name': subelem['subcommittee-fullname'],
          }
        m.append(new)

cdf = pd.DataFrame(m)
cdf.to_csv(f'data/Congress/{congress_session}_HouseCommittees.csv', index=False)

m = []
for elem in xml_data['MemberData']['members']['member']:
    try:
        committees = elem['committee-assignments']['committee']
        if type(committees) != list:
            committees = [committees]
    except KeyError:
        committees = []
        
    try:
        subcommittees = elem['committee-assignments']['subcommittee']
        if type(subcommittees) != list:
            subcommittees = [subcommittees]
    except KeyError:
        subcommittees = []
        
    assignments = []
    for a in committees:
        try:
            assignments.append((a['@comcode'], a['@rank']))
        except KeyError:
            continue
    for a in subcommittees:
        try:
            assignments.append((a['@subcomcode'], a['@rank']))
        except KeyError:
            continue

    new = {'district': elem['statedistrict'],
           'firstname': elem['member-info']['firstname'],
           'lastname': elem['member-info']['lastname'],
           'party': elem['member-info']['party'],
           'committee_assignments': assignments,
          }
    m.append(new)

mdf = pd.DataFrame(m)
mdf.to_csv(f'data/Congress/{congress_session}_HouseReps.csv', index=False)

url = r'https://www.senate.gov/general/contact_information/senators_cfm.xml'
fileName = 'data/Congress/senators_cfm.xml'
with urllib3.PoolManager() as http:
    r = http.request('GET', url)
    with open(fileName, 'wb') as fout:
        fout.write(r.data)

with open(fileName, 'r') as f:
    xml_data1 = xmltodict.parse(f.read())

url = 'https://www.senate.gov/legislative/LIS_MEMBER/cvc_member_data.xml'
fileName = 'data/Congress/cvc_member_data.xml'
with urllib3.PoolManager() as http:
    r = http.request('GET', url)
    with open(fileName, 'wb') as fout:
        fout.write(r.data)

with open(fileName, 'r') as f:
    xml_data2 = xmltodict.parse(f.read())

sen_coms = []
comcodes = {}
for elem in xml_data2['senators']['senator']:
    mem_id = elem['bioguideId']
    firstname = elem['name']['first']
    lastname = elem['name']['last']
    try:
        committees = elem['committees']['committee']
        if type(committees) != list:
            committees = [committees]
    except KeyError:
        committees = []
    
    assignments = []
    for a in committees:
        try:
            assignments.append(a['@code'])
            comcodes[a['@code']] = a['#text']
        except KeyError:
            continue
    
    sen_coms.append({'id': mem_id, 'firstname': firstname, 'lastname': lastname, 'committee_assignments': assignments})
sen_coms = pd.DataFrame(sen_coms).set_index('id')

abbrs = [c[:-2] for c in list(comcodes.keys())]

xmls = []
for abbr in abbrs:
    url = f'https://www.senate.gov/general/committee_membership/committee_memberships_{abbr}.xml'
    fileName = f'data/Congress/Senate_Committees/committee_memberships_{abbr}.xml'
    with urllib3.PoolManager() as http:
        r = http.request('GET', url)
        with open(fileName, 'wb') as fout:
            fout.write(r.data)

    with open(fileName, 'r') as f:
        xml_data = xmltodict.parse(f.read())
    
    xmls.append(xml_data.copy())

assignments = dict(zip(sen_coms.index, [[] for _ in range(len(sen_coms))]))

for comm in xmls:
    code = comm['committee_membership']['committees']['committee_code']
    name = comm['committee_membership']['committees']['committee_name']
    members = comm['committee_membership']['committees']['members']
    comcodes[code] = name

    for sen in members['member']:
        mname = sen['name']
        pos = sen['position']
        try:
            assignments[sen_coms.loc[(sen_coms['firstname'] == mname['first']) & (sen_coms['lastname'] == mname['last'])].index[0]] += [(code, pos)]
        except IndexError:
            print(mname)
            
    try:
        subcommittees = comm['committee_membership']['committees']['subcommittee']
    except KeyError:
        subcommittees = []
    
    for subc in subcommittees:
        code = subc['committee_code']
        name = subc['subcommittee_name']
        members = subc['members']
        comcodes[code] = name
        
        if members:
            for sen in members['member']:
                mname = sen['name']
                pos = sen['position']
                try:
                    assignments[sen_coms.loc[(sen_coms['firstname'] == mname['first']) & (sen_coms['lastname'] == mname['last'])].index[0]] += [(code, pos)]
                except IndexError:
                    print(mname)

m = []
class_map = {'Class I': '-Sen1', 'Class II': '-Sen2', 'Class III': '-Sen3'}
for elem in xml_data1['contact_information']['member']:
    new = {'state': elem['state'] + class_map[elem['class']],
           'firstname': elem['first_name'],
           'lastname': elem['last_name'],
           'party': elem['party'],
           'committee_assignments': assignments[elem['bioguide_id']]
          }
    m.append(new)

cdf = pd.DataFrame(m)

cdf.to_csv(f'data/Congress/{congress_session}_Senators.csv', index=False)