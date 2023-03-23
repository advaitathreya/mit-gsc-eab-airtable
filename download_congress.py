import urllib3
import xmltodict
import pandas as pd
import sys

congress_session = int(sys.argv[1])

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
           'parent': elem['@comcode'],
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
cdf.to_csv(f'data/Congress/{congress_session}_HouseCommittees.tsv', sep='\t', index=False)

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
        
    cleadership = []
    cmembership = []
    for a in committees:
        try:
            if a['@rank'] == '1':
                cleadership.append(f"{a['@comcode']}_{congress_session}")
            else:
                cmembership.append(f"{a['@comcode']}_{congress_session}")
        except KeyError:
            continue
    for a in subcommittees:
        try:
            if a['@rank'] == '1':
                cleadership.append(f"{a['@subcomcode']}_{congress_session}")
            else:
                cmembership.append(f"{a['@subcomcode']}_{congress_session}")
        except KeyError:
            continue
            
    new = {'district': elem['statedistrict'],
           'firstname': elem['member-info']['firstname'],
           'lastname': elem['member-info']['lastname'],
           'party': elem['member-info']['party'],
           'committee_leadership': cleadership,
           'committee_membership': cmembership,
           'UID': elem['member-info']['bioguideID'],
          }
    m.append(new)

mdf = pd.DataFrame(m)
mdf.to_csv(f'data/Congress/{congress_session}_HouseReps.tsv', sep='\t', index=False)

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

cleadership = dict(zip(sen_coms.index, [[] for _ in range(len(sen_coms))]))
cmembership = dict(zip(sen_coms.index, [[] for _ in range(len(sen_coms))]))
comcodes = []

for comm in xmls:
    code = comm['committee_membership']['committees']['committee_code']
    name = comm['committee_membership']['committees']['committee_name']
    members = comm['committee_membership']['committees']['members']
    comcodes.append({'code': code, 'name': name, 'parent': code})

    for sen in members['member']:
        mname = sen['name']
        pos = sen['position']
        try:
            if pos == 'Member':
                cmembership[sen_coms.loc[(sen_coms['firstname'] == mname['first']) & (sen_coms['lastname'] == mname['last'])].index[0]] += [f"{code}_{congress_session}"]
            else:
                cleadership[sen_coms.loc[(sen_coms['firstname'] == mname['first']) & (sen_coms['lastname'] == mname['last'])].index[0]] += [f"{code}_{congress_session}"]
        except IndexError:
            print(mname)
            
    try:
        subcommittees = comm['committee_membership']['committees']['subcommittee']
    except KeyError:
        subcommittees = []
    
    for subc in subcommittees:
        scode = subc['committee_code']
        name = subc['subcommittee_name']
        members = subc['members']
        comcodes.append({'code': scode, 'name': name, 'parent': code})
        
        if members:
            for sen in members['member']:
                mname = sen['name']
                pos = sen['position']
                try:
                    if pos == 'Member':
                        cmembership[sen_coms.loc[(sen_coms['firstname'] == mname['first']) & (sen_coms['lastname'] == mname['last'])].index[0]] += [f"{scode}_{congress_session}"]
                    else:
                        cleadership[sen_coms.loc[(sen_coms['firstname'] == mname['first']) & (sen_coms['lastname'] == mname['last'])].index[0]] += [f"{scode}_{congress_session}"]
                except IndexError:
                    print(mname)

m = []
class_map = {'Class I': '-Sen1', 'Class II': '-Sen2', 'Class III': '-Sen3'}
for elem in xml_data1['contact_information']['member']:
    new = {'state': elem['state'] + class_map[elem['class']],
           'firstname': elem['first_name'],
           'lastname': elem['last_name'],
           'party': elem['party'],
           'committee_leadership': cleadership[elem['bioguide_id']],
           'committee_membership': cmembership[elem['bioguide_id']],
           'UID': elem['bioguide_id'],
          }
    m.append(new)

cdf = pd.DataFrame(m)
cdf.to_csv(f'data/Congress/{congress_session}_Senators.tsv', sep='\t', index=False)

comcodes = pd.DataFrame(comcodes)
comcodes.to_csv(f'data/Congress/{congress_session}_SenateCommittees.tsv', sep='\t', index=False)
