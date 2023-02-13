import urllib3
import xmltodict
import pandas as pd

url = r'https://clerk.house.gov/xml/lists/MemberData.xml'
fileName = 'data/Congress/MemberData.xml'
with urllib3.PoolManager() as http:
    r = http.request('GET', url)
    with open(fileName, 'wb') as fout:
        fout.write(r.data)

with open(fileName, 'r') as f:
    xml_data = xmltodict.parse(f.read())

m = []
for elem in xml_data['MemberData']['members']['member']:
    new = {'district': elem['statedistrict'],
           'firstname': elem['member-info']['firstname'],
           'lastname': elem['member-info']['lastname'],
           'party': elem['member-info']['party'],
          }
    m.append(new)

cdf = pd.DataFrame(m)

cdf.to_csv('data/Congress/HouseReps.csv', index=False)

url = r'https://www.senate.gov/general/contact_information/senators_cfm.xml'
fileName = 'data/Congress/senators_cfm.xml'
with urllib3.PoolManager() as http:
    r = http.request('GET', url)
    with open(fileName, 'wb') as fout:
        fout.write(r.data)

with open(fileName, 'r') as f:
    xml_data = xmltodict.parse(f.read())

m = []
class_map = {'Class I': '-Sen1', 'Class II': '-Sen2', 'Class III': '-Sen3'}
for elem in xml_data['contact_information']['member']:
    new = {'state': elem['state'] + class_map[elem['class']],
           'firstname': elem['first_name'],
           'lastname': elem['last_name'],
           'party': elem['party'],
          }
    m.append(new)

cdf = pd.DataFrame(m)
cdf.to_csv('data/Congress/Senators.csv', index=False)