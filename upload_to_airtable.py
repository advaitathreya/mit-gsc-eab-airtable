import pandas as pd
import yaml
import airtable_helper as ah

udf = pd.read_csv('data/UniversitiesTable.csv')
hdf = pd.read_csv('data/Congress/HouseReps.csv')
sdf = pd.read_csv('data/Congress/Senators.csv')

# non-voting reps are designated '98' in NCES but '00' in House Clerk database
udf.loc[udf['Congressional District (118th)'].str[-2:] == '98', 'Congressional District (118th)'] \
= udf['Congressional District (118th)'].str[-2:] + '00'

with open('airtable_keys.yaml', 'r') as f:
    airtable_keys = yaml.safe_load(f)

def get_record_keys(upl_table_name='', prim_key_col='',
                    api_key=airtable_keys['API']['PAT'], base_id=airtable_keys['Bases']['GSC EAB']):
    
    tab_dict = ah.airtable_download(airtable_keys['Tables'][upl_table_name], api_key=api_key, base_id=base_id)
    tab_df = ah.convert_to_dataframe(tab_dict)

    if len(tab_df) > 0:
        try:
            return tab_df.reset_index().set_index(prim_key_col)['index'].to_dict()
        except TypeError:
            tab_df[prim_key_col] = tab_df[prim_key_col].str[0]
            return tab_df.reset_index().set_index(prim_key_col)['index'].to_dict()
    else:
        return {}


def create_and_upload_df(dataframe, data_idx_col='',
                         upl_table_name='', prim_key_col='',
                         rename_columns_dict={}, drop_columns=[],
                         linked_records={},
                         set_column_vals={},
                         upload=True,
                         api_key=airtable_keys['API']['PAT'], base_id=airtable_keys['Bases']['GSC EAB']):
    
    u_record_id_dict = get_record_keys(upl_table_name, prim_key_col)

    idf = dataframe.copy()
    idf[data_idx_col] = idf[data_idx_col].astype(str)
    idf['Index'] = idf[data_idx_col].map(u_record_id_dict)
    m = idf['Index'].isnull()
    new = m.sum()
    old = len(idf) - new
    idf.loc[m,'Index'] = [f'newrecord{i}' for i in range(new)]
    idf.set_index('Index', inplace=True)

    idf.rename(columns=rename_columns_dict, inplace=True)
    idf.drop(columns=drop_columns, inplace=True)
    for k, v in set_column_vals.items():
        idf[k] = v
        
    for col, opt in linked_records.items():
        c_record_id_dict = get_record_keys(opt[0], opt[1])
        c_record_id_dict = {k: c_record_id_dict[k] for k in c_record_id_dict.keys()}        
        idf[col] = idf[col].apply(lambda row: [c_record_id_dict[v] for v in (row if type(row) == list else [row]) if c_record_id_dict.get(v)])

    idf = idf.replace('', None)

    if upload:
        print(f'Updating {old} records, adding {new} new records to {upl_table_name} table.')
        ah.upload_pandas_dataframe(idf, airtable_keys['Tables'][upl_table_name], api_key=api_key, base_id=base_id)
    
    return idf

print('Uploading institution names')

print('    Universities')

_ = create_and_upload_df(dataframe=udf[['IPEDS Unique ID', 'Institution']], data_idx_col='IPEDS Unique ID',
                         upl_table_name='Institutions', prim_key_col='UID',  
                         rename_columns_dict={'Institution': 'Name', 'IPEDS Unique ID': 'UID'},
                         set_column_vals={'Type': 'University'}
                        )

print('    House Districts')

idf = hdf[['district']].copy()
idf['UID'] = 'CD118' + idf['district'].astype(str)

_ = create_and_upload_df(dataframe=idf, data_idx_col='UID',
                         upl_table_name='Institutions', prim_key_col='UID',  
                         rename_columns_dict={'district': 'Name'},
                         set_column_vals={'Type': 'Legislative Office - House Rep'}
                        )

print('    Senate Seats')

idf = sdf[['state']].copy()
idf['UID'] = idf['state'].astype(str)

_ = create_and_upload_df(dataframe=idf, data_idx_col='UID',
                         upl_table_name='Institutions', prim_key_col='UID',  
                         rename_columns_dict={'state': 'Name'},
                         set_column_vals={'Type': 'Legislative Office - Senator'}
                        )

print('Uploading people names')

print('    House Representatives')

idf = hdf[['district', 'firstname', 'lastname']].copy()
idf['Name'] = idf['firstname'].astype(str) + ' ' + idf['lastname'].astype(str)

_ = create_and_upload_df(dataframe=idf, data_idx_col='Name',
                         upl_table_name='People', prim_key_col='Name',  
                         rename_columns_dict={'firstname': 'First Name', 'lastname': 'Last Name', 'district': 'Institutions'},
                         drop_columns=['Name'],
                         linked_records={'Institutions': ['Institutions', 'Name']},
                         set_column_vals={'Role': 'Elected Member'},
                        )

print('    Senators')

idf = sdf[['state', 'firstname', 'lastname']].copy()
idf['Name'] = idf['firstname'].astype(str) + ' ' + idf['lastname'].astype(str)

_ = create_and_upload_df(dataframe=idf, data_idx_col='Name',
                         upl_table_name='People', prim_key_col='Name',  
                         rename_columns_dict={'firstname': 'First Name', 'lastname': 'Last Name', 'state': 'Institutions'},
                         drop_columns=['Name'],
                         linked_records={'Institutions': ['Institutions', 'Name']},
                         set_column_vals={'Role': 'Elected Member'},
                        )

print('Uploading Congress data')

print('    House Representatives')

idf = hdf.copy()
idf['Member'] = idf['firstname'].astype(str) + ' ' + idf['lastname'].astype(str)
idf.drop(columns=['firstname', 'lastname'], inplace=True)

_ = create_and_upload_df(dataframe=idf, data_idx_col='district',
                         upl_table_name='Congress', prim_key_col='Name',
                         rename_columns_dict={'district': 'District', 'party': 'Party'},
                         linked_records={'Member': ['People', 'Name'],
                                         'District': ['Institutions', 'Name'],
                                        },
                         set_column_vals={'Chamber': 'House'}
                        )

print('    Senators')

idf = sdf.copy()
idf['Member'] = idf['firstname'].astype(str) + ' ' + idf['lastname'].astype(str)
idf.drop(columns=['firstname', 'lastname'], inplace=True)

_ = create_and_upload_df(dataframe=idf, data_idx_col='state',
                         upl_table_name='Congress', prim_key_col='Name',
                         rename_columns_dict={'state': 'District', 'party': 'Party'},
                         linked_records={'Member': ['People', 'Name'],
                                         'District': ['Institutions', 'Name'],
                                        },
                         set_column_vals={'Chamber': 'Senate'}
                        )

print('Uploading university data')

idf = udf.copy()
idf['Senate Seats'] = pd.Series(dtype=object)
for i in range(len(idf)):
    idf.at[i, 'Senate Seats'] = list(sdf[sdf['state'].str[:2] == idf.loc[i, 'State']]['state'])

idf.loc[idf['HBCU'] == 'Yes', 'HBCU'] = True
idf.loc[idf['HBCU'] == 'No', 'HBCU'] = False

_ = create_and_upload_df(dataframe=idf, data_idx_col='IPEDS Unique ID',
                         upl_table_name='Universities', prim_key_col='IPEDS Unique ID',
                         rename_columns_dict={'Institution': 'Institutions'},
                         linked_records={'Congressional District (118th)': ['Congress', 'Name'],
                                         'Senate Seats': ['Congress', 'Name'],
                                         'Institutions': ['Institutions', 'Name'],
                                        },     
                         drop_columns=['IPEDS Unique ID']
                        )

