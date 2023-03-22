import pandas as pd
import geopandas as gpd

yr = 2021
data_dir = 'data/NCES'

hd_cols = ['instnm', 'ialias', 'addr', 'city', 'stabbr', 'zip', 'longitud', 'latitude',
           'control', 'groffer', 'hbcu', 'locale', 
           'c21basic', 'c21ipgrd', 'c21enprf', 'c21szset']

effy_cols = ['efytotlt', 'efytotlm', 'efytotlw', 'efynralt', 'efynralm', 'efynralw']

effy_dist_cols = ['efydetot', 'efydeexc', 'efydesom', 'efydenon']

c_cols = ['unitid', 'awlevel', 
          'ctotalt','ctotalm','ctotalw',
          'cnralt', 'cnralm', 'cnralw  ',
         ]


df_effy = pd.read_csv(f'{data_dir}/effy{yr}_data_stata.csv')
df_effy = df_effy[df_effy['effylev'] == 'Graduate']
df_effy.set_index('unitid', inplace=True)
df_effy = df_effy[effy_cols]

df_effy_dist = pd.read_csv(f'{data_dir}/effy{yr}_dist_data_stata.csv')
df_effy_dist = df_effy_dist[df_effy_dist['effydlev'] == 'Graduate']
df_effy_dist.set_index('unitid', inplace=True)
df_effy_dist = df_effy_dist[effy_dist_cols]
df_effy_dist.fillna(0, inplace=True)
df_effy_dist = df_effy_dist.astype(int)

df_effy = df_effy.join(df_effy_dist)

df_c = pd.read_csv(f'{data_dir}/c{yr}_a_data_stata.csv')
df_c = df_c[(df_c['awlevel'].isin(["Master's degree", "Doctor's degree - research/scholarship"])) & 
            (df_c['cipcode'] == 'Grand total') & 
            (df_c['majornum'] == 'First major')]
df_c = df_c[c_cols]
msdf_c = df_c[df_c['awlevel'] == "Master's degree"].set_index('unitid').drop(columns='awlevel').add_prefix('ms_')
drdf_c = df_c[df_c['awlevel'] == "Doctor's degree - research/scholarship"].set_index('unitid').drop(columns='awlevel').add_prefix('dr_')
jdf_c = msdf_c.join(drdf_c).fillna(0)
jdf_c.fillna(0, inplace=True)
jdf_c = jdf_c.astype(int)
df_effy = df_effy.join(jdf_c)
df_effy.fillna(0, inplace=True)

df_hd = pd.read_csv(f'{data_dir}/hd{yr}_data_stata.csv')
df_hd.set_index('unitid', inplace=True)
df_hd = df_hd[hd_cols]
df_hd = df_hd.loc[df_effy.index]

df_hd = df_hd.astype(str)
df_effy = df_effy.astype(int)
df = df_hd.join(df_effy)

df.reset_index(inplace=True)

colnam_map = {
    'unitid'   : 'IPEDS Unique ID',
    'instnm'   : 'Institution',
    'ialias'   : 'Alias', 
    'addr'     : 'Address', 
    'city'     : 'City', 
    'stabbr'   : 'State', 
    'zip'      : 'ZIP code', 
    'longitud' : 'Longitude',
    'latitude' : 'Latitude',
    'control'  : 'Control', 
    'groffer'  : 'Offers graduate program', 
    'hbcu'     : 'HBCU', 
    'locale'   : 'Locale', 
    'c21basic' : 'Carnegie Classification 2021: Basic', 
    'c21ipgrd' : 'Carnegie Classification 2021: Graduate Instructional Program', 
    'c21enprf' : 'Carnegie Classification 2021: Enrollment Profile', 
    'c21szset' : 'Carnegie Classification 2021: Size and Setting',
    'efytotlt' : 'Grand total', 
    'efytotlm' : 'Grand total men', 
    'efytotlw' : 'Grand total women', 
    'efynralt' : 'Nonresident alien total',
    'efynralm' : 'Nonresident alien men',
    'efynralw' : 'Nonresident alien women',
    'efydetot' : 'Total enrolled graduate students', 
    'efydeexc' : 'Exclusively distance education', 
    'efydesom' : 'Some distance education', 
    'efydenon' : 'No distance education',
    'ms_ctotalt'  : 'Grand total (MS)',
    'ms_ctotalm'  : 'Grand total men (MS)',
    'ms_ctotalw'  : 'Grand total women (MS)',
    'ms_cnralt'   : 'Nonresident alien total (MS)',
    'ms_cnralm'   : 'Nonresident alien men (MS)',
    'ms_cnralw  ' : 'Nonresident alien women (MS)',
    'dr_ctotalt'  : 'Grand total (DR)',
    'dr_ctotalm'  : 'Grand total men (DR)',
    'dr_ctotalw'  : 'Grand total women (DR)',
    'dr_cnralt'   : 'Nonresident alien total (DR)',
    'dr_cnralm'   : 'Nonresident alien men (DR)',
    'dr_cnralw  ' : 'Nonresident alien women (DR)',
}

df.rename(columns=colnam_map, inplace=True)

rdf = gpd.read_file('data/Census/cd118.shp')
uldf = gpd.GeoDataFrame(df['IPEDS Unique ID'], geometry=gpd.points_from_xy(df.Longitude, df.Latitude)).set_crs("EPSG:4326")

dfj = gpd.sjoin(uldf, rdf, how='left')

udf = pd.merge(df, dfj[['IPEDS Unique ID', 'CD118FP']])
udf['Congressional District (118th)'] = udf['State'] + udf['CD118FP']
udf.drop(columns=['CD118FP'], inplace=True)

udf.to_csv('data/UniversitiesTable.csv', index=False)
