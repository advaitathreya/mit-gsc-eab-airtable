import requests
import zipfile
import io
import pandas as pd

yr = 2021
data_files = [f'HD{yr}', f'EFFY{yr}', f'EFFY{yr}_DIST', f'C{yr}_A']

ipeds_locs = 'https://nces.ed.gov/ipeds/datacenter/data/'
ipeds_fils = '{}_Data_Stata.zip'
ipeds_dict = '{}_Dict.zip'
save_dir = 'data/NCES/'

for ds in data_files:
    print('GETTING FILES FOR {}'.format(ds))
    rdata = requests.get(ipeds_locs + ipeds_fils.format(ds))
    rdict = requests.get(ipeds_locs + ipeds_dict.format(ds))
    rdata_zip = zipfile.ZipFile(io.BytesIO(rdata.content))
    rdict_zip = zipfile.ZipFile(io.BytesIO(rdict.content))

    print('Extracting files from zip archive:')
    rdata_zip.printdir()
    rdict_zip.printdir()
    rdata_zip.extractall(path=save_dir)
    rdict_zip.extractall(path=save_dir)

    # print('Saving zip archive to disk.')
    # open(save_dir + ipeds_fils.format(ds), 'wb').write(rdata.content)
    # open(save_dir + ipeds_dict.format(ds), 'wb').write(rdict.content)

    print('Replacing Code Values with Code Labels.')

    # Extract frequencies tab the data dictionary (hdYYYY.xlsx)
    freqs = pd.read_excel(save_dir + '{}.xlsx'.format(ds.lower()),
                          sheet_name='Frequencies', dtype=object)
    # Put institutional data into a data frame (df)
    df = pd.read_csv(save_dir + '{}_data_stata.csv'.format(ds.lower()), 
                     encoding='ISO-8859-1')    

    # Get list of categorical variable names
    cat_colms = set(freqs['varname'])

    # Loop through categorical columns
    for col in cat_colms:
        if col == 'STABBR': continue
        # Get map keys (code values)
        code_values = freqs[freqs['varname'] == col]['codevalue']
        if col == 'CIPCODE': code_values = code_values.str.replace('.', '', regex=False)
        # Convert map keys to int where appropriate
        code_values = [int(i) if str(i).isdigit() 
                       else i for i in code_values]
        # Get map value (ValueLabels)
        code_labels = freqs[freqs['varname'] == col]['valuelabel']
        var_map = dict(zip(code_values, code_labels)) 
        # Apply mapping dictionary to categorical column
        df[col] = df[col].map(var_map)

    print('Writing {}_data_stata.csv'.format(ds.lower()))
    df.columns = [i.lower() for i in df.columns]
    df.to_csv(save_dir + '{}_data_stata.csv'.format(ds.lower()), index=False)
    print('Done!', end='\n\n')
