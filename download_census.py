import ftplib
import geopandas as gpd
import pandas as pd
import tqdm

FTP_IP = 'ftp2.census.gov'
FTP_DIR = '/geo/tiger/TIGER_RD18/STATE/'

g = []
with ftplib.FTP(FTP_IP) as ftp:
    ftp.login()
    ls = ftp.nlst(FTP_DIR)
    for state in tqdm.tqdm(ls):
        state_fips = state.split('/')[-1][:2]
        FILENAME = f"{state}/{state_fips}/tl_rd22_{state_fips}_cd118.zip"
        with open(f'data/Census/states/tl_rd22_{state_fips}_cd118.zip', 'wb') as f:
            ftp.retrbinary('RETR ' + FILENAME, f.write)
        with open(f'data/Census/states/tl_rd22_{state_fips}_cd118.zip', 'rb') as f:
            g.append(gpd.read_file(f).to_crs("EPSG:4326"))
            
rdf = gpd.GeoDataFrame(pd.concat(g, ignore_index=True), crs=g[0].crs)

rdf.to_file("data/Census/cd118.shp")