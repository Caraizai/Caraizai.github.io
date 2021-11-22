########################################################################################################################

#---------- Code used to download and merge all files needed for fatal accidents ------------------------------------###
#---------- written by Carlos Andés Araiza Iturria, for questions contact carlos_araiza02@hotmail.com --------------####

########################################################################################################################

# Packages needed

import pandas as pd
import numpy as np
import requests
pd.options.mode.chained_assignment = None  # default='warn'

# Functions needed

# Function to merge without changing order of rows
def mergeLeftInOrder(x, y, on=None):
    x = x.copy()
    x["Order"] = np.arange(len(x))
    z = x.merge(y, how='left', on=on).set_index("Order").iloc[np.arange(len(x)), :]
    return z
# Function to download from url
def download_url(url, save_path, chunk_size=128):
    r = requests.get(url, stream=True)
    with open(save_path, 'wb') as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            fd.write(chunk)

############ Download files from NHTSA website
# set the path for the files to be saved in you comupter here:
path = '/Users/carlosaraiza/Canada/PhD UWaterloo/PhD Thesis/FARS/FARS'
# Define which years to download:
iniyear = 2001
endyear = 2019

# Download files in zip format from the NHTSA website for the period of study.
for i in range(iniyear,endyear+1):
    download_url('https://www.nhtsa.gov/file-downloads/download?p=nhtsa/downloads/FARS/'+str(i)+'/National/FARS'+str(i)+'NationalCSV.zip',
    save_path= path + str(i) +'NationalCSV.zip')

################## Merge individual files into a single database for the period "iniyear" to "endyear" ##############
# To continue, the zip files downloaded in the step above need to be unzipped, allowing Python to read the files.


# Merge the Person files
# Start the merge process using the last year of data in period of study
person = pd.read_csv(path +str(endyear)+'NationalCSV/Person.CSV', encoding='latin-1')
# Variables desired for analysis from the person files
list_per = ['ST_CASE','VEH_NO','PER_NO','AGE','SEX','YEAR','PER_TYP','INJ_SEV','SEAT_POS','DRINKING',
            'DRUGS','HISPANIC','RACE']
# Add a new variable with the corresponding year in order to make annual distinctions
person['YEAR'] = endyear
# Extract a subset of database: a smaller database with only the variables from 'list_per' is created.
# In order for the extraction to work the user needs to confirm all variables from 'list_per' are available
# with the appropriate name.
df_per = person[[*list_per]]
for year in range(endyear-1,iniyear-1,-1):
    print(year)
    person = pd.read_csv(path +str(year)+'NationalCSV/Person.CSV', encoding='latin-1')
    person['YEAR'] = year
    person = person[[*list_per]]
    df_per = df_per.append(person)
#End of merge for the Person files

# Merge the Accident files
# Start the merge process using the last year of data in period of study
accident = pd.read_csv(path +str(endyear)+'NationalCSV/Accident.csv', encoding='latin-1')
df_acc = accident
for year in range(endyear-1,iniyear-1,-1):
    print(year)
    accident = pd.read_csv(path +str(year)+'NationalCSV/Accident.CSV', encoding='latin-1')
    df_acc = df_acc.append(accident)
list_acc = ['ST_CASE','STATE','STATENAME','YEAR','COUNTY','HARM_EV','HOUR','WEATHER','CF1','FATALS']
# Extract a subset of database: a smaller database with only the variables from 'list_acc' is created.
df_acc = df_acc[[*list_acc]]

# Merge the Vehicle files
# Start the merge process using the last year of data in period of study
vehicle = pd.read_csv(path +str(endyear)+'NationalCSV/Vehicle.CSV', encoding='latin-1')
vehicle['YEAR'] = endyear
vehicle = vehicle.rename(columns = {'PREV_SUS1':'PREV_SUS'})
list_vec = ['ST_CASE','VEH_NO','YEAR','NUMOCCS','MAKE', 'MODEL', 'MOD_YEAR', 'HIT_RUN', 'BODY_TYP',
            'DEFORMED','IMPACT1','SPEC_USE', 'TRAV_SP', 'DEATHS', 'DR_DRINK', 'DR_ZIP' ,'L_TYPE', 'CDL_STAT',
            'PREV_ACC', 'PREV_SUS', 'PREV_DWI', 'PREV_SPD','SPEEDREL', 'DR_SF1']
df_vec = vehicle[[*list_vec]]
for year in range(endyear-1,iniyear-1,-1):
    print(year)
    vehicle = pd.read_csv(path +str(year)+'NationalCSV/Vehicle.CSV', encoding='latin-1')
    vehicle = vehicle.rename(columns={'PREV_SUS1': 'PREV_SUS'})
    # DR_CF1 1997-2009, DR_SF1 2010-Later
    vehicle = vehicle.rename(columns={'DR_CF1': 'DR_SF1'})
    vehicle = vehicle.rename(columns={'OCUPANTS': 'NUMOCCS'})
    if year<=2008:
        vehicle['SPEEDREL'] = np.nan
    if year<=2003:
        vehicle['L_TYPE'] = np.nan
    vehicle['YEAR'] = year
    vehicle = vehicle[[*list_vec]]
    df_vec = df_vec.append(vehicle)
# Extract a subset of database: a smaller database with only the variables from 'list_vec' is created.

# Save each database with the desired variables into 3 separate files:
# 1. For all the accidents,
# 2. For all the vehicles,
# 3. For all the people.

df_acc.to_csv(path[:-4] + 'Accidents_2001-2019.csv')
df_vec.to_csv(path[:-4] + 'Vehicle_2001-2019.csv')
df_per.to_csv(path[:-4] + 'Person_2001-2019.csv')

# Create a unique file with all relevant variables for the period "iniyear" to "endyear"
# The Person file is used as the base, then, we add the vehicle information and the accident information.

# Merge the person and vehicle files through their unique identification key
df_per['KEY'] = df_per.YEAR.astype(np.str) + df_per.ST_CASE.astype(np.str) + df_per.VEH_NO.astype(np.str)
df_vec['KEY'] = df_vec.YEAR.astype(np.str) + df_vec.ST_CASE.astype(np.str) + df_vec.VEH_NO.astype(np.str)
df = mergeLeftInOrder(x=df_per,y=df_vec, on="KEY")

# Redundant variables are created, delete
del df['ST_CASE_y'], df['VEH_NO_y'], df['YEAR_y']
# Rename the original variables
df = df.rename(columns={'ST_CASE_x': 'ST_CASE'})
df = df.rename(columns={'VEH_NO_x': 'VEH_NO'})
df = df.rename(columns={'YEAR_x': 'YEAR'})

# Merge the Accident file to the 'Person + Vehicle' file.
df['KEY'] = df.YEAR.astype(np.str) + df.ST_CASE.astype(np.str)
df_acc['KEY'] = df_acc.YEAR.astype(np.str) + df_acc.ST_CASE.astype(np.str)
df = mergeLeftInOrder(x=df,y=df_acc, on="KEY")

# Redundant variables are created, delete
# Delete 'Key' variable
del df['ST_CASE_y'], df['YEAR_y'], df['KEY']
# Rename the original variables
df = df.rename(columns={'ST_CASE_x': 'ST_CASE'})
df = df.rename(columns={'YEAR_x': 'YEAR'})

# Save the unique file with all relevant variables for the period "iniyear" to "endyear"
df.to_csv(path[:-4] + 'AccVecPer_2001-2019.csv')

