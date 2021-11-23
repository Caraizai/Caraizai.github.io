########################################################################################################################

#---------- Code used to download and merge files needed for FARS, GES and CRSS ------------------------------------####
#---------- during the period of study -----------------------------------------------------------------------------####
#---------- written by Carlos Andrés Araiza Iturria, for questions contact carlos_araiza02@hotmail.com --------------####

########################################################################################################################

# SET the path for the files to be saved in you comupter here:
# The 'Auxiliary files' folder need to be stored in this path
path = '/Users/caraizai/Documents/1st Paper/'
# Define 'Period of study' to download data from these years:
iniyear = 2001
endyear = 2019

# Packages needed

import pandas as pd
import numpy as np
import pyreadstat
import requests
import zipfile
import os
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

# This will create a path to Download files in zip format from the NHTSA website
# for the period of study.
# The FARS databases will be stored in a specific folder called 'FARS'
path_fars = path + 'FARS/'
# The GES and/or CRSS databases will be stored in a specific folder called 'GESCRSS'
path_gescrss = path + 'GESCRSS/'
os.makedirs(path_fars)
os.makedirs(path_gescrss)
path_aux = path + 'Auxiliary files/'
NHTSA_url = 'https://www.nhtsa.gov/file-downloads/download?p=nhtsa/downloads/'
########################################################################################################################
#--------------------------------------------- FARS ----------------------------------------------------------------####
########################################################################################################################

############ Download and extract .zip files from NHTSA website
for year in range(iniyear,endyear+1):
    download_url(NHTSA_url+'FARS/'+str(year)+'/National/FARS'+str(year)+'NationalCSV.zip',
    save_path=path_fars+'FARS'+str(year)+'NationalCSV.zip')
    with zipfile.ZipFile(path_fars+'FARS'+str(year)+'NationalCSV.zip','r') as zip_ref:
        zip_ref.extractall(path_fars+'FARS'+str(year)+'NationalCSV')

################## Merge individual files into a single database for the period "iniyear" to "endyear" ##############
########################################################################################################################

# Merge the Person files
# Start the merge process using the last year of data in period of study
person = pd.read_csv(path_fars+'FARS'+str(endyear)+'NationalCSV/Person.CSV', encoding='latin-1')
# Variables desired for analysis from the person files
list_per = ['ST_CASE','VEH_NO','PER_NO','AGE','SEX','YEAR','PER_TYP','INJ_SEV','SEAT_POS','DRINKING',
            'DRUGS','HISPANIC','RACE']
# Add a new variable with the corresponding year in order to make annual distinctions
person['YEAR'] = endyear
# Extract a subset of database: a smaller database with only the variables from 'list_per' is created.
# In order for the extraction to work the user needs to confirm all variables from 'list_per' are available
# with the appropriate name.
if(endyear>=2019):
    # The recording of 'Race' is in a separate file starting from 2019
    race = pd.read_csv(path_fars+'FARS'+str(endyear)+'NationalCSV/Race.CSV', encoding='latin-1')
    # Merge the person and race files through their unique identification key
    person['KEY'] = person.ST_CASE.astype(np.str) + person.VEH_NO.astype(np.str) + person.PER_NO.astype(np.str)
    race['KEY'] = race.ST_CASE.astype(np.str) + race.VEH_NO.astype(np.str) + race.PER_NO.astype(np.str)
    race = race.loc[race.RACE==1][[*['KEY', 'RACENAME']]]
    race = race.rename(columns={'RACENAME': 'RACE'})
    person = mergeLeftInOrder(x=person, y=race, on="KEY")
    df_per = person[[*list_per]]
else:
    df_per = person[[*list_per]]
for year in range(endyear-1,iniyear-1,-1):
    person = pd.read_csv(path_fars+'FARS'+str(year)+'NationalCSV/Person.CSV', encoding='latin-1')
    person['YEAR'] = year
    person = person[[*list_per]]
    df_per = df_per.append(person)
#End of merge for the Person files

# Merge the Vehicle files
# Start the merge process using the last year of data in period of study
vehicle = pd.read_csv(path_fars+'FARS'+str(endyear)+'NationalCSV/Vehicle.CSV', encoding='latin-1')
vehicle['YEAR'] = endyear
vehicle = vehicle.rename(columns = {'PREV_SUS1':'PREV_SUS'})
list_vec = ['ST_CASE','VEH_NO','YEAR','NUMOCCS','MAKE', 'MODEL', 'MOD_YEAR', 'HIT_RUN', 'BODY_TYP',
            'DEFORMED','IMPACT1','SPEC_USE', 'TRAV_SP', 'DEATHS', 'DR_DRINK', 'DR_ZIP' ,'L_TYPE', 'CDL_STAT',
            'PREV_ACC', 'PREV_SUS', 'PREV_DWI', 'PREV_SPD','SPEEDREL', 'DR_SF1']
df_vec = vehicle[[*list_vec]]
for year in range(endyear-1,iniyear-1,-1):
    vehicle = pd.read_csv(path_fars+'FARS'+str(year)+'NationalCSV/Vehicle.CSV', encoding='latin-1')
    vehicle = vehicle.rename(columns={'PREV_SUS1': 'PREV_SUS'})
    # DR_CF1 1997-2009, DR_SF1 2010-Later
    vehicle = vehicle.rename(columns={'DR_CF1': 'DR_SF1'})
    vehicle = vehicle.rename(columns={'OCUPANTS': 'NUMOCCS'})
    if year<=2008:
        vehicle['SPEEDREL'] = np.nan
    if year<=2003:
        vehicle['L_TYPE'] = np.nan
    vehicle['YEAR'] = year
# Extract a subset of database: a smaller database with only the variables from 'list_vec' is created.
    vehicle = vehicle[[*list_vec]]
    df_vec = df_vec.append(vehicle)

# Merge the Accident files
# Start the merge process using the last year of data in period of study
accident = pd.read_csv(path_fars+'FARS'+str(endyear)+'NationalCSV/Accident.csv', encoding='latin-1')
list_acc = ['ST_CASE', 'STATENAME', 'YEAR', 'COUNTY', 'HARM_EV', 'HOUR', 'WEATHER', 'CF1', 'FATALS']
df_acc = accident[[*list_acc]]
# Before 2016 the state number was used instead of STATENAME. We use this list to relace the numerical value,
# with the state number
state_name = pd.read_csv(path_aux + 'Statenumbers.CSV')
state_name['STATE'] = state_name.STATE.astype(np.str)
for year in range(endyear-1,iniyear-1,-1):
    accident = pd.read_csv(path_fars+'FARS'+str(year)+'NationalCSV/Accident.CSV', encoding='latin-1')
    if year < 2016:
        accident['STATE'] = accident.STATE.astype(np.str)
        accident = mergeLeftInOrder(x=accident, y=state_name, on="STATE")
# Extract a subset of database: a smaller database with only the variables from 'list_acc' is created.
    accident = accident[[*list_acc]]
    df_acc = df_acc.append(accident)

# Save each database with the desired variables into 3 separate files:
# 1. For all the accidents,
# 2. For all the vehicles,
# 3. For all the people.

df_acc.to_csv(path_fars+'1 Accident_'+str(iniyear)+'-'+str(endyear)+'.csv')
df_vec.to_csv(path_fars+'2 Vehicle_'+str(iniyear)+'-'+str(endyear)+'.csv')
df_per.to_csv(path_fars+'3 Person_'+str(iniyear)+'-'+str(endyear)+'.csv')

# Create a unique file with the selected variables for the period "iniyear" to "endyear"
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
df.to_csv(path_fars + '0 AccVecPer_'+str(iniyear)+'-'+str(endyear)+'.csv')

########################################################################################################################
#--------------------------------------------- GES/CRSS ------------------------------------------------------------####
########################################################################################################################

############ Download and extract .zip files from NHTSA website
for year in range(iniyear,endyear+1):
    SOURCE = 'GES'
    if year>=2016:
        SOURCE = 'CRSS'
        EXTname = SOURCE + str(year) + 'CSV'
        EXT = 'CSV'
    elif year == 2015:
        EXTname = SOURCE + str(year) + 'csv'
        EXT = 'CSV'
    elif year == 2014:
        EXTname = SOURCE + str(year) + 'flat'
        EXT = 'TXT'
    elif (year >= 2011) & (year <= 2013):
        EXTname = SOURCE + str(year)[-2:] + '_Flatfile'
        EXT = 'TXT'
    elif year == 2010:
        EXTname = SOURCE + str(year)[-2:] + '_flatfile'
        EXT = 'TXT'
    elif year==2009:
        EXTname = SOURCE + str(year)[-2:] + ' FlatFile'
        EXT = 'TXT'
    elif (year>=2007)&(year<=2008):
        EXTname = SOURCE + str(year) + ''
        EXT = 'SAS'
    elif (year>=2005)&(year<=2006):
        EXTname = 'SAS/'+ SOURCE + str(year)
        EXT = 'SAS'
    elif year == 2004:
        EXTname = SOURCE + str(year)[-2:]
        EXT = 'SAS'
    elif (year >= 2002) & (year <= 2003):
        EXTname = 'SAS/'+ SOURCE + str(year)[-2:]
        EXT = 'SAS'
    elif year == 2001:
        # WARNING: For Year==2001, the datafile type is .sd2, this is an old SAS data format.
        # As far as we know, Python is unable to read or convert .sd2 files.
        # We suggest converting the .sd2 file to a .CSV type using SPSS
        EXTname = 'SAS/Ges' + str(year)[-2:]
        EXT = 'SAS'
    if year >= 2016:
        url = NHTSA_url + SOURCE +'/' + str(year) + '/'+ EXTname +'.zip'
    else:
        url = NHTSA_url + SOURCE+'/' + SOURCE + str(year)[-2:] + '/' + EXTname +'.zip'
    # Download and extract zip file
    download_url(url, save_path=path_gescrss + SOURCE + str(year) + EXT +'.zip')
    with zipfile.ZipFile(path_gescrss + SOURCE + str(year) + EXT +'.zip', 'r') as zip_ref:
        zip_ref.extractall(path_gescrss + SOURCE + str(year) + EXT)

################## Merge individual files into a single database for the period "iniyear" to "endyear" ##############

# Merge the Person files
# Start the merge process using the last year of data in period of study
person = pd.read_csv(path_gescrss+'CRSS'+str(endyear)+'CSV/person.csv', encoding='latin-1')
# Variables desired for analysis from the person files
list_per = ['CASENUM','VEH_NO','PER_NO','AGE','SEX','YEAR','SOURCE','PER_TYP','INJ_SEV','SEAT_POS','DRINKING','DRUGS']
# Add a new variable with the corresponding YEAR in order to make annual distinctions
person['YEAR'] = endyear
# Add a new variable with the corresponding SOURCE in order to make sample distinctions
# Change from 'CRSS' to 'GES' if (endyear<2016)
person['SOURCE'] = 'CRSS'
# Extract a subset of database: a smaller database with only the variables from 'list_per' is created.
# In order for the extraction to work the user needs to confirm all variables from 'list_per' are available
# with the appropriate name.
df_per = person[[*list_per]]

# WARNING: For Year==2001, the datafile type is .sd2, this is an old SAS data format.
# As far as we know, Python is unable to read or convert .sd2 files.
# We suggest converting the .sd2 file to a .CSV type using SPSS
for year in range(endyear-1,iniyear-1,-1):
    if year>=2016:
        person = pd.read_csv(path_gescrss + 'CRSS' + str(year) + 'CSV/Person.CSV', encoding='latin-1')
    elif year==2015:
        person = pd.read_csv(path_gescrss + 'GES' + str(2015) + 'CSV/Person.CSV', encoding='latin-1')
    elif year == 2010:
        person = pd.read_csv(path_gescrss + 'GES' + str(year) + 'TXT/TXT/Person.txt', delimiter="\t")
    elif (year>=2009)&(year<=2014):
        person = pd.read_csv(path_gescrss + 'GES' + str(year) + 'TXT/Person.txt', delimiter="\t")
    elif (year>=2002)&(year<=2008):
        person = pyreadstat.read_sas7bdat(path_gescrss + 'GES' + str(year) + 'SAS/person.sas7bdat')[0]
    elif year==2001:
        # WARNING: For Year==2001, the datafile .sd2 was converted to .csv with SPSS
        person = pd.read_csv(path_gescrss + 'GES2001SAS/Person.CSV', encoding='latin-1')
    if year>=2016:
        person['SOURCE'] = 'CRSS'
    else:
        person['SOURCE'] = 'GES'
    person['YEAR'] = year
    person = person.rename(columns={'ï»¿CASENUM': 'CASENUM'})
    person = person.rename(columns={'PERNO': 'PER_NO'})
    # SAS Name: PERNO 1988-2010, PER_NO 2011-Later
    person = person.rename(columns={'PER_DRUG': 'DRUGS'})
    # SAS Name: PER_DRUG 1990-2010, DRUGS 2011-Later
    person = person.rename(columns={'VEHNO': 'VEH_NO'})
    # SAS Name: VEHNO 1988-2010, VEH_NO 2011-Later
    person = person.rename(columns={'PER_TYPE': 'PER_TYP'})
    # SAS Name: PER_TYPE 1988-2008, PER_TYP 2009-Later
    person = person.rename(columns={'PER_ALCH': 'DRINKING'})
    # SAS Name: PER_ALCH 1988-2010, DRINKING 2011-Later
    person = person[[*list_per]]
    df_per = df_per.append(person)

# Merge the Vehicle files
# Start the merge process using the last year of data in period of study
vehicle = pd.read_csv(path_gescrss+'CRSS'+str(endyear)+'CSV/vehicle.csv', encoding='latin-1')
vehicle['YEAR'] = endyear
list_vec = ['CASENUM','VEH_NO','YEAR','NUMOCCS','MAKE', 'MODEL', 'MOD_YEAR', 'HIT_RUN', 'BODY_TYP',
            'DEFORMED','IMPACT1','SPEC_USE', 'TRAV_SP', 'DR_ZIP','SPEEDREL', 'DR_SF1']
df_vec = vehicle[[*list_vec]]
for year in range(endyear-1,iniyear-1,-1):
    if year>=2016:
        vehicle = pd.read_csv(path_gescrss + 'CRSS'+str(year)+'CSV/vehicle.CSV', encoding='latin-1')
    elif year==2015:
        vehicle = pd.read_csv(path_gescrss + 'GES' + str(2015) + 'CSV/vehicle.CSV',encoding='latin-1')
    elif year == 2010:
        vehicle = pd.read_csv(path_gescrss + 'GES' + str(year) + 'TXT/TXT/vehicle.txt', delimiter="\t")
    elif (year>=2009)&(year<=2014):
        vehicle = pd.read_csv(path_gescrss + 'GES'+ str(year)+'TXT/vehicle.txt', delimiter = "\t")
    elif (year>=2002)&(year<=2008):
        vehicle = pyreadstat.read_sas7bdat(path_gescrss + 'GES'+ str(year)+'SAS/vehicle.sas7bdat')[0]
    elif year==2001:
        vehicle = pd.read_csv(path_gescrss + 'GES2001SAS/vehicle.CSV',encoding='latin-1')
    if ('DR_SF1' not in vehicle.columns):
        vehicle['DR_SF1'] = np.nan
    vehicle = vehicle.rename(columns={'ï»¿CASENUM': 'CASENUM'})
    vehicle = vehicle.rename(columns={'DR_ZIP_C': 'DR_ZIP'})
    vehicle = vehicle.rename(columns={'DZIPCODE': 'DR_ZIP'})
    vehicle = vehicle.rename(columns={'MODEL_YR': 'MOD_YEAR'})
    vehicle = vehicle.rename(columns={'VEHNO': 'VEH_NO'})
    vehicle = vehicle.rename(columns={'IMPACT': 'IMPACT1'})
    vehicle = vehicle.rename(columns={'VEH_SEV': 'DEFORMED'})
    vehicle = vehicle.rename(columns={'SPEED': 'TRAV_SP'})
    vehicle['YEAR'] = year
    vehicle = vehicle[[*list_vec]]
    df_vec = df_vec.append(vehicle)

# Merge the Accident files
# Start the merge process using the last year of data in period of study
accident = pd.read_csv(path_gescrss+'CRSS'+str(endyear)+'CSV/accident.csv', encoding='latin-1')
accident['YEAR'] = endyear
list_acc = ['CASENUM','YEAR','HARM_EV','HOUR','WEATHER','CF1','STRATUM', 'REGION', 'PSU','PJ','WEIGHT']
df_acc = accident[[*list_acc]]
for year in range(endyear-1,iniyear-1,-1):
    if year>=2016:
        accident = pd.read_csv(path_gescrss + 'CRSS'+str(year)+'CSV/accident.CSV', encoding='latin-1')
    elif year==2015:
        accident = pd.read_csv(path_gescrss + 'GES' + str(2015) + 'CSV/accident.CSV',encoding='latin-1')
    elif year == 2010:
        accident = pd.read_csv(path_gescrss + 'GES' + str(year) + 'TXT/TXT/accident.txt', delimiter="\t")
    elif (year>=2009)&(year<=2014):
        accident = pd.read_csv(path_gescrss + 'GES'+ str(year)+'TXT/accident.txt', delimiter = "\t")
    elif (year>=2002)&(year<=2008):
        accident = pyreadstat.read_sas7bdat(path_gescrss + 'GES'+ str(year)+'SAS/accident.sas7bdat')[0]
    elif year == 2001:
        accident = pd.read_csv(path_gescrss + 'GES2001SAS/accident.CSV',encoding='latin-1')
    if ('CF1' not in accident.columns):
        accident['CF1'] = np.nan
    accident = accident.rename(columns={'EVENT1': 'HARM_EV'})
    accident = accident.rename(columns={'ï»¿CASENUM': 'CASENUM'})
    accident['YEAR'] = year
    accident = accident[[*list_acc]]
    df_acc = df_acc.append(accident)

# Save each database with the desired variables into 3 separate files:
# 1. For all the accidents,
# 2. For all the vehicles,
# 3. For all the people.

df_acc.to_csv(path_gescrss + '1 Accident_'+str(iniyear)+'-'+str(endyear)+'.csv')
df_vec.to_csv(path_gescrss + '2 Vehicle_'+str(iniyear)+'-'+str(endyear)+'.csv')
df_per.to_csv(path_gescrss + '3 Person_'+str(iniyear)+'-'+str(endyear)+'.csv')

# Create a unique file with the selected variables for the period "iniyear" to "endyear"
# The Person file is used as the base, then, we add the vehicle information and the accident information.

# CASENUM and VEH_NO should be used to merge the Person data file with the Vehicle data file
df_per['KEY'] = df_per.YEAR.astype(np.str) + df_per.CASENUM.astype(np.str) + df_per.VEH_NO.astype(np.str)
df_vec['KEY'] = df_vec.YEAR.astype(np.str) + df_vec.CASENUM.astype(np.str) + df_vec.VEH_NO.astype(np.str)
df = mergeLeftInOrder(x=df_per,y=df_vec, on="KEY")

# Redundant variables are created, delete
del df['CASENUM_y'], df['VEH_NO_y'], df['YEAR_y']
# Rename the original variables
df = df.rename(columns={'CASENUM_x': 'CASENUM'})
df = df.rename(columns={'VEH_NO_x': 'VEH_NO'})
df = df.rename(columns={'YEAR_x': 'YEAR'})

# Merge the Accident file to the 'Person + Vehicle' file.
df['KEY'] = df.YEAR.astype(np.str) + df.CASENUM.astype(np.str)
df_acc['KEY'] = df_acc.YEAR.astype(np.str) + df_acc.CASENUM.astype(np.str)
df = mergeLeftInOrder(x=df,y=df_acc, on="KEY")

# Redundant variables are created, delete
# Delete 'Key' variable
del df['CASENUM_y'], df['YEAR_y'], df['KEY']
# Rename the original variables
df = df.rename(columns={'CASENUM_x': 'CASENUM'})
df = df.rename(columns={'YEAR_x': 'YEAR'})

# Save the unique file with selected variables for the period "iniyear" to "endyear"
df.to_csv(path_gescrss + '0 AccVecPer_'+str(iniyear)+'-'+str(endyear)+'.csv')

########################################################################################################################
#-------------------------- END OF CODE FOR DATABASE CONSTRUCTION --------------------------------------------------####
########################################################################################################################
