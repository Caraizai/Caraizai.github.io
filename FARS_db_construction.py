########################################################################################################################

#---------- Code used to download and merge all files needed for fatal accidents ------------------------------------###
#---------- Then, the filtering criteria are defined below to finish the database construction ----------------------###
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

############ (Step 1) Download files from NHTSA website
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
del df['ST_CASE_y'], df['YEAR_y'], df['KEY']
# Rename the original variables
df = df.rename(columns={'ST_CASE_x': 'ST_CASE'})
df = df.rename(columns={'YEAR_x': 'YEAR'})

# Save the unique file with all relevant variables for the period "iniyear" to "endyear"
df.to_csv(path[:-4] + 'AccVecPer_2001-2019.csv')

############################## FILTERING PROCESS ##############################
df = pd.read_csv('/Users/carlosaraiza/Canada/PhD UWaterloo/PhD Thesis/FARS/AccVecPer_2001-2019.csv', encoding='latin-1')
del df['Order']
# 1,653,760 people
df.__len__()
# 654,078 MVTA
df['KEY'] = df.YEAR.astype(np.str) + df.ST_CASE.astype(np.str)
df.KEY.nunique()

# We create a variable that counts number of vehicles involved in each accident.
# This variable will help in the filtering process.
df['NUM_VEH'] = 1
list_vehno = df.VEH_NO.unique()
list_vehno = list_vehno[list_vehno>0]
for i in list_vehno:
    df.loc[df.VEH_NO==i,'NUM_VEH'] = i
    df.loc[df['KEY'].isin(df[df.NUM_VEH==i]['KEY']),'NUM_VEH'] = i

# 1. Remove accidents where only 1 vehicle is involved and that vehicle is a truck,
# atv, snowmobile, golf cart, a motorcycle, or unknown (346,782)
#np.sum(df['KEY'].isin(df.loc[(df.BODY_TYP>=11)&(df.BODY_TYP!=17)&(df.NUM_VEH==1)&(df.VEH_NO==1),'KEY']))
df = df[~df['KEY'].isin(df.loc[(df.BODY_TYP>=11)&(df.BODY_TYP!=17)&(df.NUM_VEH==1)&(df.VEH_NO==1),'KEY'])]
# Accidents where all the vehicles involved are non-standard cars
df_temp = df.loc[df['KEY'].isin(df.loc[(df.NUM_VEH>1)&(df.BODY_TYP>=11)&(df.BODY_TYP!=17),'KEY']),['NUM_VEH','KEY','BODY_TYP']]
df_temp['test'] = (df_temp['BODY_TYP']<11)|(df_temp['BODY_TYP']==17)
temp = df_temp.groupby('KEY')['test'].sum()
list_keys_to_remv = temp[temp==0].index
np.sum(df['KEY'].isin(list_keys_to_remv.to_list())) # (255,219)
df = df[~df['KEY'].isin(list_keys_to_remv.to_list())]
# Current Length: 948,447

#1. Accidents where someone died prior to crash (139)
list_del = df.loc[(df.INJ_SEV==6),'KEY'].unique()
np.sum(df['KEY'].isin(list_del))
df = df.loc[~df['KEY'].isin(list_del)]
df.__len__()
df.KEY.nunique()





# 2. Remove: those that have num_veh = 1 and they are in a vehicle (VEH_NO=1) + driver's license is commercial,
# Commercial license from values 1 to 9 (79,546)
np.sum(df['KEY'].isin(df.loc[(df.NUM_VEH==1)&(df.VEH_NO==1)&(df.CDL_STAT>=1)&(df.CDL_STAT<=9),'KEY']))
df = df[~df['KEY'].isin(df.loc[(df.NUM_VEH==1)&(df.VEH_NO==1)&(df.CDL_STAT>=1)&(df.CDL_STAT<=9),'KEY'])]
# Accidents with more than 1 vehicle and all drivers have commercial licenses
df_temp = df.loc[df['KEY'].isin(df.loc[(df.NUM_VEH>1)&(df.PER_TYP==1)&(df.CDL_STAT>=1)&(df.CDL_STAT<=9),'KEY']),['NUM_VEH','KEY','CDL_STAT','PER_TYP','VEH_NO']]
df_temp['test'] = df_temp.loc[df_temp.PER_TYP==1,'CDL_STAT']==0
temp = df_temp.groupby('KEY')['test'].sum()
list_keys_to_remv = temp[temp==0].index
np.sum(df['KEY'].isin(list_keys_to_remv.to_list())) #23,766
df = df[~df['KEY'].isin(list_keys_to_remv.to_list())]
# Current Length: 1,550,448



# 4. Commercial types using special use
# just 1 vehicle (1,320)
np.sum(df['KEY'].isin(df.loc[(df.SPEC_USE>=1)&(df.SPEC_USE<=24)&(df.NUM_VEH==1)&(df.VEH_NO==1),'KEY']))
df = df[~df['KEY'].isin(df.loc[(df.SPEC_USE>=1)&(df.SPEC_USE<=24)&(df.NUM_VEH==1)&(df.VEH_NO==1),'KEY'])]
# more than 1 vehicle
df_temp = df.loc[df['KEY'].isin(df.loc[(df.NUM_VEH>1)&(df.SPEC_USE>=1)&(df.SPEC_USE<=24),'KEY']),['NUM_VEH','KEY','SPEC_USE']]
df_temp['test'] = (df_temp.SPEC_USE==0)
temp = df_temp.groupby('KEY')['test'].sum()
list_keys_to_remv = temp[temp==0].index
np.sum(df['KEY'].isin(list_keys_to_remv.to_list())) # (83)
df = df[~df['KEY'].isin(list_keys_to_remv.to_list())]
# Current Length: 947,044


df.to_csv('/Users/carlosaraiza/Canada/PhD UWaterloo/PhD Thesis/FARS/AccVecPer_filtered_v2_2001-2019.csv')
df = pd.read_csv('/Users/carlosaraiza/Canada/PhD UWaterloo/PhD Thesis/FARS/AccVecPer_filtered_v2_2001-2019.csv', encoding='latin-1')
df.KEY.nunique()
#334,399






