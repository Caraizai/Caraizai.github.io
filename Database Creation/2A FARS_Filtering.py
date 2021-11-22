####################################### FILTERING PROCESS ##############################################################

#---------- After the FARS database construction using '1A FARS_Construction,py' ------------------------------------###
#---------- Here, the filtering process for personal automobile insurance is done ------------------------------------###
#---------- written by Carlos Andés Araiza Iturria, for questions contact carlos_araiza02@hotmail.com --------------####

########################################################################################################################

# Packages needed

import pandas as pd
import numpy as np

# Read data, set the path for your computer
path = '/Users/carlosaraiza/Canada/PhD UWaterloo/PhD Thesis/FARS/'
df = pd.read_csv(path + 'AccVecPer_2001-2019.csv', encoding='latin-1')
del df['Order']

# Create 'KEY' as unique identifier for each MVTA
df['KEY'] = df.YEAR.astype(np.str) + df.ST_CASE.astype(np.str)

# 'NUM_VEH' is a variable that counts the number of vehicles involved in each MVTA to aid in the filtering process
df['NUM_VEH'] = 1
list_vehno = df.VEH_NO.unique()
list_vehno = list_vehno[list_vehno>0]
for i in list_vehno:
    df.loc[df.VEH_NO==i,'NUM_VEH'] = i
    df.loc[df['KEY'].isin(df[df.NUM_VEH==i]['KEY']),'NUM_VEH'] = i

# Filter #1: Remove MVTA where the vehicle involved is a 'non-standard car' such as a truck, atv, snowmobile,
# golf cart, a motorcycle, or unknown (just 1 vehicle)
df = df[~df['KEY'].isin(df.loc[(df.BODY_TYP>=11)&(df.BODY_TYP!=17)&(df.NUM_VEH==1)&(df.VEH_NO==1),'KEY'])]
# More than 1 vehicle
df_temp = df.loc[df['KEY'].isin(df.loc[(df.NUM_VEH>1)&(df.BODY_TYP>=11)&(df.BODY_TYP!=17),'KEY']),['NUM_VEH','KEY','BODY_TYP']]
df_temp['test'] = (df_temp['BODY_TYP']<11)|(df_temp['BODY_TYP']==17)
temp = df_temp.groupby('KEY')['test'].sum()
list_keys_to_remv = temp[temp==0].index
df = df[~df['KEY'].isin(list_keys_to_remv.to_list())]

# Filter #2: Remove commercial types using special use (just 1 vehicle)
df = df[~df['KEY'].isin(df.loc[(df.SPEC_USE>=1)&(df.SPEC_USE<=24)&(df.NUM_VEH==1)&(df.VEH_NO==1),'KEY'])]
# More than 1 vehicle
df_temp = df.loc[df['KEY'].isin(df.loc[(df.NUM_VEH>1)&(df.SPEC_USE>=1)&(df.SPEC_USE<=24),'KEY']),['NUM_VEH','KEY','SPEC_USE']]
df_temp['test'] = (df_temp.SPEC_USE==0)
temp = df_temp.groupby('KEY')['test'].sum()
list_keys_to_remv = temp[temp==0].index
df = df[~df['KEY'].isin(list_keys_to_remv.to_list())]

# Filter #3: Remove MVTA where driver's license is commercial. (just 1 vehicle)
df = df[~df['KEY'].isin(df.loc[(df.NUM_VEH==1)&(df.VEH_NO==1)&(df.CDL_STAT>=1)&(df.CDL_STAT<=9),'KEY'])]
# More than 1 vehicle
df_temp = df.loc[df['KEY'].isin(df.loc[(df.NUM_VEH>1)&(df.PER_TYP==1)&(df.CDL_STAT>=1)&(df.CDL_STAT<=9),'KEY']),['NUM_VEH','KEY','CDL_STAT','PER_TYP','VEH_NO']]
df_temp['test'] = df_temp.loc[df_temp.PER_TYP==1,'CDL_STAT']==0
temp = df_temp.groupby('KEY')['test'].sum()
list_keys_to_remv = temp[temp==0].index
df = df[~df['KEY'].isin(list_keys_to_remv.to_list())]

# Filter #4: Accidents where inj_sev is not reported or unknown for all doesn't apply. If the accident
# is in FARS it means someone died. i.e. at least someone has INJ_SEV=4

# Filter #5: Accidents where someone died prior to crash
list_del = df.loc[(df.INJ_SEV==6),'KEY'].unique()
df = df.loc[~df['KEY'].isin(list_del)]

# Save the Filtered FARS database
df.to_csv(path + 'AccVecPer_filtered_2001-2019.csv')
