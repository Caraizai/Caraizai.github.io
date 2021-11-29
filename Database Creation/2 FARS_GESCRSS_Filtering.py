####################################### FILTERING PROCESS ##############################################################

#---------- After the FARS and GES/CRSS database construction using '1A FARS_GESCRSS_Construction.py'----------------###
#---------- This code has a filtering process to do research for personal automobile insurance  ---------------------###

#---------- written by Carlos Andrés Araiza Iturria, for questions contact carlos_araiza02@hotmail.com --------------####

########################################################################################################################

################################################ INPUT SECTION #########################################################
# SET the path for the folder where the 'FARS' and 'GESCRSS' folders were stored in '1A FARS_GESCRSS_Construction.py'
path = '/Users/carlosaraiza/Test/'
# Initial and last years in period of study:
iniyear = 2001
endyear = 2019
########################################################################################################################

# Packages needed
import pandas as pd
import numpy as np

########################################################################################################################
#--------------------------------------------- FARS ----------------------------------------------------------------####
########################################################################################################################

# Read the merged FARS database for the period of study
path_fars = path + 'FARS/'
df = pd.read_csv(path_fars + '0 AccVecPer_'+str(iniyear)+'-'+str(endyear)+'.csv', encoding='latin-1', low_memory=False)
del df['Order']

# Create 'KEY' as unique identifier for each MVTA
df['KEY'] = df.YEAR.astype(str) + df.ST_CASE.astype(str)

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
df_temp = df.loc[df['KEY'].isin(df.loc[(df.NUM_VEH>1)&(df.PER_TYP==1)&(df.CDL_STAT>=1)&(df.CDL_STAT<=9),'KEY']),
                 ['NUM_VEH','KEY','CDL_STAT','PER_TYP','VEH_NO']]
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
df = df.reset_index()
del df['index'], df['KEY']
df.to_csv(path_fars + '0 AccVecPer_filtered_'+str(iniyear)+'-'+str(endyear)+'.csv')

########################################################################################################################
#--------------------------------------------- GES/CRSS ------------------------------------------------------------####
########################################################################################################################

path_gescrss = path + 'GESCRSS/'
df = pd.read_csv(path_gescrss + '0 AccVecPer_'+str(iniyear)+'-'+str(endyear)+'.csv', encoding='latin-1',
                 low_memory=False)
del df['Order']

# Create 'KEY' as unique identifier for each MVTA
df['KEY'] = df.YEAR.astype(str) + df.CASENUM.astype(str)

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
df_temp = df.loc[df['KEY'].isin(df.loc[(df.NUM_VEH>1)&(df.BODY_TYP>=11)&(df.BODY_TYP!=17),'KEY']),
                 ['NUM_VEH','KEY','BODY_TYP']]
df_temp['test'] = (df_temp['BODY_TYP']<11)|(df_temp['BODY_TYP']==17)
temp = df_temp.groupby('KEY')['test'].sum()
list_keys_to_remv = temp[temp==0].index
df = df[~df['KEY'].isin(list_keys_to_remv.to_list())]

# Filter #2: Remove commercial types using special use (just 1 vehicle)
df = df[~df['KEY'].isin(df.loc[(df.SPEC_USE>=1)&(df.SPEC_USE<=13)&(df.NUM_VEH==1)&(df.VEH_NO==1),'KEY'])]
# More than 1 vehicle
df_temp = df.loc[df['KEY'].isin(df.loc[(df.NUM_VEH>1)&(df.SPEC_USE>=1)&(df.SPEC_USE<=13),'KEY']),
                 ['NUM_VEH','KEY','SPEC_USE']]
df_temp['test'] = (df_temp.SPEC_USE==0)
temp = df_temp.groupby('KEY')['test'].sum()
list_keys_to_remv = temp[temp==0].index
df = df[~df['KEY'].isin(list_keys_to_remv.to_list())]

# Filter #3: Remove MVTA where driver's license is commercial. (just 1 vehicle)
# The commercial license variable is not available for GES/CRSS

# Filter #4: Cases where all people involved have INJ=SEV 9 will be removed (not reported or unknown)
# In 2010, those with injury severity not reporte were coded with 7.
df['temp'] = ((df.INJ_SEV==9)|((df.YEAR==2010)&(df.INJ_SEV==7))).astype(int)
df['constant'] = 1
df_temp = df.groupby(['KEY'])[['temp','constant']].sum()
list_del = df_temp.loc[df_temp.temp==df_temp.constant].index
df = df.loc[~df['KEY'].isin(list_del)]

# Filter #5: Accidents where someone died prior to crash
list_del = df.loc[(df.INJ_SEV==6),'KEY'].unique()
df = df.loc[~df['KEY'].isin(list_del)]

# Save the GESCRSS FARS database
del df['temp'], df['KEY'], df['constant']
df = df.reset_index()
del df['index']
df.to_csv(path_gescrss + '0 AccVecPer_filtered_'+str(iniyear)+'-'+str(endyear)+'.csv')

########################################################################################################################
#-------------------------- END OF CODE FOR DATABASE FILTERING --------------------------------------------------####
########################################################################################################################
