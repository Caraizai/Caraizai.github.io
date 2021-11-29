####################################### SAMPLING PROCEDURE #############################################################

#------ After the FARS and GES/CRSS database construction, filtering and standardization, using ---------------------###
#-------'1 FARS_GESCRSS_Construction.py', '2 FARS_GESCRSS_Filteering.py' and '3 FARS_GESCRSS_Standardizing.py'. -----###
#---- This code samples from both the GES/CRSS and FARS databases and merges the data into a final ------------------###
#---- database ready for statistical analysis. This procedure is based on the pooling method validated in  ----------###
#---- https://www.sciencedirect.com/science/article/abs/pii/S0001457515300427 ---------------------------------------###

#---------- written by Carlos Andrés Araiza Iturria, for questions contact carlos_araiza02@hotmail.com -------------####

########################################################################################################################

################################################ INPUT SECTION #########################################################
# SET the path for the folder where the 'FARS' and 'GESCRSS' folders were stored in '1A FARS_GESCRSS_Construction.py'
# The 'Auxiliary files' folder needs to be available in 'path'
path = '/Users/carlosaraiza/Test/'
# Initial and last years in period of study:
iniyear = 2001
endyear = 2019
########################################################################################################################

# Packages needed
import pandas as pd
import numpy as np

# Read the filtered and standardized databases
path_fars = path + 'FARS/'
path_gescrss = path + 'GESCRSS/'
df = pd.read_csv(path_gescrss + '0 AccVecPer_Filtered_Standardized_'+str(iniyear)+'-'+str(endyear)+'.csv',
                 encoding='latin-1', low_memory=False)
df_fars = pd.read_csv(path_fars + '0 AccVecPer_Filtered_Standardized_'+str(iniyear)+'-'+str(endyear)+'.csv',
                      encoding='latin-1', low_memory=False)
del df['Order'], df_fars['Order']

########################################################################################################################
########################################### Pooling procedure ##########################################################
########################################################################################################################

# Step 1. From GES/CRSS, sample without replacement from each maximum severity to match exactly the NHTSA weights.
cases = []
# Sample size, as a percentage
ssize = 0.55
for year in range(iniyear,endyear+1):
    # In a given year, frequency, as a percentage of each maximum severity by MVTA
    p4 = df.loc[(df.YEAR==year)&(df.MAX_SEV==4)].groupby('ST_CASE')['WEIGHT'].first().sum() / df.loc[(df.YEAR==year)].groupby('ST_CASE')['WEIGHT'].first().sum()
    p3 = df.loc[(df.YEAR==year)&(df.MAX_SEV==3)].groupby('ST_CASE')['WEIGHT'].first().sum() / df.loc[(df.YEAR==year)].groupby('ST_CASE')['WEIGHT'].first().sum()
    p2 = df.loc[(df.YEAR==year)&(df.MAX_SEV==2)].groupby('ST_CASE')['WEIGHT'].first().sum() / df.loc[(df.YEAR==year)].groupby('ST_CASE')['WEIGHT'].first().sum()
    p1 = df.loc[(df.YEAR==year)&(df.MAX_SEV==1)].groupby('ST_CASE')['WEIGHT'].first().sum() / df.loc[(df.YEAR==year)].groupby('ST_CASE')['WEIGHT'].first().sum()
    p0 = df.loc[(df.YEAR==year)&(df.MAX_SEV==0)].groupby('ST_CASE')['WEIGHT'].first().sum() / df.loc[(df.YEAR==year)].groupby('ST_CASE')['WEIGHT'].first().sum()

    p_dist = np.array([p0,p1,p2,p3,p4])
    # Total MVTA in a given year by MAX_SEV
    cases_grouped = df.loc[df.YEAR==year].groupby('ST_CASE')['MAX_SEV'].first()
    n = cases_grouped.__len__()
    # Number of MVTA needed to match the weights percentage
    n_p_dist = np.round(p_dist*n*ssize)

    # Sampling begins for GES/CRSS
    for i in [0,1,2,3,4]:
        prob_dist = list(df.loc[(df.YEAR==year)&(df.MAX_SEV==i)].groupby('ST_CASE')['WEIGHT'].first() / np.sum(df.loc[(df.YEAR==year)&(df.MAX_SEV==i)].groupby('ST_CASE')['WEIGHT'].first()))
        # Change the seed '2020' to obtain different samples
        rng = np.random.default_rng(2020)
        # If 'ssize' is to large, this test will print the year for which the sample size doees't work and therefore
        # needs to be set to a smaller number.
        test = np.sum(cases_grouped==i)>int(n_p_dist.tolist()[i])
        if test:
            temp = rng.choice(cases_grouped[cases_grouped == i].index.to_list(), int(n_p_dist.tolist()[i]), p=prob_dist, replace = False)
            temp = temp.tolist()
            cases.append(temp)
        else:
            print(year)

cases = [item for items in cases for item in items]
df_temp = df.loc[df['ST_CASE'].isin(cases)]

########################################################################################################################
# Step 2. Remove fatal MVTA from GESCRSS and replace with a sample from the FARS database
# An appropriate weight is added to keep the sample representative of the national scale.

# Remove from GESCRSS those with MAX_SEV = 4, i.e. MVTA that have a fatal injury.
cases_fatal = df_temp.loc[df_temp.MAX_SEV==4].groupby('ST_CASE')[['MAX_SEV']].first().index.tolist()
df_replace = df_temp.loc[~df_temp['ST_CASE'].isin(cases_fatal)]

# Sample from the FARS database.
n_replace = []
fars_cases = []
df_fars['KEY'] = df_fars.YEAR.astype(str) + df_fars.ST_CASE.astype(str)
# Sample from the FARS dataset
for year in range(iniyear,endyear+1):
    n = df_fars.loc[df_fars.YEAR==year]['ST_CASE'].unique().__len__()
    n_replace.append(df_temp.loc[(df_temp.YEAR==year)&(df_temp.MAX_SEV==4)]['ST_CASE'].unique().__len__())
    prob_dist = np.repeat(1,n)/n
    # Change the seed '2020' to obtain different samples
    rng = np.random.default_rng(2020)
    temp = rng.choice(df_fars.loc[df_fars.YEAR==year]['KEY'].unique().tolist(),
                      int(n*ssize), p=prob_dist.tolist(), replace=False)
    temp = temp.tolist()
    fars_cases.append(temp)
fars_cases = [item for items in fars_cases for item in items]
df_temp_fars = df_fars.loc[df_fars['KEY'].isin(fars_cases)]

# Add appropriate weight to sampled cases from the FARS database
df_temp_fars['WEIGHT'] = 0
for i, year in enumerate(range(iniyear,endyear+1)):
    mj = n_replace[i]
    nj = df_temp_fars.loc[df_temp_fars.YEAR==year,'ST_CASE'].unique().__len__()
    df_temp_fars.loc[df_temp_fars.YEAR==year,'WEIGHT'] = mj/nj
df_temp_fars.loc[:,'SOURCE'] = 'FARS'

# Merge of the FARS sample with the GESCRSS sample.
# Add appropriate weight to sampled cases from the GES/CRSS database
df_final = pd.concat([df_replace, df_temp_fars], axis=0)
df_final.loc[(df_final.SOURCE=='CRSS')|(df_final.SOURCE=='GES'),'WEIGHT'] = 1

del df_final['KEY']
# Save the GESCRSS and FARS resulting sample.
df_final.to_csv(path + 'FARS_GESCRSS_Sample_'+str(iniyear)+'-'+str(endyear)+'.csv')

########################################################################################################################
#-------------------------- END OF CODE FOR DATABASE SAMPLING --------------------------------------------------####
########################################################################################################################


