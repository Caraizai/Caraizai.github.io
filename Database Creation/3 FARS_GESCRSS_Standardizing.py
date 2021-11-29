####################################### CODING STANDARDIZING ###########################################################

#------ After the FARS and GES/CRSS database construction and filtering, using '1 FARS_GESCRSS_Construction.py'------###
#------- and '2 FARS_GESCRSS_Filteering.py'.  This code homogenises the historical coding practice  -----------------###
#-------- considering the period 2001-2019 to the coding practice of 2019. ------------------------------------------###
#-------- For FARS, the 2019 coding practice can be consulted in  ---------------------------------------------------###
#-------- https://crashstats.nhtsa.dot.gov/Api/Public/ViewPublication/813023 ----------------------------------------###
#-------- For GES, the last coding practice can be consulted in  ----------------------------------------------------###
#-------- https://crashstats.nhtsa.dot.gov/Api/Public/ViewPublication/813022 ----------------------------------------###
#-------- For CRSS, the 2019 coding practice can be consulted in  ---------------------------------------------------###
#-------- https://crashstats.nhtsa.dot.gov/Api/Public/ViewPublication/812320 ----------------------------------------###

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
pd.options.mode.chained_assignment = None  # default='warn'

# Functions needed

# Function to merge without changing order of rows
def mergeLeftInOrder(x, y, on=None):
    x = x.copy()
    x["Order"] = np.arange(len(x))
    z = x.merge(y, how='left', on=on).set_index("Order").iloc[np.arange(len(x)), :]
    return z

########################### Standardize variable codings for GES/CRSS and FARS #########################################
# Read the filtered databases
path_fars = path + 'FARS/'
path_gescrss = path + 'GESCRSS/'
df = pd.read_csv(path_gescrss + '0 AccVecPer_filtered_'+str(iniyear)+'-'+str(endyear)+'.csv', encoding='latin-1',
                 low_memory=False)
del df['Unnamed: 0']
df_fars = pd.read_csv(path_fars + '0 AccVecPer_filtered_'+str(iniyear)+'-'+str(endyear)+'.csv', encoding='latin-1',
                      low_memory=False)
del df_fars['Unnamed: 0']
# Set the path to read the auxiliary file
path_aux = path + 'Auxiliary files/'

# Note for all variables: Those with unknown or not reported numerical values are replace with NaN (np.nan in Python)
#------------------------------------------ ST_CASE -------------------------------------------------------------------#
# The name of the ID variable for each MVTA in GES/CRSS is CASENUM, we change the name to that of the ID name in FARS
df = df.rename(columns={'CASENUM': 'ST_CASE'})

#------------------------------------------ AGE -----------------------------------------------------------------------#
df_fars.loc[(df_fars.AGE==99)&(df_fars.YEAR<=2008),'AGE'] = np.nan
df_fars.loc[(df_fars.AGE==998)|(df_fars.AGE==999),'AGE'] = np.nan
df.loc[(df.AGE==998)|(df.AGE==999),'AGE'] = np.nan
# Note for FARS: 2001-2008 if the person was older than 97 they were categorized as 97.
df.loc[(df.AGE>97),'AGE'] = 97
df_fars.loc[(df_fars.AGE>97),'AGE'] = 97

#------------------------------------------ GENDER --------------------------------------------------------------------#
df = df.rename(columns={'SEX': 'GENDER'})
df_fars = df_fars.rename(columns={'SEX': 'GENDER'})
df_fars.loc[(df_fars.GENDER==8)|(df_fars.GENDER==9),'GENDER'] = np.nan
df.loc[(df.GENDER==7)|(df.GENDER==8)|(df.GENDER==9),'GENDER'] = np.nan

#------------------------------------------ PER_TYP -------------------------------------------------------------------#
df.loc[(df.PER_TYP==2)|(df.PER_TYP==9)|(df.PER_TYP==77),'PER_TYP'] = 2
df_fars.loc[(df_fars.PER_TYP==2)|(df_fars.PER_TYP==9),'PER_TYP'] = 2
df.loc[(df.PER_TYP==8)&(df.YEAR<=2004),'PER_TYP'] = 3
df.loc[(df.PER_TYP==7)&(df.YEAR>=2005)&(df.YEAR<=2008),'PER_TYP'] = 3
df_fars.loc[(df_fars.PER_TYP==4),'PER_TYP'] = 3
df.loc[(df.PER_TYP==4),'PER_TYP'] = 3
df.loc[(df.PER_TYP==7)&(df.YEAR>=2009),'PER_TYP'] = 6
df_fars.loc[(df_fars.PER_TYP==7),'PER_TYP'] = 6
# Codes 8 and 10 can be merged with 3 since its stationary people (working vehicle, transport device,
# or standing in buildings)
df.loc[(df.PER_TYP==8)|(df.PER_TYP==10),'PER_TYP'] = 3
df_fars.loc[(df_fars.PER_TYP==8)|(df_fars.PER_TYP==10)|(df_fars.PER_TYP==19),'PER_TYP'] = 3

#------------------------------------------ INJ_SEV -------------------------------------------------------------------#
# In 2010, those not reported were coded as 7
df.loc[(df.INJ_SEV==9)|(df.INJ_SEV==7),'INJ_SEV'] = np.nan
df_fars.loc[(df_fars.INJ_SEV==9)|(df_fars.INJ_SEV==8),'INJ_SEV'] = np.nan
# Randomly (uniformly) re-assign those with INJ_SEV 5 to 1,2 or 3
rng = np.random.default_rng(2020)
df.loc[df.INJ_SEV==5,'INJ_SEV'] = rng.choice([1, 2, 3],df.loc[df.INJ_SEV==5,'INJ_SEV'].__len__(),
                                             p=[1/3, 1/3, 1/3])
df_fars.loc[df_fars.INJ_SEV==5,'INJ_SEV'] = rng.choice([1, 2, 3],df_fars.loc[df_fars.INJ_SEV==5,'INJ_SEV'].__len__(),
                                                       p=[1/3, 1/3, 1/3])

#------------------------------------------ MAX_SEV -------------------------------------------------------------------#
# In the FARS database, by construction, the most serious injury is a fatal injury.
df['MAX_SEV'] = df.INJ_SEV
df.loc[(df.INJ_SEV.isna())|(df.MAX_SEV==6),'MAX_SEV'] = np.nan
list_maxsev = df.groupby('ST_CASE')['MAX_SEV'].max()
for i in [0,1,2,3,4]:
    df.loc[df['ST_CASE'].isin(list_maxsev[list_maxsev==i].index),'MAX_SEV'] = i

#------------------------------------------ DRINKING ------------------------------------------------------------------#
df_fars.loc[(df_fars.DRINKING==8)|(df_fars.DRINKING==9),'DRINKING'] = np.nan
df.loc[(df.DRINKING==8)|(df.DRINKING==9)|(df.DRINKING==7)|(df.DRINKING==6),'DRINKING'] = np.nan
df.loc[(df.DRINKING==1)&(df.YEAR<=2008),'DRINKING'] = 0
df.loc[(df.DRINKING==2)&(df.YEAR<=2008),'DRINKING'] = 1

#------------------------------------------ DRUGS ---------------------------------------------------------------------#
df_fars.loc[(df_fars.DRUGS==8)|(df_fars.DRUGS==9),'DRUGS'] = np.nan
df.loc[(df.DRUGS==8)|(df.DRUGS==9)|(df.DRUGS==7)|(df.DRUGS==6),'DRUGS'] = np.nan
df.loc[(df.DRUGS==1)&(df.YEAR<=2008),'DRUGS'] = 0
df.loc[(df.DRUGS==2)&(df.YEAR<=2008),'DRUGS'] = 1

#------------------------------------------ NUMOCCS -------------------------------------------------------------------#
df.loc[((df.NUMOCCS==97)&(df.SOURCE=='GES'))|(df.NUMOCCS==99)|(df.NUMOCCS==999),'NUMOCCS'] = np.nan
df_fars.loc[(df_fars.NUMOCCS==97)|(df_fars.NUMOCCS==98)|(df_fars.NUMOCCS==99),'NUMOCCS'] = np.nan

#------------------------------------------ MAKE ----------------------------------------------------------------------#
df_fars.loc[(df_fars.MAKE==97)|(df_fars.MAKE==99),'MAKE'] = np.nan
df.loc[(df.MAKE==97)|(df.MAKE==99),'MAKE'] = np.nan

#------------------------------------------ MODEL ---------------------------------------------------------------------#
# Any model higher than 397 is a 'non-standard' car (that would be utility vehicles,
# motorcycles, ATV, medium/heavy trucks/ buses or notreported/unknown)
# Since we are interested only in personal automobile insurance, these values are replaced with NaN.
df.loc[(df.MODEL>397),'MODEL'] = np.nan
df_fars.loc[(df_fars.MODEL>397),'MODEL'] = np.nan

# Volkswagen (MAKE 30), KIA (MAKE 63) and Oldsmobile (MAKE 21) have some differences in historical coding practice.
df.loc[(df.MAKE==30)&(df.MODEL==47)&(df.YEAR<=2010),'MODEL'] = 40
df.loc[(df.MAKE==30)&(df.MODEL==49)&(df.YEAR<=2010),'MODEL'] = 47
df.loc[(df.MAKE==30)&(df.MODEL==48)&(df.YEAR<=2010),'MODEL'] = 42
df.loc[(df.MAKE==30)&(df.MODEL==50)&(df.YEAR<=2010),'MODEL'] = 48
df.loc[(df.MAKE==63)&(df.MODEL==32)&(df.YEAR<=2010),'MODEL'] = 999 # temporary bucket
df.loc[(df.MAKE==63)&(df.MODEL==33)&(df.YEAR<=2010),'MODEL'] = 32
df.loc[(df.MAKE==63)&(df.MODEL==999)&(df.YEAR<=2010),'MODEL'] = 33.
df.loc[(df.MAKE==21)&(df.MODEL==24)&(df.YEAR<=2010),'MODEL'] = 21

#------------------------------------------ MAKEMODEL -----------------------------------------------------------------#
df['MAKEMODEL'] = np.nan
df.loc[~((df.MAKE.isna())|(df.MODEL.isna())),'MAKEMODEL'] = df[~((df.MAKE.isna())|(df.MODEL.isna()))].MAKE.astype('int').astype('str').str.zfill(2) +\
                                                            df[~((df.MAKE.isna())|(df.MODEL.isna()))].MODEL.astype('int').astype('str').str.zfill(3)
df_fars['MAKEMODEL'] = np.nan
df_fars.loc[~((df_fars.MAKE.isna())|(df_fars.MODEL.isna())),'MAKEMODEL'] = df_fars[~((df_fars.MAKE.isna())|(df_fars.MODEL.isna()))].MAKE.astype('int').astype('str').str.zfill(2) +\
                                                                           df_fars[~((df_fars.MAKE.isna())|(df_fars.MODEL.isna()))].MODEL.astype('int').astype('str').str.zfill(3)

#------------------------------------------ MOD_YEAR ------------------------------------------------------------------#
df.loc[(df.MOD_YEAR==9999)|(df.MOD_YEAR==9998)|(df.MOD_YEAR==7777),'MOD_YEAR'] = np.nan
df_fars.loc[(df_fars.MOD_YEAR==9999)|(df_fars.MOD_YEAR==9998),'MOD_YEAR'] = np.nan

#------------------------------------------ HIT_RUN -------------------------------------------------------------------#
df.loc[(df.HIT_RUN==9)|(df.HIT_RUN==8)|(df.HIT_RUN==7),'HIT_RUN'] = np.nan
df_fars.loc[(df_fars.HIT_RUN==9),'HIT_RUN'] = np.nan
df_fars.loc[(df_fars.HIT_RUN>=2),'HIT_RUN'] = 1

#------------------------------------------ BODY_TYP ------------------------------------------------------------------#
# Automobiles and auto-derivatives, with codes 1-13 and 17, are left as they are.
df.loc[(df.BODY_TYP==98)|(df.BODY_TYP==99),'BODY_TYP'] = np.nan
df.loc[(df.BODY_TYP>=14)&(df.BODY_TYP<=19)&(df.BODY_TYP!=17),'BODY_TYP'] = 15
df.loc[(df.BODY_TYP==17),'BODY_TYP'] = 14
df.loc[(df.BODY_TYP>=20)&(df.BODY_TYP<=29),'BODY_TYP'] = 16
df.loc[(df.BODY_TYP>=30)&(df.BODY_TYP<=49),'BODY_TYP'] = 17
df.loc[(df.BODY_TYP>=50)&(df.BODY_TYP<=59),'BODY_TYP'] = 18
df.loc[(df.BODY_TYP>=60)&(df.BODY_TYP<=79),'BODY_TYP'] = 19
df.loc[(df.BODY_TYP>=80)&(df.BODY_TYP<=89),'BODY_TYP'] = 20
df.loc[(df.BODY_TYP>=90)&(df.BODY_TYP<=97),'BODY_TYP'] = 21

df_fars.loc[(df_fars.BODY_TYP==98)|(df_fars.BODY_TYP==99),'BODY_TYP'] = np.nan
df_fars.loc[(df_fars.BODY_TYP>=14)&(df_fars.BODY_TYP<=19)&(df_fars.BODY_TYP!=17),'BODY_TYP'] = 15
df_fars.loc[(df_fars.BODY_TYP==17),'BODY_TYP'] = 14
df_fars.loc[(df_fars.BODY_TYP>=20)&(df_fars.BODY_TYP<=29),'BODY_TYP'] = 16
df_fars.loc[(df_fars.BODY_TYP>=30)&(df_fars.BODY_TYP<=49),'BODY_TYP'] = 17
df_fars.loc[(df_fars.BODY_TYP>=50)&(df_fars.BODY_TYP<=59),'BODY_TYP'] = 18
df_fars.loc[(df_fars.BODY_TYP>=60)&(df_fars.BODY_TYP<=79),'BODY_TYP'] = 19
df_fars.loc[(df_fars.BODY_TYP>=80)&(df_fars.BODY_TYP<=89),'BODY_TYP'] = 20
df_fars.loc[(df_fars.BODY_TYP>=90)&(df_fars.BODY_TYP<=97),'BODY_TYP'] = 21

#------------------------------------------ DEFORMED ------------------------------------------------------------------#
df.loc[(df.DEFORMED==3)&(df.YEAR<=2008),'DEFORMED'] = 6
df.loc[(df.DEFORMED==2)&(df.YEAR<=2008),'DEFORMED'] = 4
df.loc[(df.DEFORMED==1)&(df.YEAR<=2008),'DEFORMED'] = 2
df.loc[(df.DEFORMED==9)|(df.DEFORMED==8)|(df.DEFORMED==7),'DEFORMED'] = np.nan
df_fars.loc[(df_fars.DEFORMED==9)|(df_fars.DEFORMED==8),'DEFORMED'] = np.nan

#------------------------------------------ SPEC_USE ------------------------------------------------------------------#
df.loc[(df.SPEC_USE==9)|(df.SPEC_USE==98)|(df.SPEC_USE==99)|(df.SPEC_USE==77),'SPEC_USE'] = np.nan
df.loc[(df.SPEC_USE>=1)&(df.SPEC_USE<=24),'SPEC_USE'] = 1

df_fars.loc[(df_fars.SPEC_USE==9)|(df_fars.SPEC_USE==98)|(df_fars.SPEC_USE==99),'SPEC_USE'] = np.nan
df_fars.loc[(df_fars.SPEC_USE>=1)&(df_fars.SPEC_USE<=24),'SPEC_USE'] = 1

#------------------------------------------ TRAV_SP -------------------------------------------------------------------#
# The highest posted speed limit in the United States is 85 mph (137 km/h)
# and can be found only on Texas State Highway 130.
# thus, speeds greater than 96 coded as 97

df.loc[(df.TRAV_SP==999)|(df.TRAV_SP==998),'TRAV_SP'] = np.nan
df.loc[(df.TRAV_SP>96),'TRAV_SP'] = 97

df_fars.loc[(df_fars.TRAV_SP==999)|(df_fars.TRAV_SP==998),'TRAV_SP'] = np.nan
df_fars.loc[((df_fars.TRAV_SP==99)|(df_fars.TRAV_SP==98))&(df_fars.YEAR<=2008),'TRAV_SP'] = np.nan
df_fars.loc[(df_fars.TRAV_SP>96),'TRAV_SP'] = 97

#------------------------------------------ DR_ZIP --------------------------------------------------------------------#
df.loc[(df.DR_ZIP==99999)|(df.DR_ZIP==99998)|(df.DR_ZIP==99997),'DR_ZIP'] = np.nan
df_fars.loc[(df_fars.DR_ZIP==99999)|(df_fars.DR_ZIP==99997),'DR_ZIP'] = np.nan

#------------------------------------------ SPEEDREL ------------------------------------------------------------------#
df.loc[(df.SPEEDREL==9)|(df.SPEEDREL==8),'SPEEDREL'] = np.nan
df.loc[(df.SPEEDREL>=1)&(df.SPEEDREL<=5),'SPEEDREL'] = 1

df_fars.loc[(df_fars.SPEEDREL.isna())&((df_fars.DR_SF1==44)|(df_fars.DR_SF1==43)|(df_fars.DR_SF1==46)),'SPEEDREL'] = 1
df_fars.loc[(df_fars.SPEEDREL==9)|(df_fars.SPEEDREL==8),'SPEEDREL'] = np.nan
df_fars.loc[(df_fars.SPEEDREL>=1)&(df_fars.SPEEDREL<=5),'SPEEDREL'] = 1

#------------------------------------------ DR_SF1 --------------------------------------------------------------------#
# To avoid sparsity problems in our statistical modeling, 4 groups are created in the following way:
# Codes for GES/CRSS are found in parenthesis ()
# Code for FARS are found in brackets []
# 0 = None
# 1 = Careless/improper behaviour/roadrage/Emotional
# (6,8,20,21,22,24,29,32,36,50,51,54,55,57,58,59)
# [1,2,3,5,14,15,17,19,23,25,26,27,28,30,31,33,34,35,38,39,40,41,42,43,44,45,46,47,48,49,52,54,55,73,74,75,76,93]
# 2 = Police pursuit/evading law enforcement/non-traffic violation charged
# (37,60,91)
# [92]
# 3 = Miscellaneous factors
# (9,10,16,18,23,56,94,95,96,97)
# [7,11,12,13,53,56,61-72,77-90,94-98]
df.loc[(df.DR_SF1==99),'DR_SF1'] = np.nan
df.loc[(df.DR_SF1==6)|(df.DR_SF1==8)|(df.DR_SF1==20)|(df.DR_SF1==21)|\
    (df.DR_SF1==22)|(df.DR_SF1==24)|(df.DR_SF1==29)|(df.DR_SF1==32)|\
    (df.DR_SF1==36)|(df.DR_SF1==50)|(df.DR_SF1==51)|(df.DR_SF1==54)|\
    (df.DR_SF1==55)|(df.DR_SF1==57)|(df.DR_SF1==58)|(df.DR_SF1==59),'DR_SF1'] = 1
df.loc[(df.DR_SF1==37)|(df.DR_SF1==60)|(df.DR_SF1==91),'DR_SF1'] = 2
df.loc[(df.DR_SF1==9)|(df.DR_SF1==10)|(df.DR_SF1==16)|(df.DR_SF1==18)|\
    (df.DR_SF1==23)|(df.DR_SF1==56)|(df.DR_SF1==94)|(df.DR_SF1==95)|\
    (df.DR_SF1==96)|(df.DR_SF1==97),'DR_SF1'] = 3

df_fars.loc[(df_fars.DR_SF1==99),'DR_SF1'] = np.nan
df_fars.loc[(df_fars.DR_SF1==6)|(df_fars.DR_SF1==8)|(df_fars.DR_SF1==20)|(df_fars.DR_SF1==21)|\
    (df_fars.DR_SF1==22)|(df_fars.DR_SF1==24)|(df_fars.DR_SF1==29)|(df_fars.DR_SF1==32)|\
    (df_fars.DR_SF1==36)|(df_fars.DR_SF1==50)|(df_fars.DR_SF1==51)|(df_fars.DR_SF1==54)|\
    (df_fars.DR_SF1==55)|(df_fars.DR_SF1==57)|(df_fars.DR_SF1==58)|(df_fars.DR_SF1==59)|\
    (df_fars.DR_SF1 == 1) | (df_fars.DR_SF1 == 2) | (df_fars.DR_SF1 == 3) | (df_fars.DR_SF1 == 5) | \
    (df_fars.DR_SF1 == 14) | (df_fars.DR_SF1 == 15) | (df_fars.DR_SF1 == 17) | (df_fars.DR_SF1 == 19) | \
    (df_fars.DR_SF1 == 23) | (df_fars.DR_SF1 == 25) | (df_fars.DR_SF1 == 26) | (df_fars.DR_SF1 == 27) | \
    (df_fars.DR_SF1 == 28) | (df_fars.DR_SF1 == 30) | (df_fars.DR_SF1 == 31) | (df_fars.DR_SF1 == 33)|\
    (df_fars.DR_SF1 == 34) | (df_fars.DR_SF1 == 35) | (df_fars.DR_SF1 == 38) | (df_fars.DR_SF1 == 39)|\
    (df_fars.DR_SF1 == 40) | (df_fars.DR_SF1 == 41) | (df_fars.DR_SF1 == 42) | (df_fars.DR_SF1 == 43)|\
    (df_fars.DR_SF1 == 44) | (df_fars.DR_SF1 == 45) | (df_fars.DR_SF1 == 46) | (df_fars.DR_SF1 == 47)|\
    (df_fars.DR_SF1 == 48) | (df_fars.DR_SF1 == 49) | (df_fars.DR_SF1 == 52) | (df_fars.DR_SF1 == 54)|\
    (df_fars.DR_SF1 == 55) | (df_fars.DR_SF1 == 73) | (df_fars.DR_SF1 == 74) | (df_fars.DR_SF1 == 75)|\
    (df_fars.DR_SF1 == 76) | (df_fars.DR_SF1 == 93)| (df_fars.DR_SF1 == 4),'DR_SF1'] = 1
df_fars.loc[(df_fars.DR_SF1==37)|(df_fars.DR_SF1==60)|(df_fars.DR_SF1==91)|(df_fars.DR_SF1==92),'DR_SF1'] = 2
df_fars.loc[(df_fars.DR_SF1!=0)&(df_fars.DR_SF1!=1)&(df_fars.DR_SF1!=2)&(~df_fars.DR_SF1.isna()),'DR_SF1'] = 3

#------------------------------------------ STATENAME and COUNTYNAME --------------------------------------------------#
zip_states = pd.read_csv(path_aux + 'ZIPcodelist_county_state.csv', encoding='latin-1')
zip_states = zip_states.rename(columns={'ï»¿Zip Code': 'DR_ZIP'})
zip_states['DR_ZIP'] = zip_states.DR_ZIP.astype(str)
df.loc[~df.DR_ZIP.isna(),'DR_ZIP'] = df.DR_ZIP[~df.DR_ZIP.isna()].astype(int).astype(str)
df = mergeLeftInOrder(x=df,y=zip_states, on="DR_ZIP")
df.loc[~df.DR_ZIP.isna(),'DR_ZIP'] = df.DR_ZIP[~df.DR_ZIP.isna()].astype(int)

#------------------------------------------ HARM_EV -------------------------------------------------------------------#
# To avoid sparsity problems in our statistical modeling, 4 groups are created in the following way:
# 2 NON-COLLISION HARMFUL EVENTS
df.loc[(df.HARM_EV==12)&(df.YEAR==2010),'HARM_EV'] = 72
df.loc[(df.HARM_EV==90)&(df.YEAR==2010),'HARM_EV'] = 12
df.loc[(df.HARM_EV==91)&(df.YEAR==2010),'HARM_EV'] = 54
df.loc[(df.HARM_EV==92)&(df.YEAR==2010),'HARM_EV'] = 55
df.loc[(df.HARM_EV==97)|(df.HARM_EV==99)|(df.HARM_EV==98),'HARM_EV'] = np.nan
df.loc[(df.HARM_EV==10)&(df.YEAR<=2010),'HARM_EV'] = 16
df.loc[(df.HARM_EV==11)&(df.YEAR==2010),'HARM_EV'] = 6
df.loc[(df.HARM_EV==5)&(df.YEAR==2010),'HARM_EV'] = 51
df.loc[(df.HARM_EV==13)&(df.YEAR==2010),'HARM_EV'] = 5
df.loc[(df.HARM_EV==72)&(df.YEAR==2010),'HARM_EV'] = 21
df.loc[(df.HARM_EV==8)&(df.YEAR<=2010),'HARM_EV'] = 7
df.loc[(df.HARM_EV==9)&(df.YEAR<=2010),'HARM_EV'] = 7
df.loc[(df.HARM_EV==44)&(df.YEAR<=2010),'HARM_EV'] = 41
df.loc[((df.HARM_EV>=1)&(df.HARM_EV<=7))|(df.HARM_EV==16)|(df.HARM_EV==44)|\
    (df.HARM_EV==51)|(df.HARM_EV==72),'HARM_EV'] = 2
# 1 collision with motor vehicle in transport
df.loc[(df.HARM_EV==25)&(df.YEAR<=2009),'HARM_EV'] = 12
df.loc[(df.HARM_EV==12)|(df.HARM_EV==54)|(df.HARM_EV==55),'HARM_EV'] = 1
# 3 COLLISION WITH OBJECT NOT FIXED
df.loc[(df.HARM_EV==21)&(df.YEAR<=2010),'HARM_EV'] = 8
df.loc[(df.HARM_EV==22)&(df.YEAR<=2010),'HARM_EV'] = 9
df.loc[(df.HARM_EV==23)&(df.YEAR<=2010),'HARM_EV'] = 10
df.loc[(df.HARM_EV==24)&(df.YEAR<=2010),'HARM_EV'] = 11
df.loc[(df.HARM_EV==27)&(df.YEAR<=2010),'HARM_EV'] = 15
df.loc[(df.HARM_EV==28)&(df.YEAR<=2010),'HARM_EV'] = 18
df.loc[(df.HARM_EV==29)&(df.YEAR<=2010),'HARM_EV'] = 14
df.loc[(df.HARM_EV==45)&(df.YEAR<=2010),'HARM_EV'] = 42
df.loc[(df.HARM_EV==30)&(df.YEAR<=2010),'HARM_EV'] = 45
df.loc[(df.HARM_EV==73)&(df.YEAR==2010),'HARM_EV'] = 23
df.loc[(df.HARM_EV==74)&(df.YEAR==2010),'HARM_EV'] = 24
df.loc[(df.HARM_EV==8)|(df.HARM_EV==9)|(df.HARM_EV==10)|(df.HARM_EV==11)|\
    (df.HARM_EV==14)|(df.HARM_EV==15)|(df.HARM_EV==18)|(df.HARM_EV==45)|\
    (df.HARM_EV==49)|(df.HARM_EV==73)|(df.HARM_EV==74)|(df.HARM_EV==91)|\
    (df.HARM_EV==47),'HARM_EV'] = 3
# 4 COLLISION WITH FIXED OBJECT
df.loc[(df.HARM_EV!=1)&(df.HARM_EV!=2)&(df.HARM_EV!=3)&(~df.HARM_EV.isna()),'HARM_EV'] = 4
# For the FARS database the same groups are created
df_fars.loc[(df_fars.HARM_EV==99)|(df_fars.HARM_EV==98),'HARM_EV'] = np.nan
df_fars.loc[((df_fars.HARM_EV>=1)&(df_fars.HARM_EV<=7))|(df_fars.HARM_EV==16)|(df_fars.HARM_EV==44)|\
    (df_fars.HARM_EV==51)|(df_fars.HARM_EV==72)].groupby('HARM_EV')['ST_CASE'].count()
df_fars.loc[((df_fars.HARM_EV>=1)&(df_fars.HARM_EV<=7))|(df_fars.HARM_EV==16)|(df_fars.HARM_EV==44)|\
    (df_fars.HARM_EV==51)|(df_fars.HARM_EV==72),'HARM_EV'] = 2
df_fars.loc[(df_fars.HARM_EV==12)|(df_fars.HARM_EV==54)|(df_fars.HARM_EV==55)|((df_fars.HARM_EV==13)&(df_fars.YEAR<=2009)),'HARM_EV'] = 1
df_fars.loc[(df_fars.HARM_EV==8)|(df_fars.HARM_EV==9)|(df_fars.HARM_EV==10)|(df_fars.HARM_EV==11)|\
    (df_fars.HARM_EV==14)|(df_fars.HARM_EV==15)|(df_fars.HARM_EV==18)|(df_fars.HARM_EV==45)|\
    (df_fars.HARM_EV==49)|(df_fars.HARM_EV==73)|(df_fars.HARM_EV==74)|(df_fars.HARM_EV==91)|\
    (df_fars.HARM_EV==47),'HARM_EV'] = 3
df_fars.loc[(df_fars.HARM_EV!=1)&(df_fars.HARM_EV!=2)&(df_fars.HARM_EV!=3)&(~df_fars.HARM_EV.isna()),'HARM_EV'] = 4

#------------------------------------------ HOUR ----------------------------------------------------------------------#
df.loc[(df.HOUR==99),'HOUR'] = np.nan
df.loc[(df.HOUR==24),'HOUR'] = 0
df_fars.loc[(df_fars.HOUR==99),'HOUR'] = np.nan
df_fars.loc[(df_fars.HOUR==24),'HOUR'] = 0

#------------------------------------------ WEATHER -------------------------------------------------------------------#
df.loc[(df.WEATHER==99)|(df.WEATHER==98)|((df.WEATHER==9)&(df.YEAR<=2009)),'WEATHER'] = np.nan
df.loc[(df.WEATHER>=2)&(df.WEATHER<=12),'WEATHER'] = 2
df.loc[(df.WEATHER==1),'WEATHER'] = 0
df.loc[(df.WEATHER==2),'WEATHER'] = 1

df_fars.loc[(df_fars.WEATHER==99)|(df_fars.WEATHER==98)|((df_fars.WEATHER==9)&(df_fars.YEAR<=2009)),'WEATHER'] = np.nan
df_fars.loc[(df_fars.WEATHER>=2)&(df_fars.WEATHER<=12),'WEATHER'] = 2
df_fars.loc[(df_fars.WEATHER==1),'WEATHER'] = 0
df_fars.loc[(df_fars.WEATHER==2),'WEATHER'] = 1

#------------------------------------------ PREV_ACC ------------------------------------------------------------------#
df_fars.loc[(df_fars.PREV_ACC>=98),'PREV_ACC'] = np.nan
df_fars.loc[(df_fars.PREV_ACC>=1),'PREV_ACC'] = 1

#------------------------------------------ PREV_DWI ------------------------------------------------------------------#
df_fars.loc[(df_fars.PREV_DWI>=98),'PREV_DWI'] = np.nan
df_fars.loc[(df_fars.PREV_DWI>=1),'PREV_DWI'] = 1

#------------------------------------------ PREV_SUS ------------------------------------------------------------------#
df_fars.loc[(df_fars.PREV_SUS>=98),'PREV_SUS'] = np.nan
df_fars.loc[(df_fars.PREV_SUS>=1),'PREV_SUS'] = 1

#------------------------------------------ PREV_SPD ------------------------------------------------------------------#
df_fars.loc[(df_fars.PREV_SPD>=98),'PREV_SPD'] = np.nan
df_fars.loc[(df_fars.PREV_SPD>=1),'PREV_SPD'] = 1

#------------------------------------------ SEG -----------------------------------------------------------------------#
#------------------------------------------ POP2018 -------------------------------------------------------------------#
#------------------------------------------ PREV ----------------------------------------------------------------------#
#------------------------------------------ MARITAL -------------------------------------------------------------------#
#------------------------------------------ RACE ----------------------------------------------------------------------#
#------------------------------------------ Accident zip code (denoted ZCTA, only available for FARS) -----------------#
# Imputed data process for SEG, POP2018, PREV, MARITAL, RACE and ZCTA described in paper. The variables are shared
# through the .csv file in the'Auxiliary files' folder for the filtered databases.
# Note: Race is only imputed for those without the variable unknown or not reported.

Imputed_gescrss = pd.read_csv(path_aux + 'GESCRSS_IMPUTED_2001-2019.csv', encoding='latin-1', low_memory=False)
df['KEY'] = df.YEAR.astype(str) + df.ST_CASE.astype(str) + df.VEH_NO.astype(str) + df.PER_NO.astype(str)
Imputed_gescrss['KEY'] = Imputed_gescrss.YEAR.astype(str) + Imputed_gescrss.ST_CASE.astype(str) +\
                         Imputed_gescrss.VEH_NO.astype(str) + Imputed_gescrss.PER_NO.astype(str)
del Imputed_gescrss['Unnamed: 0'], Imputed_gescrss['ST_CASE'], Imputed_gescrss['VEH_NO'], Imputed_gescrss['PER_NO']
del Imputed_gescrss['YEAR']
df = mergeLeftInOrder(x=df,y=Imputed_gescrss, on="KEY")
del df['KEY']

# For the FARS database
Imputed_fars = pd.read_csv(path_aux + 'FARS_IMPUTED_2001-2019.csv', encoding='latin-1', low_memory=False)
df_fars['KEY'] = df_fars.YEAR.astype(str) + df_fars.ST_CASE.astype(str) + \
                 df_fars.VEH_NO.astype(str) + df_fars.PER_NO.astype(str)
Imputed_fars['KEY'] = Imputed_fars.YEAR.astype(str) + Imputed_fars.ST_CASE.astype(str) + \
                     Imputed_fars.VEH_NO.astype(str) + Imputed_fars.PER_NO.astype(str)
del Imputed_fars['Order'], Imputed_fars['ST_CASE'], Imputed_fars['VEH_NO'], Imputed_fars['PER_NO']
del Imputed_fars['YEAR']
df_fars = mergeLeftInOrder(x=df_fars,y=Imputed_fars, on="KEY")
del df_fars['KEY']


# Save the GESCRSS and FARS standardized database
df.to_csv(path_gescrss + '0 AccVecPer_Filtered_Standardized_'+str(iniyear)+'-'+str(endyear)+'.csv')
df_fars.to_csv(path_fars + '0 AccVecPer_Filtered_Standardized_'+str(iniyear)+'-'+str(endyear)+'.csv')


########################################################################################################################
#-------------------------- END OF CODE FOR DATABASE STANDARDIZATION -----------------------------------------------####
########################################################################################################################

