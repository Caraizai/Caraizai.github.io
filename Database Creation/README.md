This README.md file was generated on 2021-12-02 by Carlos Andrés Araiza Iturria

# GENERAL INFORMATION

A consolidated database of police-reported motor vehicle traffic accidents in the United States for actuarial applications.

The database can be directly accessed here: [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.5748416.svg)](https://doi.org/10.5281/zenodo.5748416)

Author Information

    A. Author
		Name: Carlos Andrés Araiza Iturria
		Email: caraizai@uwaterloo.ca
    
	  B. Co-author
		Name: Mary Hardy
		Email: mary.hardy@uwaterloo.ca

	  C. Co-author
		Name: Paul Marriott
		Email: pmarriott@uwaterloo.ca
    
    Institution: University of Waterloo
		Address: 200 University Ave W, Waterloo, ON N2L 3G1


Funding granted by the Natural Sciences and Engineering Research Council of Canada. Hardy: RGPIN-2018-03754, Marriott: RGPIN-2020-04015.

# DATA & FILE OVERVIEW

The script to create the database was written with Python and to simplify its readability it was split into 4 different files available. The files do the following:

  [1 FARS_GESCRSS_Construction.py](https://github.com/Caraizai/Caraizai.github.io/blob/A-CONSOLIDATED-DATABASE-OF-POLICE-REPORTED-MOTOR-VEHICLE-TRAFFIC-ACCIDENTS-IN-THE-UNITED-STATES-FOR-ACTUARIAL-APPLICATIONS/Database%20Creation/1%20FARS_GESCRSS_Construction.py) : Download the annual NHSTA files and merge them into two databases, one for GES/CRSS and another one for FARS.

  [2 FARS_GESCRSS_Filtering.py](https://github.com/Caraizai/Caraizai.github.io/blob/A-CONSOLIDATED-DATABASE-OF-POLICE-REPORTED-MOTOR-VEHICLE-TRAFFIC-ACCIDENTS-IN-THE-UNITED-STATES-FOR-ACTUARIAL-APPLICATIONS/Database%20Creation/2%20FARS_GESCRSS_Filtering.py) : Filter relevant MVTA for personal automobile insurance.

  [3 FARS_GESCRSS_Standardizing.py](https://github.com/Caraizai/Caraizai.github.io/blob/A-CONSOLIDATED-DATABASE-OF-POLICE-REPORTED-MOTOR-VEHICLE-TRAFFIC-ACCIDENTS-IN-THE-UNITED-STATES-FOR-ACTUARIAL-APPLICATIONS/Database%20Creation/3%20FARS_GESCRSS_Standardizing.py) : Standardize the NHTSA historical coding practice to that of 2019.

  [4 FARS_GESCRSS_Sample.py](https://github.com/Caraizai/Caraizai.github.io/blob/A-CONSOLIDATED-DATABASE-OF-POLICE-REPORTED-MOTOR-VEHICLE-TRAFFIC-ACCIDENTS-IN-THE-UNITED-STATES-FOR-ACTUARIAL-APPLICATIONS/Database%20Creation/4%20FARS_GESCRSS_Sample.py) : Pooling procedure to create the final database.

NOTE: The ‘[Auxiliary files](https://github.com/Caraizai/Caraizai.github.io/tree/A-CONSOLIDATED-DATABASE-OF-POLICE-REPORTED-MOTOR-VEHICLE-TRAFFIC-ACCIDENTS-IN-THE-UNITED-STATES-FOR-ACTUARIAL-APPLICATIONS/Database%20Creation/Auxiliary%20files)’ folder needs to be downloaded and the ‘.zip’ files inside need to be uncompressed for the code to work.

Estimated run time of the 4 codes:
~9 minutes
using a MacBook Pro (2020), M1, 8gb RAM.
