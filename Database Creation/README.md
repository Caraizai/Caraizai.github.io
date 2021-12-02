The script was written with Python and to simplify its readability it was split into 4 different files available here. The files do the following:

  [1 FARS_GESCRSS_Construction.py](https://github.com/Caraizai/Caraizai.github.io/blob/A-CONSOLIDATED-DATABASE-OF-POLICE-REPORTED-MOTOR-VEHICLE-TRAFFIC-ACCIDENTS-IN-THE-UNITED-STATES-FOR-ACTUARIAL-APPLICATIONS/Database%20Creation/1%20FARS_GESCRSS_Construction.py) : Download the annual NHSTA files and merge them into two databases, one for GES/CRSS and another one for FARS.

  [2 FARS_GESCRSS_Filtering.py](https://github.com/Caraizai/Caraizai.github.io/blob/A-CONSOLIDATED-DATABASE-OF-POLICE-REPORTED-MOTOR-VEHICLE-TRAFFIC-ACCIDENTS-IN-THE-UNITED-STATES-FOR-ACTUARIAL-APPLICATIONS/Database%20Creation/2%20FARS_GESCRSS_Filtering.py) : Filter relevant MVTA for personal automobile insurance.

  [3 FARS_GESCRSS_Standardizing.py](https://github.com/Caraizai/Caraizai.github.io/blob/A-CONSOLIDATED-DATABASE-OF-POLICE-REPORTED-MOTOR-VEHICLE-TRAFFIC-ACCIDENTS-IN-THE-UNITED-STATES-FOR-ACTUARIAL-APPLICATIONS/Database%20Creation/3%20FARS_GESCRSS_Standardizing.py) : Standardize the NHTSA historical coding practice to that of 2019.

  [4 FARS_GESCRSS_Sample.py](https://github.com/Caraizai/Caraizai.github.io/blob/A-CONSOLIDATED-DATABASE-OF-POLICE-REPORTED-MOTOR-VEHICLE-TRAFFIC-ACCIDENTS-IN-THE-UNITED-STATES-FOR-ACTUARIAL-APPLICATIONS/Database%20Creation/4%20FARS_GESCRSS_Sample.py) : Pooling procedure to create the final database.

NOTE: The ‘[Auxiliary files](https://github.com/Caraizai/Caraizai.github.io/tree/A-CONSOLIDATED-DATABASE-OF-POLICE-REPORTED-MOTOR-VEHICLE-TRAFFIC-ACCIDENTS-IN-THE-UNITED-STATES-FOR-ACTUARIAL-APPLICATIONS/Database%20Creation/Auxiliary%20files)’ folder needs to be downloaded and the ‘.zip’ files inside need to be uncompressed for the code to work.

Estimated run time of the 4 codes:
~9 minutes
using a MacBook Pro (2020), M1, 8gb RAM.
