The code was written with Python and to simplify its readability it was split into 4 different files available here. The files do the following:

1 FARS_GESCRSS_Construction.py : Download the annual NHSTA files and merge them into two databases, one for GES/CRSS and another one for FARS.

2 FARS_GESCRSS_Filtering.py : Filter relevant MVTA for personal automobile insurance.

3 FARS_GESCRSS_Standardizing.py : Standardize the NHTSA historical coding practice to that of 2019.

4 FARS_GESCRSS_Sample.py : Pooling procedure to create the final database.

NOTE: The ‘Auxiliary files’ folder needs to be downloaded and the ‘.zip’ files inside need to be uncompressed for the code to work.

Estimated run time of the 4 codes:
~9 minutes
using a MacBook Pro (2020), M1, 8gb RAM.
