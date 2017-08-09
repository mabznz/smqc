# Strong Motion Noise Check

Queries hazard database for strong motion values and orders by most likely station to be non performing with data quality issues.

Please refer for examples
https://wiki.geonet.org.nz/display/dmcops/Strong+Motion+Noise+checks

As hazard database only stores summarised pga, pgv and mmi data values ranging for an hour, script should be set up to run every hour. It will create files for each noise check if they do not exist and continually append to these files once they do exist. This is to allow data to be collected for longer periods as data quality poor performance may be related to a regular weekly factor for instance.

## Dependancies

* For script to run hazard_r user password must be an environment variable HAZARD_PASSWD

* Needs to run in Geonet VPN.

* Log and csv data files written to /tmp. Change to appropiate.
