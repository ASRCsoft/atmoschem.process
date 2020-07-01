# ASRC Routine Meteorological and Chemistry Data

These files contain the ASRC's routine meteorological and chemistry data from
the Whiteface Mountain summit (WFMS), the Whiteface Mountain lodge (WFML),
Pinnacle State Park (PSP), and Queens College (QC) up to 2020-05-01. Along with
data from ASRC instruments, the dataset also includes data from the NYS
Department of Environmental Conservation, the NYS Mesonet, and the EPA's Air
Quality System.

The data has been processed to remove values that don't reflect ambient
conditions, to adjust values for instrument calibrations, and to aggregate
measurements to hourly values. All measurements are accompanied by flags
following NARSTO guidelines (see
cdiac.ess-dive.lbl.gov/programs/NARSTO/metadatastandards/consensus_flag_standard_20011105.pdf).
Whiteface Mountain data before 2001 was not originally flagged, and M1 and V0
flags have been added retroactively. Ultrafine PM values have an additional
AQS-style flag column.

WS_raw and WD_raw columns are raw averages of wind speed and wind direction
measurements, respectively. These values are sometimes misleading and should be
used with caution. All other WS and WD values are vector averages that combine
wind speeds and directions.

Whiteface Mountain lodge CO measurements seem to be affected by room temperature
and should also be used with caution.

The instruments.csv file contains instrument information for some recent data.

Any questions should be directed to Mr. William May at wcmay@albany.edu or
Dr. James Schwab at jschwab@albany.edu.
