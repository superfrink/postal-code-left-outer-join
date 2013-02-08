postal-code-left-outer-join
======================================================

Left-outer-join CSV program for Canadian postal codes

======================================================

Usage message:

C:\Documents and Settings\chad\My Documents\code\perl\adam>postal_code_join.exe

  Usage:

    postal_code_join.exe -igf GEOCODE_FILE -irf RECORD_FILE -ojf JOINED_FILE

  Where:

    -igf     The input geocoding file.  This is a CSV file with three fields:
             { postal code, latt, long }.  (See example below.)

    -irf     The input records file.  This is a CSV file with 2 or more fields:
             { postal code, ... }.  (See example below.)

    -ojf     The output joined file.  This is the name of the CSV file to
             produce.  It will have 2 more columns than the input record file:
             { postal code, ... , latt, long }.  (See example below.)
             If this file already exists it is renamed before the new joined
             file is written.

  Options:

    -d       Debugging details such as some data structures will be printed.
    -h       Displays this message and exits.

  Examples:

      postal_code_join.exe -h
      postal_code_join.exe -igf postal_codes.csv -irf survey_data.csv -ojf survey_with_geo.csv
      postal_code_join.exe -igf igf.csv          -irf irf.csv         -ojf ojf.csv
      postal_code_join.exe -d -igf igf.csv       -irf irf.csv         -ojf ojf.csv

  Example file data:

    Input Geocode File:
      t2j5k9,50.930745,-114.01481

    Input Records File:
      t2j5k9,lots,of,stuff,here
      t2b2t2,some,other,stuff,here
      t2j5k9,this,is,record,three

    Output Joined File:
      t2j5k9,lots,of,stuff,here,50.930745,-114.01481
      t2b2t2,some,other,stuff,here,NULL,NULL
      t2j5k9,this,is,record,three,50.930745,-114.01481

