#!/usr/bin/perl -w
# file: postal_code_join.pl
# purpose: perform a SQL "left outer join" operation on two CSV files
# created: Jan 2009, Chad Clark, chad.clark @ gmail . com
#
# SVN root: FIXME
# $Id$

use strict;
use Data::Dumper;
use Fcntl ':flock';
use File::Basename;
use Getopt::Long;
use IO::Seekable;
use Readonly;
use Text::CSV;


# -- globals -----------------------------------------------------

Readonly my $SUB_SUCCESS => 1;
Readonly my $SUB_FAILURE => 0;

Readonly my $IDX_POSTCODE => 0;
Readonly my $IDX_LAT      => 1;
Readonly my $IDX_LONG     => 2;

my $DEBUG = 0;


# -- subroutines -------------------------------------------------

sub normalize_postal_code($);
sub read_geocode_file($$);
sub read_records_file($$);
sub string_looks_like_a_valid_postal_code($);
sub usage_message();
sub write_joined_file($$$);


# -- main --------------------------------------------------------

# GOAL : get the command line arguments
my %CONFIG = (
);

GetOptions(
    "d|debug"           => \$DEBUG ,
    "h|help"            => \$CONFIG{help} ,
    "igf=s"             => \$CONFIG{input_geocode_file_name} ,
    "irf=s"             => \$CONFIG{input_records_file_name} ,
    "ojf=s"             => \$CONFIG{output_joined_file_name} ,
);

if (($CONFIG{help}) or
    (not defined $CONFIG{input_geocode_file_name} ) or
    (not defined $CONFIG{input_records_file_name} ) or
    (not defined $CONFIG{output_joined_file_name} )
) {
    print usage_message();
    exit 1;
}

# GOAL : read in the input geocode file

my $geocode_hash;
unless (read_geocode_file($CONFIG{input_geocode_file_name}, \$geocode_hash)) {
    die "$0: Unable to read input geocode file.  '$CONFIG{input_geocode_file_name}'\n";
}
$DEBUG && print "geocode_hash: " , Dumper($geocode_hash);


# GOAL : read in the input records file

my $record_list;
unless (read_records_file($CONFIG{input_records_file_name}, \$record_list)) {
    die "$0: Unable to read input records file.  '$CONFIG{input_records_file_name}'\n";
}
$DEBUG && print "record_list: " , Dumper($record_list);

# GOAL : write out the joined file

unless (write_joined_file($CONFIG{output_joined_file_name}, $geocode_hash, $record_list)) {
    die "$0: Unable to write the joined file.  '$CONFIG{output_joined_file_name}'\n";
}


# FIXME


exit 0;


# -- subroutines -------------------------------------------------

sub normalize_postal_code($) {
# FIXME : comment

    my $postal_code = shift;

    my $new_code = $postal_code;

    $new_code = lc $new_code;
    $new_code =~ s/\s+//g;

    return $new_code;
}


sub read_geocode_file($$) {

    my $file_name = shift;
    my $o_hash    = shift;

    my $hash = {};

    # if the file doesn't exist assume it's empty
    unless (-f $file_name) {
	warn "$0: Input geocode file does not exist.  '$file_name'\n";
	return $SUB_FAILURE;
    }

    my $csv = Text::CSV->new();

    unless (open FH, "+<", $file_name) {
	warn "$0: Unable to read file.  '$file_name'\n";
	return $SUB_FAILURE;
    }

    # lock the file so we get consistent data from it
    unless (flock(FH,LOCK_EX)) {
	warn "$0: Can't lock geocode file. '$file_name' '$!'\n";
	return $SUB_FAILURE;
    }
    unless (seek(FH, 0, 0)) {
	warn "$0: Can't seek to start of geocode file. '$file_name' '$!'\n";
	return $SUB_FAILURE;
    }

    LINE: while (my $line = <FH>) {
	next LINE if ($line =~ m/^#/);

	my $status = $csv->parse($line);

	if($status != 1) {
	    warn "$0: Unable to parse geocode line, skiping.  '$line'\n";
	    next LINE;
	}

	my @fields = $csv->fields();

	if(scalar @fields != 3) {
	    warn "$0: Geocode line does not have 3 fields, skiping.  '$line'\n";
	    next LINE;
	}

	# normalize the format of the postal code
	$fields[$IDX_POSTCODE] = normalize_postal_code($fields[$IDX_POSTCODE]);

	# check that the field data is valid.
	unless (string_looks_like_a_valid_postal_code($fields[$IDX_POSTCODE])) {
	    warn "$0: Value in geocode file does not look like a postal code, "
		. "ignoring.  '" . $fields[$IDX_POSTCODE] . "'\n";
	    next LINE;
	}

	# FIXME : no checks for latt & long yet

	$hash->{$fields[$IDX_POSTCODE]} = [$fields[$IDX_LAT],$fields[$IDX_LONG]];
    }

    flock(FH,LOCK_UN);
    close FH;

    $$o_hash = $hash;
    return $SUB_SUCCESS;
}


sub read_records_file($$) {

    my $file_name = shift;
    my $o_list    = shift;

    my $list = [];

    # if the file doesn't exist assume it's empty
    unless (-f $file_name) {
	warn "$0: Input records file does not exist.  '$file_name'\n";
	return $SUB_FAILURE;
    }

    my $csv = Text::CSV->new();

    unless (open FH, "+<", $file_name) {
	warn "$0: Unable to read file.  '$file_name'\n";
	return $SUB_FAILURE;
    }

    # lock the file so we get consistent data from it
    unless (flock(FH,LOCK_EX)) {
	warn "$0: Can't lock records file. '$file_name' '$!'\n";
	return $SUB_FAILURE;
    }
    unless (seek(FH, 0, 0)) {
	warn "$0: Can't seek to start of records file. '$file_name' '$!'\n";
	return $SUB_FAILURE;
    }

    LINE: while (my $line = <FH>) {
	next LINE if ($line =~ m/^#/);

	my $status = $csv->parse($line);

	if($status != 1) {
	    warn "$0: Unable to parse records line, skiping.  '$line'\n";
	    next LINE;
	}

	my @fields = $csv->fields();

	# get the normalized format of the postal code
	my $postal_code = normalize_postal_code($fields[$IDX_POSTCODE]);

	# check that the postal code looks valid.
	unless (string_looks_like_a_valid_postal_code($postal_code)) {
	    warn "$0: Value in records file does not look like a postal code, "
		. "ignoring.  '" . $fields[$IDX_POSTCODE] . "'\n"
		. Dumper (\@fields);
	    next LINE;
	}

	push @$list, \@fields;
    }

    flock(FH,LOCK_UN);
    close FH;

    $$o_list = $list;
    return $SUB_SUCCESS;
}


sub string_looks_like_a_valid_postal_code($) {

    my $str = shift;

    unless ($str =~ m/^([A-Z]\d[A-Z]\s*\d[A-Z]\d)$/i) {
	return 0;
    }

    return 1;
}


sub usage_message() {

my $prog_name = fileparse($0);

my $str = qq{
  Usage: $prog_name -igf GEOCODE_FILE -irf RECORD_FILE -ojf JOINED_FILE

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
      $prog_name -h
      $prog_name -igf postal_codes.csv -irf survey_data.csv -ojf survey_with_geo.csv
      $prog_name -igf igf.csv          -irf irf.csv         -ojf ojf.csv
      $prog_name -d -igf igf.csv       -irf irf.csv         -ojf ojf.csv

  Example file data:

    Input Geocode File:
      t2j5k9,50.930745,-114.01481

    Input Records File:
      t2j5k9,lots,of,stuff,here
      t2b2t2,some,other,stuff,here
      T2J5K9,this,is,record,three

    Output Joined File:
      t2j5k9,lots,of,stuff,here,50.930745,-114.01481
      t2b2t2,some,other,stuff,here,NULL,NULL
      T2J5K9,this,is,record,three,50.930745,-114.01481

};
}


sub write_joined_file($$$) {

    my $file_name    = shift;
    my $geocode_hash = shift;
    my $record_list  = shift;

    # make a backup of any existing output file
    if (-e $file_name) {
	rename $file_name, $file_name . "." . time;
    }

    # open the file for writing
    unless (open FH, ">", $file_name) {
	warn "$0: Unable to open file for writing.  '$file_name'\n";
	return $SUB_FAILURE;
    }

    # lock the file so we get consistent data from it
    unless (flock(FH,LOCK_EX)) {
	warn "$0: Can't lock database file. '$file_name' '$!'\n";
	return $SUB_FAILURE;
    }

    unless (seek(FH, 0, 0)) {
	warn "$0: Can't seek to start of database file. '$file_name' '$!'\n";
	return $SUB_FAILURE;
    }

    # write out the newly joined records

    my $csv = Text::CSV->new();
    RECORD: foreach my $record (@$record_list) {

	my $postal_code = $record->[$IDX_POSTCODE];
	$postal_code = normalize_postal_code($postal_code);

	# GOAL : get the lat/long pair for the postal code for this record

	my $lat_long_pair = ["NULL","NULL"];  # default values

	if (defined $geocode_hash->{$postal_code}) {
	    # CLAIM : the $geocode_hash has latt and long for this postal code
	    $lat_long_pair = $geocode_hash->{$postal_code};
	}

	# GOAL : write out the joined data for this record

	unless ($csv->combine(@$record, @$lat_long_pair)) {
	    warn "$0: Unable to combine fileds for database." . Dumper($record);
	    next RECORD;
	}

        print FH $csv->string() , "\n";

    }

    flock(FH,LOCK_UN);
    close FH;

    return $SUB_SUCCESS;
}
