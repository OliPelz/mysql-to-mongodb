#!/usr/bin/perl


#TODO: overhaul description here. 
#In short: use this script if you have ONE master table you want to embed some flat tables (flat table = table which has no references to other tables. in perl this means a hash with only scalar values!). This is a fast script. If you have to embedd multiple tables in each other use convert_sql_to_json_embed.pl instead (much slower - big memory footprint!)

# Note: first line in the config file is defined as the master table
use DBI;
use DBD::mysql;
use JSON;
use strict;
use warnings;
use Data::Dumper;
use File::Temp;

# we generate a column in every data table, we need to make it unique by using time
# because it must not be available as a column in the original sql table
my $linkId = "__Link_Id__".time;

my $DEBUG = 1;
my $DEBUG_REC = 5000;
#usage ./convert_sql_to_mongo_embed.pl <db user> <db name> <dp passwd> <db_host> <db_port> <input file> <output dir>


#TODO: This instruction here is deprecated
#perl ~/Git/mysql-to-mongodb/convert_sql_to_json_embedd.pl root genome_rnai_universal_v14_tuning \
#<secretPassword> \
#localhost \
#3306 \
#myrules.conf
#`date +%Y-%m-%d`


#for the input file the input format has the following simple format:

#[unique identifier e.g. single alphabet to identify sql table]:sql query to produce a table surrounded by double quotes
#[unique identifier e.g. single alphabet to identify sql table]:sql query to produce a table surrounded by double quotes
#[unique identifier e.g. single alphabet to identify sql table]:sql query to produce a table surrounded by double quotes
#[...]
#
#[relationship rule 1 for embedding data: oneToMany/oneToOne]
#[unique identifier rule 1 of sql table ]:id to join to relationship, this table will be used to embed the other one
#[unique identifier rule 1 of sql table]:id to join to relationship, this table will be used to ENMBED INTO the other one
#[rule 1 of name of the new column on the table to embed the other one in]
#
#[...rule blocks can be repeated n times]
#
#example for a config file:
#
#A:"SELECT E.ExperimentId as experiment, P.name as phenoName, O.organismName as organismName \
#FROM OPTIMIZED_PhenotypeSearch P \
#inner join OPTIMIZED_PhenotypeSearchExperiment E \
#on P.id = E.PhenoSearchId \
#inner join Organism O \
#on E.OrganismId = O.id \
#order by P.name"\
#:"select * from NewExternalExperiment"
#C:"SELECT * from OPTIMIZED_PhenotypeSearchPhenotypes" \

#oneToMany \
#A:experiment \
#B:id \
#experiment

#oneToMany \
#B:id \
#C:expId
#phenotypeGroups



#connect to database
my ($db_user, $db_name, $db_pass, $db_host, $db_port) = ($ARGV[0], $ARGV[1],$ARGV[2],$ARGV[3],$ARGV[4]);
my $dbh = DBI->connect("DBI:mysql:$db_name;host=$db_host;port=$db_port","$db_user","$db_pass")
or closeDBAndDie("Couldn't connect to database: ");

my $outputPath = $ARGV[6] || die "no output path is given as parameter";
if($outputPath =~ /([^\/])$/) {# append trailing slash / if not set
   $outputPath.="/";
} 

###set encoding 'n stuff
my $sth = $dbh->prepare("SET NAMES 'utf8'")
or closeDBAndDie("Couldn't prepare statement");
$sth->execute() or closeDBAndDie("Couldn't connect to database: ");
$sth = $dbh->prepare("SET FOREIGN_KEY_CHECKS=0")
or closeDBAndDie("Couldn't prepare statement");
$sth->execute() or closeDBAndDie("Couldn't connect to database: ");


open(CONFIGFILE, $ARGV[5]) || die "cannot open input file";


my $line;
my %sql;
my %rule;
my $lineCnt = 0;
my $blockCount = 0;
my $relationCount = 0; 
my $masterTableId;
#first: read in config file
while($line = <CONFIGFILE>) {
	$lineCnt++;
	if($line =~ /^\n$/) {
		$blockCount++;
		next;
	}
	$line =~ s/\n//g;
	# sql block
	if($blockCount == 0) {  
		if($line =~ /^(\w+):"([^"]+)"$/) {
			my $id= $1;
			my $sql_statement = $2;
			if(defined($sql{$id}{"query"})) {
				die "unique ids are non-unique, there are several sql lines with same identifier : $id";
			}
			$sql{$id}{"query"} = $sql_statement;
			if($lineCnt == 1) {
			    $masterTableId = $id;
			}
		}
	}
	#embedd block
	elsif($blockCount > 0) {
		if($line =~ /^(oneToOne|oneToMany|manyToMany),(\w+):(\w+),(\w+):(\w+),(\w+)$/) {
			$rule{"join"}{$relationCount} = [$1,$2,$3,$4,$5,$6];
			$relationCount++;
		}

		if($line =~ /^(\w+)=([_\w\*,]+)$/) {
			$rule{"colFilters"}{$1} = $2;
		}
	}
}
close CONFIGFILE;
die "no sql statements could be read from the cfg fie" if(scalar keys %sql == 0);

print STDERR "started quering sql database\n";
#next: query all table information from sql database and put in datastructure
foreach my $table_id (keys %sql) {
	my $sql_string = $sql{$table_id}{"query"};
	$sth = $dbh->prepare($sql_string) or closeDBAndDie("Couldn't prepare statement");
	$sth->execute() or closeDBAndDie("Couldn't connect to database: ");
	my @table_data;
	while (my $hash_ref = $sth->fetchrow_hashref) { 
		foreach my $myKey (keys %$hash_ref) {
			#there are some key value pairs which give trouble later, so change undef to empty string, e.g. I have seen strange things such as:
			# %myHash; $myHash{"myKey"}  = undef;
			if(!defined($hash_ref->{$myKey})) {
				$hash_ref->{$myKey} = "";
			}
		   die "the code needs to save data in a column called $linkId" if($myKey eq $linkId);
	    }
	    push @table_data, $hash_ref;
	}
	print STDERR scalar(@table_data)." records returned from sql query id ".$table_id."\n" if($table_id);
	print STDERR "done querying sql database for sql id ".$table_id."\n";
	$sql{$table_id}{"data"} = \@table_data;
}
print STDERR "...done\n";
my $cnt;
my $size;
my %colFilters   = %{$rule{"colFilters"}};

#filter tables and put in new datastructure and make links to original data
my %sqlDataFiltered;
my %linkToSql;

#this is only temp for making unique
my %uniq_data_to_id_temp;
#the ids to connect to unique values
my $uniq_id = 0;
print STDERR "started filtering tables\n";
foreach my $table_id (sort keys %sql) {
	my $filter = $colFilters{$table_id};
	$cnt = 0;
	$size = scalar(@{$sql{$table_id}{"data"}});
	foreach my $data (@{$sql{$table_id}{"data"}}) {
	#the filtered hash	
	   my $data_new   = &copyHash($data, $filter);
       #store only unique ones and connet to original database table
	   my $uniq_string = &valuesToString($data_new);
	   if(!defined($uniq_data_to_id_temp{$uniq_string})) {
	     	$uniq_data_to_id_temp{$uniq_string} = $uniq_id;
		    $sqlDataFiltered{$table_id}{$uniq_id} = $data_new;
		    $uniq_id++;
	   }
	   my $id = $uniq_data_to_id_temp{$uniq_string};
	#link the filtered table data to original table 
	#
	   $data->{$linkId} = $id;
   	   print STDERR $table_id." filtering record no $cnt / $size \n" if($DEBUG && $cnt++%$DEBUG_REC == 0);
	   
    }
	print STDERR "filtering table ".$table_id." done\n";
}
print STDERR "...done\n";

print STDERR "started validating filtering\n";
#test code if filtering worked!
foreach my $table_id (sort keys %sql) {
   foreach my $data (@{$sql{$table_id}{"data"}}) {
	  my $link = $data->{$linkId};
	  my $data2 = $sqlDataFiltered{$table_id}{$link};
	  $cnt = 0;
   	  $size = scalar(@{$sql{$table_id}{"data"}});
	  foreach my $myKey (keys %$data2) {
		  my $myValue = $data2->{$myKey};
		  if(!defined($data->{$myKey})) {
			  die "filtering did not work, key not found '".$myKey."' in original data:\n".Dumper($data);
		  }
		  if($data->{$myKey} ne $data2->{$myKey}) {
			  die "filtering did not work, value differ for key '".$myKey."', original: ".$data->{$myKey}." vs :".$data2->{$myKey};
		  }
	  } 
  	   print STDERR $table_id." validating record no $cnt / $size \n" if($DEBUG && $cnt++%$DEBUG_REC == 0);
	  
   }
 	print STDERR "validating filtering of table ".$table_id." done\n"; 
}
print STDERR "...done\n";

&saveSqlObj(\%sql);

print STDERR "started actual embedding and print out procedure\n";
#now process all relations and add to each other
my @master_table = @{$sql{$masterTableId}{"data"}};
my $file = $outputPath.$masterTableId.".json";
open(FILE, ">", $file) || die "cannot open file $file for writing";
print FILE "[";
my $entryCount = 0;
$cnt = 0;
$size = scalar @master_table;
#for every row in the master table
foreach my $to_row (@master_table) {
	my $to_filt   = $sqlDataFiltered{$masterTableId}{$to_row->{$linkId}};
	
   if($entryCount++ > 0) {
      print FILE ",";  # we build our own json arr, so we need to introduce commas for our object serperation
   }
	#for every relation embedding rule
	foreach my $relationCount (sort keys %{$rule{"join"}}) {
		my @singleRule = @{$rule{"join"}{$relationCount}};
		my ($relation, $dummy, $to_join, $from_id, $from_join, $embedCol) = @singleRule;
		my @from_table = @{$sql{$from_id}{"data"}};
		#for every row in the relation
		my $found = 0;
		my $embedString;
		my @embedArr;
		foreach my $from_row (@from_table) {
			#if we can embed by given id
			if($to_row->{$to_join} eq $from_row->{$from_join}) {
				$found = 1;
				# embed the data in our filtered datastructure
				my $from_filt = $sqlDataFiltered{$from_id}{$from_row->{$linkId}};
				
				#the actual embed line. embed the data to the column named $embedCol
				if($relation eq "oneToOne") {
				   $embedString = $from_filt;
			    }
				elsif($relation eq "oneToMany") {  #make array
					push @embedArr, $from_filt;
				}
			}
		}
		if($found) {
		   if($relation eq "oneToOne") {
		      $to_filt->{$embedCol} = $embedString;
	       }
		   elsif($relation eq "oneToMany") {  #make array
			   $to_filt->{$embedCol} = \@embedArr;
		   }
	    }
  	   print STDERR "printing output record from mastertable no $cnt / $size \n" if($DEBUG && $cnt++%$DEBUG_REC == 0);
	   print FILE to_json($to_filt);
    }
}
print STDERR "generated output file $file\n";
print FILE "]";
close FILE;



#now print out results of all rules
print "";

# ----------subs------------
#
#sub to close the database handle
sub closeDBAndDie {
my $param = $_[0];
my $dbh = $_[1];
$dbh->disconnect or warn $dbh->errstr;
die $param;
}
sub valuesToString {
	my $hashRef = $_[0];
	my $returnValue = "";
	foreach my $myKey (sort keys %$hashRef) {
		$returnValue.="_".$hashRef->{$myKey};
	}
	return $returnValue;
}
sub copyHash {
	my $hashRef    = $_[0];
	my $colsToCopy = $_[1];
	
	my @cols;
	if($colsToCopy eq "*") {
	   return $hashRef;
	   #@cols = keys %$hashRef;	
	}
	else {
		my %return_hash;
		@cols = split(",", $colsToCopy);
		foreach my $col (@cols) {
			$return_hash{$col} = $hashRef->{$col};
		}
	}
}
sub uniqArrOfHash {
	my $arrRef    = $_[0];
	my %uniqVals;
	foreach my $el (@$arrRef) {
		my $uniqString = valuesToString($el);
		if(!defined($uniqVals{$uniqString})) {
			$uniqVals{$uniqString} = $el;
		}
	}
	return values %uniqVals;
}
sub saveSqlObj{
	my $dataStruct = $_[0];
    my ($fh, $filename) = tempfile(time() . "_" . rand().".bin", DIR => `pwd`, UNLINK => 0); 
    print("storing sql datastruct\n");
    store($dataStruct, $filename);
    print("end of storing sql datastruct, created file $filename\n");
}
sub loadObj {
  my $file = $_[0];
  my $ref = retrieve($file);
  return $ref;
}
sub loadData {
  my $file = $_[0];
  my $arrref = &loadObj($file);
#restore to global variables
 return $arrref; 
 
}
