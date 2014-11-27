#!/usr/bin/perl
use DBI;
use DBD::mysql;
use JSON;
use strict;
use warnings;

#usage ./convert_sql_to_mongo_embed.pl <db user> <db name> <dp passwd> <db_host> <db_port> <input file> <output dir>



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

my $out_handle;

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
my %embed;
my $blockCount = 0; # counter for blocks  (blocks are separated by newline)
my $embedCount = 0;
#read in config file
while($line = <CONFIGFILE>) {
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
		}
	}
	#embedd block
	elsif($blockCount > 0) {
		if($line =~ /^(\w+):([_\w]+)$/ && !defined($embed{$embedCount}{"embed_to_id"})) {
			$embed{$embedCount}{"embed_to_id"} = $1;
			$embed{$embedCount}{"embed_to_col"} = $2;
		}
		#embed_to always comes before embed_from
		elsif($line =~ /^(\w+):([_\w]+)$/ && defined($embed{$embedCount}{"embed_to_id"})) {
			$embed{$embedCount}{"embed_from_id"} = $1;
			$embed{$embedCount}{"embed_from_col"} = $2;
		}
		#TODO: manyToMany not implemented yet
		elsif($line =~ /^(oneToOne|oneToMany|manyToMany)$/) {
			$embed{$embedCount}{"relation"} = $1;
		}
		elsif($line =~ /^([\w_]+)$/) {
			$embed{$embedCount}{"columnName"} = $1;
			$embedCount++;
		}
	}
}
close CONFIGFILE;

#next query all data tables from sql
foreach my $id (keys %sql) {
	my $sql_string = $sql{$id}{"query"};
	$sth = $dbh->prepare($sql_string) or closeDBAndDie("Couldn't prepare statement");
	$sth->execute() or closeDBAndDie("Couldn't connect to database: ");
	my @table_data;
	while (my $hash_ref = $sth->fetchrow_hashref) { 
	    push @table_data, $hash_ref;
	}
	$sql{$id}{"data"} = \@table_data;
}
#last embed stuff (sort i use for better debugging)
my %embed_hash; # keeps information about embedding while we loop the hash
foreach my $cnt (sort keys %embed) {
	#xtract needed data
	my $relation = $embed{$cnt}{"relation"};
	my $colName = $embed{$cnt}{"columnName"};
	my $embed_to_id = $embed{$cnt}{"embed_to_id"};
	my $embed_to_col = $embed{$cnt}{"embed_to_col"};
	my $embed_from_id = $embed{$cnt}{"embed_from_id"};
	my $embed_from_col = $embed{$cnt}{"embed_from_col"};
	
	#see if we can "join" the tables for connection/embedding
	foreach my $to (@{$sql{$embed_to_id}{"data"}}) {
		my $embed_container = undef;
		foreach my $from (@{$sql{$embed_from_id}{"data"}}) {
			if($to->{$embed_to_col} eq $from->{$embed_from_col}) {
				if($relation eq "oneToOne") { #oneToOne is scalar
				    $embed_container = $from;
			    }
			    elsif($relation eq "oneToMany"){ #oneToMany is array
			       push @{$embed_container}, $from;
		        } 
	        } 
       }
#after all is collected in the embed_container we have to embedd it actually in the colname 
    $to->{$colName} = $embed_container;
	print "";
    }
}


foreach my $id (sort keys %sql) {
	my $data = $sql{$id}{"data"};
	my $file = $ARGV[6]."/".$id.".json";
	open(OUT, ">", $file) || die "cannot open file to write $file";
	print OUT to_json($data);
	close(OUT);
	print STDOUT "wrote output file $file\n";
}


#while (my @arr = $sth->fetchrow_array) {
#    print $out_handle to_json(\@arr);
#}

$dbh->disconnect;



#sub to close the database handle
sub closeDBAndDie {
my $param = $_[0];
my $dbh = $_[1];
$dbh->disconnect or warn $dbh->errstr;
die $param;
}
