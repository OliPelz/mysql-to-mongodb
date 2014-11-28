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
my %rule;
my $blockCount = 0;
my $relationCount = 0; 
my $ruleCount = 0;
#first: read in config file
while($line = <CONFIGFILE>) {
	if($line =~ /^\n$/) {
		$blockCount++;
		$relationCount = 0;
		#dont start until first rule block has been read in
		if(defined($rule{$ruleCount}{"filter"})) {
			$ruleCount++;
		}
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
		if($line =~ /^(oneToOne|oneToMany|manyToMany),(\w+):(\w+),(\w+):(\w+),(\w+)$/) {
			$rule{$ruleCount}{"relation"}{$relationCount} = $1;
			$rule{$ruleCount}{"to_id"}{$relationCount} = $2;
			$rule{$ruleCount}{"to_join"}{$relationCount} = $2;
			$rule{$ruleCount}{"from_id"}{$relationCount} = $3;
			$rule{$ruleCount}{"from_join"}{$relationCount} = $3;
			$rule{$ruleCount}{"embedCol"}{$relationCount} = $4;	
			$relationCount++;
		}

		if($line =~ /^(\w+)=([_\w]+)$/) {
			$rule{$ruleCount}{"colFilters"}{$1} = $2;
		}
	}
}
close CONFIGFILE;

#next: query all table information from sql database and put in datastructure
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


# next filter all sql data tables by the rules given in cfg
# and make unique datastructures
# hashes contain all the rules from the cfg file
my %uniq_tos;
my %uniq_from;

foreach my $ruleCount (sort keys %rule) {
	#xtract needed data
	my $relation = $rule{$ruleCount}{"relation"};
	my $to_id   = $rule{$ruleCount}{"to_id"};
	my $from_id = $rule{$ruleCount}{"from_id"};
	my $to_join   = $rule{$ruleCount}{"to_join"};
	my $from_join = $rule{$ruleCount}{"from_join"};
	my $embedCol  = $rule{$ruleCount}{"embedCol"};
	my %colFilters   = %{$rule{$ruleCount}{"colFilters"}};
	
	# filter "to" table by colsTo
	# and make uniq
	foreach my $to (@{$sql{$from_id}{"data"}}) {
		#the filtered hash
		my $filter = $colsFilters
		my $to_new   = &copyHash($to, $colsTo);
		#make unique
		my $value_string = &valuesToString($to_new);
		if(!defined($uniq_to{$value_string})) {
			$uniq_to{$ruleCount}{$value_string}{"data"} = $to_new;
			$uniq_to{$ruleCount}{$value_string}{"embed_to_id"}   = $to_join;
		}   	
	}
	# filter "from" table by colsFrom
	# and make uniq
	foreach my $from (@{$sql{$from_id}{"data"}}) {
		#filter hash
		my $from_new   = &copyHash($from, $filters{});
		#make unique
		my $value_string = &valuesToString($from_new);
		if(!defined($uniq_from{$value_string})) {
			$uniq_from{$rule_cnt}{$value_string}{"data"} = $from_new;
			$uniq_from{$rule_cnt}{$value_string}{"embed_from_id"}   = $from_join;
		}   	
	}
}
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
		$returnValue.="_".$hashRef->{$myKey} || "";
	}
	return $returnValue;
}
sub copyHash {
	my $hashRef    = $_[0];
	my $colsToCopy = $_[1];
	
	my @cols;
	if($colsToCopy eq "*") {
	   @cols = keys %$hashRef;	
	}
	else {
	   @cols = split(",", $colsToCopy);	
	}
	my %return_hash;
	foreach my $col (@cols) {
		$return_hash{$col} = $hashRef->{$col} || "";
	}
	return \%return_hash;
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
