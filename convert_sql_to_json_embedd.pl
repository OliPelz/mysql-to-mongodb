#!/usr/bin/perl
use DBI;
use DBD::mysql;
use JSON;
use strict;
use warnings;

#usage ./convert_sql_to_mongo_embed.pl <db user> <db name> <dp passwd> <db_host> <db_port> <output file> <sql statement> [<relation embed type><id_connect_a_b_merged_with _to_> <sql_to_embed> <id_connect_a_b> <sql_to_embed> ....]
#e.g. 

#perl ~/Git/mysql-to-mongodb/convert_sql_to_json_embedd.pl root genome_rnai_universal_v14_tuning \
#<secretPassword> \
#localhost \
#3306 \
#`date +%Y-%m-%d`/phenotypeNamesOrganism.json \
#"SELECT E.ExperimentId as embedExperiment, E.OrganismId as embedOrgId, P.name as phenoName, O.organismName as organismName \
#FROM OPTIMIZED_PhenotypeSearch P \
#inner join OPTIMIZED_PhenotypeSearchExperiment E \
#on P.id = E.PhenoSearchId \
#inner join Organism O \
#on E.OrganismId = O.id \
#order by P.name" \
#embedExperiment_id \
#"select * from Experiment \
#oneToMany \
#embedOrgId_to_id \
#select * from Organism


#connect to database
my ($db_user, $db_name, $db_pass, $db_host, $db_port) = ($ARGV[0], $ARGV[1],$ARGV[2],$ARGV[3],$ARGV[4]);
my $dbh = DBI->connect("DBI:mysql:$db_name;host=$db_host;port=$db_port","$db_user","$db_pass")
or closeDBAndDie("Couldn't connect to database: ");

my $sql_string = $ARGV[6];

my $out_handle;
open($out_handle, ">", $ARGV[5]) || die "cannot open output file ".$ARGV[5];

###set encoding 'n stuff
my $sth = $dbh->prepare("SET NAMES 'utf8'")
or closeDBAndDie("Couldn't prepare statement");
$sth->execute() or closeDBAndDie("Couldn't connect to database: ");
$sth = $dbh->prepare("SET FOREIGN_KEY_CHECKS=0")
or closeDBAndDie("Couldn't prepare statement");
$sth->execute() or closeDBAndDie("Couldn't connect to database: ");

$sth = $dbh->prepare($sql_string) or closeDBAndDie("Couldn't prepare statement");
$sth->execute() or closeDBAndDie("Couldn't connect to database: ");
my @result_rows;
while (my $hash_ref = $sth->fetchrow_hashref) { 
    push @result_rows, $hash_ref;
}
if(scalar @ARGV > 6) {
	for(my $i = 7; $i < scalar @ARGV; $i+=3) {
		die "must define an sql statement if additional embedding parameters are defined" 
		   if(!defined($ARGV[$i+1])|| $ARGV[$i+1] eq "" || $ARGV[$i+2] eq "");
		my $relation = $ARGV[$i];
		my $leftId;
		my $rightId;
		if($ARGV[$i+1] =~ /(\w+)_to_(\w+)/) {
		  $leftId = $1;
          $rightId = $2;
 		}
		else {
			die "parameter _to_ not in the right format";
		}

		my $sql_string         = $ARGV[$i+2];
		$sth = $dbh->prepare($sql_string) or closeDBAndDie("Couldn't prepare statement");
		$sth->execute() or closeDBAndDie("Couldn't connect to database: ");
		my @embed_table;
		while (my $hash_ref = $sth->fetchrow_hashref) { 
		    push @embed_table, $hash_ref;
		}
		#now make the actual embed
		foreach my $left_row (@result_rows) {
			my $found = 0;
			#we need to backup this
			my $left_contentId = $left_row->{$leftId};
			foreach my $right_row (@embed_table) {
				if($left_contentId  eq $right_row->{$rightId}) {
					if($relation eq "oneToOne") {
					   $left_row->{$leftId} = $right_row;
					}
					elsif ($relation eq "oneToMany") { 
					   if (ref $left_row->{$leftId} ne 'ARRAY') {
						   $left_row->{$leftId} = [];
					   }  
 					   push @{$left_row->{$leftId}}, $right_row; 
					}
					$found = 1;
				}
			}
			if(!$found) {
				$left_row->{$leftId} = undef;
				print STDERR "field ".$left_row->{$leftId}." with id $leftId not found when embedding\n";
			}
		}
    }
}

print $out_handle to_json(\@result_rows);


#while (my @arr = $sth->fetchrow_array) {
#    print $out_handle to_json(\@arr);
#}



$dbh->disconnect;
close $out_handle;



#sub to close the database handle
sub closeDBAndDie {
my $param = $_[0];
my $dbh = $_[1];
$dbh->disconnect or warn $dbh->errstr;
die $param;
}
