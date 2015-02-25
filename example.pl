use strict;
use warnings;

use lib 'lib/perl/DBEngine/';
use DBEngine;
use Data::Dumper;


my $db_engine = DBEngine::new($ARGV[0]);
$db_engine->CreateDatabase('kor12');
