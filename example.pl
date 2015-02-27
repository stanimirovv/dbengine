use strict;
use warnings;

use lib 'lib/perl/DBEngine/';
use DBEngine;
use Data::Dumper;

my $db_engine = DBEngine::new($ARGV[0]);

$db_engine->CreateDatabase('my_test');
$db_engine->Connect('my_test');
$db_engine->CreateTable('simple_text', [ {name => "ab", type => 1}, {name => 'bb', type => 2}]);
$db_engine->InsertInto('simple_text', {ab => 2, bb => 'stan'});
$db_engine->InsertInto('simple_text', {ab => 3, bb => 'kyle'});

print Dumper $db_engine->GetEntriesByValue('simple_text', [ {column_name => 'ab', desired_value => 3, compare_by => '=='}]);
#print Dumper $db_engine->GetEntriesByValue('simple_text', [{column_name => 'ab', desired_value => 3}, {column_name => 'ab', desired_value => 2}]);
print Dumper $db_engine->GetEntriesByValue('simple_text');
$db_engine->DeleteEntireTable('simple_text');
#$db_engine->DropTable('simple_text');
$db_engine->DropDatabase('my_db');
$db_engine->Disconnect();



