use strict;
use warnings;

use lib 'lib/perl/DBEngine/';
use DBEngine;
use Data::Dumper;

my $db_engine = DBEngine::new($ARGV[0]);

#$db_engine->CreateDatabase('my_test3');
$db_engine->Connect('my_test3');
#$db_engine->CreateTable('simple_text', [ {name => "a", type => 1},
#{name => 'b', type => 1},{name => 'c', type=> 2}, {name => 'd', type => 2}, {name => 'e', type => 2}]);
my $time = time;

#$db_engine->InsertInto('simple_text', {ab => 4, bb => '12312312'});

$db_engine->MapEntries('simple_text', 'update', [{column_name => 'a', desired_value => 50, compare_by => '=='}], {new_row => {a => 3, b => 3, c => 3, d =>3, e=>3}});
#$db_engine->MapEntries('simple_text', 'update', [{column_name => 'a', desired_value => 50, compare_by => '=='}], {new_row => {a => 4, b => 4, c => 4, d=>4, e =>4}});
#$db_engine->DeleteRows('simple_text', [{column_name => 'ab', desired_value => 10, compare_by => '>'}]);
#$db_engine->MapEntries('simple_text', 'select');
#print Dumper $db_engine->GetEntriesByValue('simple_text', [ {column_name => 'ab', desired_value => 6, compare_by => '<'}]);
#print Dumper $db_engine->GetEntriesByValue('simple_text', [{column_name => 'ab', desired_value => 3}, {column_name => 'ab', desired_value => 2}]);
#print Dumper $db_engine->GetEntriesByValue('simple_text');
#$db_engine->TruncateTable('simple_text');
#$db_engine->DropTable('simple_text');
#$db_engine->DropDatabase('my_db');
#

print "total time:  ", time - $time, "\n";

$db_engine->Disconnect();
