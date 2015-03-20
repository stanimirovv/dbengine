use strict;
use warnings;

use lib 'lib/perl/DBEngine/';
use DBEngine;
use Data::Dumper;
use Time::HiRes qw( time );


my $db_engine = DBEngine::new($ARGV[0]);

#$db_engine->CreateDatabase('my_test2');
$db_engine->Connect('my_test2');
#$db_engine->CreateTable('shalalala', [ {name => "a", type => 1},{name => 'b', type => 1}]);

#$db_engine->CreateTable('simple_text', [ {name => "a", type => 1},
#{name => 'b', type => 2}]);
#$db_engine->InsertInto('simple_text', {id => 10, a => 4, b => "hello bc"});

#$db_engine->InsertInto('shalalala', {id => 1, a => 4, b => 31});
my $time = time;


#$db_engine->MapEntries('simple_text', 'update', [{column_name => 'a', desired_value => 50, compare_by => '=='}], {new_row => {a => 3, b => 3, c => 3, d =>3, e=>3}});
#$db_engine->MapEntries('simple_text', 'update', [{column_name => 'a', desired_value => 50, compare_by => '=='}], {new_row => {a => 4, b => 4, c => 4, d=>4, e =>4}});
#$db_engine->DeleteRows('simple_text', [{column_name => 'ab', desired_value => 10, compare_by => '>'}]);
#print Dumper $db_engine->MapEntries('shalalala', 'select');
$db_engine->GetIndex('simple_text');
#$db_engine->RefreshIndex('simple_text');
#print Dumper $db_engine;

#print Dumper $db_engine->SearchInIndex('shalalala', 10);
#print Dumper $db_engine;
print Dumper $db_engine->IndexSelect('simple_text', 10);


#$db_engine->MapEntries('simple_text', 'delete');
#print Dumper $db_engine->MapEntries('simple_text', 'select');

#print Dumper $db_engine->GetEntriesByValue('simple_text', [ {column_name => 'ab', desired_value => 6, compare_by => '<'}]);
#print Dumper $db_engine->GetEntriesByValue('simple_text', [{column_name => 'ab', desired_value => 3}, {column_name => 'ab', desired_value => 2}]);
#print Dumper $db_engine->GetEntriesByValue('simple_text');
#$db_engine->TruncateTable('simple_text');
#$db_engine->DropTable('simple_text');
#$db_engine->DropDatabase('my_db');
#

print "total time:  ", time - $time, "\n";

$db_engine->Disconnect();
