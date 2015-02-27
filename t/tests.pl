use strict;
use warnings;

use Test::Simple tests => 8;

use lib 'lib/perl/DBEngine/';
use DBEngine;
use Data::Dumper;


my $db_engine = DBEngine::new($ARGV[0]);

ok($db_engine->CreateDatabase('test_db1'));
ok($db_engine->Connect('test_db1'));
ok($db_engine->CreateTable('simple_text', [ {name => "ab", type => 1}, {name => 'bb', type => 2}]));
ok($db_engine->InsertInto('simple_text', {ab => 2, bb => 'stan'}));
ok($db_engine->InsertInto('simple_text', {ab => 3, bb => 'kyle'}));

ok(print Dumper $db_engine->GetEntriesByValue('simple_text', [ {column_name => 'ab', desired_value => 3, compare_by => '=='}]));
#print Dumper $db_engine->GetEntriesByValue('simple_text', [{column_name => 'ab', desired_value => 3}, {column_name => 'ab', desired_value => 2}]);
ok(print Dumper $db_engine->GetEntriesByValue('simple_text'));
ok($db_engine->DeleteEntireTable('simple_text'));
#$db_engine->DropTable('simple_text');
#$db_engine->DropDatabase('my_db');



