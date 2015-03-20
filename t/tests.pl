use strict;
use warnings;

use Test::More tests => 8;
#use Test::Exception;

use lib 'lib/perl/DBEngine/';
use DBEngine;
use Data::Dumper;


my $db_engine = DBEngine::new($ARGV[0]);

#Test normal stuff
#ok($db_engine->CreateDatabase('test_db30'));
ok($db_engine->Connect('test_db30'));
#ok($db_engine->CreateTable('simple_text', [ {name => "ab", type => 1}, {name => 'bb', type => 2}]));
ok($db_engine->InsertInto('simple_text', {id=> 1,ab => 2, bb => 'stan'}));
ok($db_engine->InsertInto('simple_text', {id=>1, ab => 3, bb => 'kyle'}));
ok($db_engine->MapEntries('simple_text', 'select'));
ok(sub{ $db_engine->GetIndex('simple_text'); print "ok";});
ok(sub{ $db_engine->RefreshIndex('simple_text'); print "ok";});
ok(sub  {   Dumper $db_engine->GetIndex('simple_text');
            Dumper $db_engine->IndexSelect('simple_text', 1);
        });


# table / database duplication is not nice!
#diag { $db_engine->CreateDatabase('test_db30') } 'expecting to die';
#diag { $db_engine->CreateTable('simple_text', [ {name => "ab", type => 1}, {name => 'bb', type => 2}]) } 'expecting to die';


