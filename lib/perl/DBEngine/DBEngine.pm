package DBEngine;

use strict;
use warnings;

my $data_types =    {
                        integer =>  {
                                        encoding_value  => 1,
                                        constraints     => undef,
                                        encode          => 12,
                                        decode          => 31,
                                    },
                        text    =>  {
                                        encoding_value  => 2,
                                        constraints     => undef,
                                        encode          => 12,
                                        decode          => 31,
                                    },
                        boolean =>  {
                                        encoding_value  => 3,
                                        constraints     => \&BooleanConstraints,
                                        encode          => 12,
                                        decode          => 31,
                                    },
                    };
                    

sub BooleanConstraints($)
{
    my ($val) = @_;

    ASSERT(defined $val);
    ASSERT($val == 1 ||  $val == 0, 'Bad value for the boolean!');
}

sub PackInteger($)
{
    my ($integer) = @_;
    
    ASSERT(defined $integer, "Undefined integer!");
    return pack('i', int($integer)); #dies if not a number
}

sub UnpackInteger($)
{
    my ($bytes) = @_;

    ASSERT(defined $bytes, "Undefined bytes to unpack!");
    return unpack('i', $bytes); #dies if not a number 
}

sub PackText($)
{

}

sub UnpackText()
{

}

=documentation
@paramIN root the path in which the databases are stored
=cut

sub new($)
{
    my ($root) = @_;

    ASSERT(defined $root, 'undefined root!');
    ASSERT($root ne '', "undefined database root!");

    my $self = {root => $root};

    bless $self;
    return $self;
}

sub CreateDatabase($$)
{
    my ($self, $dbname) = @_;

    ASSERT(defined $self);
    ASSERT(defined $dbname);
    ASSERT(!(-d $dbname), "A database with that name already exists!");

    mkdir $dbname;
    my $fh; #dummy fh
    open $fh, '>', "$dbname/$dbname"."_meta" or die 'Cloud not create meta file. Deleting db directory.';
    close $fh;
}

sub DropDatabase($$)
{
    my ($self, $dbname) = @_;

    ASSERT(defined $self);
    ASSERT(defined $dbname);

    `rm -rf $dbname` or die 'cloud not delete database';
}

sub Connect($$)
{
    my ($self, $dbname) = @_;

    ASSERT(defined $dbname, 'undefined database to connect to!');
    ASSERT($dbname ne '', 'empty string for dbname is not acceptable!');

    $$self{connection} = $dbname;
}


=documentation

=cut
#TODO Major todo

sub CreateTable($$$)
{
    my ($self, $table_name, $table_schema) = @_;

    ASSERT(defined $self);
    ASSERT(defined $table_name, ' No name specified for the table!');
    ASSERT($table_name ne '', 'A table can not have an empty name!');
    ASSERT($table_name ne "$$self{connection}_meta", "This name is reserved!");
    ASSERT(defined $table_schema, 'A table needs to have a schema!');

    my $fh; #dummy fh
    #nonsense...
    open $fh, '<', "$$self{connection}/$table_name"."_meta" or die "Cloud not read meta file. Does $$self{connection}_meta exist ? Are permissions ok?";
    #todo read the table's names

    close $fh;
}

sub DropTable($$)
{
    my ($self, $table_name);

    ASSERT(defined $self);
    ASSERT(defined $table_name, ' No table specified!');

    ASSERT(unlink("$$self{connection}/$table_name"), ' Error when dropping the table'.$!);

}

sub InsertInto()
{

}

sub GetEntireTable()
{

}

sub GetEntryById()
{

}

sub GetEntryByExpression()
{

}

sub DeleteEntireTable()
{

}

sub DeleteEntryById()
{

}

sub DeleteEntriesByExpression()
{

}


sub ASSERT($;$)
{
    my ($expression, $message) = @_;

    if(!$expression)
    {
        die "ASSERT FAILED! Line:", (caller(0))[2], ,"  ", $message, "\n";
    }
}

1;
