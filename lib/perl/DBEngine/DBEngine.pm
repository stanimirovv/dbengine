package DBEngine;

use strict;
use warnings;

use Data::Dumper;

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

sub PackInteger($$)
{
    my ($fh, $integer) = @_;
    
    ASSERT(defined $integer, "Undefined integer!");
    return pack('i', int($integer)); #dies if not a number
}

sub UnpackInteger($)
{
    my ($fh) = @_;

    #ASSERT(defined $bytes, "Undefined bytes to unpack!");
    #return unpack('i', $bytes); #dies if not a number 
}

sub PackText($$)
{
    my ($fh, $text) = @_;

}

sub UnpackText($)
{
    my ($fh) = @_;

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
    open $$self{meta}, "<", "$$self{root}$$self{connection}/$$self{connection}"."_meta" or die "Cloud not read meta file. Does $$self{connection}_meta exist ? Are permissions ok?".$!;

}

sub Disconnect($)
{
    my ($self) = @_;

    ASSERT(defined $self);
    ASSERT(defined $$self{meta});

    $$self{meta}->close() or die "Cloud not close the meta fh!  ".$!;

}

=documentation
@paramIN search_for_table_name
@return returns undefined if there is no such table or a hashref
the hashref has 2 keys - name and columns. The columns are a hashref which
contains as key the column name and as value the int type of the column
=cut

sub GetTableDetails($;$)
{
    my ($self, $search_for_table_name) = @_;

    ASSERT(defined $self, ' undefined self');
    if(defined $search_for_table_name)
    {
        ASSERT($search_for_table_name ne '');
    }

    while(1)
    {
        my $table  = { columns => []};
        
        my $bytes;
        my $bytes_read = read($$self{meta}, $bytes, 4);
        ASSERT(defined $bytes_read, "Error in reading the bytes!");
        if($bytes_read == 0)
        {
            return;
        }
    
        # Every table starts and ends with a zero
        my $unpacked_bytes = unpack('i', $bytes);
        ASSERT($unpacked_bytes == 0); 

        $bytes_read = read($$self{meta}, $bytes, 4);
        ASSERT($bytes_read == 4, 'Error when reading the table length(name)');    
        my $table_name_length = unpack('i', $bytes) or die "Can't unpack the table name's length";
        print "Table name length: $table_name_length \n";
    
        $bytes_read = read($$self{meta}, $bytes, $table_name_length);
        print "bytes, read = $bytes_read \n";
        ASSERT($bytes_read > 0, " Bad db format!");
        my $table_name = unpack("a$bytes_read", $bytes); 
        print "Table name is: $table_name \n"; 
    
        $$table{name} = $table_name;

        my $first_iter = 1;
        my $columns = {};

        #read all columns
        while(1)
        {    
            $bytes_read = read($$self{meta}, $bytes, 4);
            ASSERT($bytes_read > 0, "Bad db format! (column type)");
            my $column_type = unpack('i', $bytes);
            print "Column type is : $column_type \n";
            if($column_type == 0)
            {
                last;
            }
            
            $bytes_read = read($$self{meta}, $bytes, 4);
            if(!$first_iter && $bytes_read == 0)
            {
                last; 
            }
            ASSERT($bytes_read > 0, "Bad db format! (column name length)");
            my $column_name_length = unpack('i', $bytes);
            $bytes_read = read($$self{meta}, $bytes, $column_name_length);
            ASSERT($bytes_read > 0, "Bad db format! (column name value)");

            my $column_name = unpack("a$column_name_length", $bytes);
            print "Column name is: $column_name \n";
         
            $$columns{$column_name} = $column_type;
            $first_iter = 0;
        }

        push(@{$$table{columns}}, $columns); 

        if($$table{name} eq $search_for_table_name)
        {
            return $table;
        }
    }
}

=pod
    Creates a table. If a table with the name exists, it throws an exception.
@paramIN table_schema - an array reference which contains hashrefs of the following structure:
    {
        name => 'string' which contains the name
        type => int which contains the index of the type. The index of the types can be seen
                in the beginning of the module
    }
    both elements should be present, or operation will be aborted.

    To the disc the information is written in the following way: zero table_name column_type column_name_length column_name ... zero
=cut
#TODO write only if the entire table is ok with the encoding. Since it will damage the wannabe database

sub CreateTable($$$)
{
    my ($self, $table_name, $table_schema) = @_;

    ASSERT(defined $self);
    ASSERT(defined $$self{connection}, "You are not connected to a database!");
    ASSERT(defined $table_name, ' No name specified for the table!');
    ASSERT($table_name ne '', 'A table can not have an empty name!');
    ASSERT($table_name ne "$$self{connection}_meta", "This name is reserved!");
    ASSERT(defined $table_schema, 'A table needs to have a schema!');
    ASSERT($table_name !~ m/\s/g, 'Spaces are not allowed in the table name!\n');

    my $table = $self->GetTableDetails($table_name);
    print Dumper $table;
    ASSERT(!defined($table), "Table alredy exists!");

    seek($$self{meta}, 0, 0);
    
    # all databases are stored in the meta file
    my $fh;
    open $fh, ">>", "$$self{root}$$self{connection}/$$self{connection}"."_meta" or die "Cloud not read meta file. Does $$self{connection}_meta exist ? Are permissions ok?".$!;
    $| = 1;
    my $table_name_length = length($table_name);

    # Every table definition starts with a zero
    my $bytes = pack("i i a$table_name_length", 0, $table_name_length, $table_name) or die "Cloud not pack new table name!";
    print $fh $bytes or die " ".$!;
    
    #add every column
    for my $column (@$table_schema)
    {
        ASSERT(defined $$column{name}, " Undefined column name");
        ASSERT(defined $$column{type}, " Undefined column type");
    
        my $column_name_length = length($$column{name});
        $bytes = pack("i i a$column_name_length", $$column{type}, $column_name_length, $$column{name}) or die " Cloud not pack new column!";
        print $fh $bytes or die " ".$!;
    } 

    # Every table definition starts and ends with a zero
    $bytes = pack("i", 0);
    print $fh $bytes or die " ".$!;
    close $fh or die "cloud not close filehandle! ".$!;

    # Database data file
    open($fh, '>', "$$self{connection}/$table_name") or die "Can't create database file!".$!;
    close($fh) or die $!;
}

sub DropTable($$)
{
    my ($self, $table_name) = @_;

    ASSERT(defined $self);
    ASSERT(defined $table_name, ' No table specified!');

    ASSERT(unlink("$$self{connection}/$table_name"), ' Error when dropping the table'.$!);

}

sub InsertInto($$$)
{
    my ($self, $table, $values) = @_;
    ASSERT(defined $table);
    ASSERT(defined $values);
    ASSERT(ref($values) eq 'HASH', "The passed values is not in the correct format!");

    my $table_info = $self->GetTableDetails($table);
    
    ASSERT(defined $table_info, "The table in which an insert is attempted doesn't exist");
     

}

=pod
@param $value a hashref with one key. The key is the column. 
The value is the value which the column must be equal to
If the value is undefined the entire table will be returned.
=cut

sub GetEntryByValue($$$)
{
    my ($self, $table_name, $value) = @_;

}

sub GetEntryByExpression()
{

}

sub DeleteEntireTable()
{

}

sub DeleteEntryByValue()
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
