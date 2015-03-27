package DBEngine;

use strict;
use warnings;

use Data::Dumper;
use Try::Tiny;
use Fcntl qw(:flock SEEK_END);

#TODO add constraints
#TODO add type that uses constraints
#TODO expand tests
#TODO move the table meta in the beginning of the table's file
#TODO stop using magic variables

#TODO fix the corrupted data
#TODO fix the multi table issue.

=pod
    Below are the definitions for every implemented data type.
=cut

my $data_types =    {
                        1   =>  {
                                        name            => 'integer',
                                        constraints     => undef,
                                        pack            => \&PackInteger,
                                        unpack          => \&UnpackInteger,
                                        compare         => \&CompareIntegers,

                                    },
                        2   =>  {
                                        name            => 'text',
                                        constraints     => undef,
                                        pack            => \&PackText,
                                        unpack          => \&UnpackText,
                                        compare         => \&CompareText,
                                    },

                        3   =>  {
                                        name            => 'positive_int',
                                        constraints     => \&BooleanConstraints,
                                        pack            => 12,
                                        unpack          => 31,
                                        compare         => 'TODO',
                                    },
                    };


my $data_manipulate_methods = { };

################################ TYPES ########################################

=documentation
    All compare functions will have the following params:
@parmIN first instance of type to be compared
@paramIN second instance of type to be compared
@paramIN how they should be compared (<, >, ==)
@return 1 if true, 0 if false
=cut

sub CompareIntegers($$$)
{
    my ($a, $b, $by) = @_;

    ASSERT(defined $a);
    ASSERT(defined $b);
    ASSERT(defined $by);
    ASSERT($by eq "<" || $by eq "==" || $by eq ">", "BAD FORMAT FOR COMPARE INT");

    if($by eq "<")
    {
        return $a < $b;
    }
    if($by eq ">")
    {
        return $a > $b;
    }
    if($by eq "==")
    {
        return $a == $b;
    }

    ASSERT(0);
}

sub CompareText($$$)
{
    my ($a, $b, $by) = @_;

    ASSERT(defined $a);
    ASSERT(defined $b);
    ASSERT(defined $by);
    ASSERT($by eq "<" || $by eq "==" || $by eq ">", "BAD FORMAT FOR COMPARE INT");

    if($by eq "<")
    {
        return $a lt $b;
    }
    if($by eq ">")
    {
        return $a gt $b;
    }
    if($by eq "==")
    {
        return $a eq $b;
    }

    ASSERT(0);
}


=documentation
    All constraints methods will take the same arguments:
@paramIn    The value which must be validated.
@paramIn optional file handle to the table in which is inserted (to check for unique for example)
should be inserted
=cut
#TODO implement it

sub BooleanConstraints($;$)
{
    my ($val) = @_;

    ASSERT(defined $val);
    ASSERT($val == 1 ||  $val == 0, 'Bad value for the boolean!');
}

=documentation
    All pack functions return the same thing
@return a list of scalars containing the bytes of the different part of the string.
=cut

sub PackInteger($)
{
    my ($integer) = @_;

    ASSERT(defined $integer, "Undefined integer!");

    #It is an array ref since most types will contain more than one field
    #For example text has two - length and data
    my $ref = ();
    push(@$ref, pack('i', int($integer))); #dies if not a number
    return $ref;
}

=documentation
    Since meta information will be stored in the beginning of each row it is a good idea to
    store the position of the beginning of the columns in order to be able to update them without
    rewriting the entire file.
@param positions - array ref containing the positions of each column
=cut

sub UnpackInteger($;$)
{
    my ($fh, $positions) = @_;

    my $file_pos = tell($fh);
    my $bytes;
    my $bytes_read = read($fh, $bytes, 4);
    ASSERT(defined $bytes_read, " Cloud not read from file");
    if($bytes_read == 0)
    {
        die "EOF";
    }
    push(@$positions, $file_pos);
    return unpack('i', $bytes); #dies if not a number
}

sub PackText($)
{
    my ($text) = @_;

    ASSERT(defined $text, "Undefined text!");

    my $text_length = length($text);

    my $ref = ();
    push(@$ref, pack('i', $text_length)); #dies if not a number
    push(@$ref, pack("a$text_length", $text));
    return $ref;

}

sub UnpackText($$)
{
    my ($fh, $positions) = @_;

    my $file_pos = tell($fh);
    my $bytes;
    my $bytes_read = read($fh, $bytes, 4);
    ASSERT(defined $bytes_read, " Cloud not read from file");
    if($bytes_read == 0)
    {
        die "EOF";
    }
    my $text_length = unpack('i', $bytes) or die "Error when unpacking int".$!;
    $bytes_read = read($fh, $bytes, $text_length);
    if($bytes_read == 0)
    {
        die "EOF";
    }
    push(@$positions, $file_pos);

    return unpack("a$text_length", $bytes); #dies if not a number

}

=documentation
The "InternalValue" may change. Currently it is an integer and it is used
when statuses are needed.

List of statuses:
0 Row is ok
1 Row is deleted
2 Row is still inserted
3 Row is staged for removing, but the row that must be inserted on it's place isn't inserted yet.
=cut

sub PackInternalValue($)
{
    my ($internal_value) = @_;

    ASSERT(defined $internal_value, "Undefined integer!");

    #It is an array ref since most types will contain more than one field
    #For example text has two - length and data
    my $bytes;
    $bytes = pack('i', int($internal_value)); #dies if not a number
    return $bytes;;
}

############################# END TYPES #######################################

############################# API #############################################


=documentation
@paramIN root the path in which the databases are stored
=cut

sub new($)
{
    my ($root) = @_;

    ASSERT(defined $root, 'undefined root!');
    ASSERT($root ne '', "undefined database root!");

    my $self = {root => $root, indexes => {}};

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

=documentation
Dropping is irreversable.
=cut
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
Currently this function does nothing.
=cut

sub Disconnect($)
{
    my ($self) = @_;

    ASSERT(defined $self);
}


=documentation
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

    # Make sure the columns don't have name of internal columns
    for my $column (@$table_schema)
    {
        ASSERT(defined $$column{name}, " Undefined column name");
        ASSERT(defined $$column{type}, " Undefined column type");
        if($$column{name} eq "id" || $$column{name} eq "row_status")
        {
            die "You are using reserved column names. Currently id and row_status are reserved!";
        }
    }
    seek($$self{meta}, 0, 0);

    # all databases are stored in the meta file
    my $fh;
    open $fh, ">>", "$$self{root}/$$self{connection}/$$self{connection}"."_meta" or die "Cloud not read meta file. Does $$self{connection}_meta exist ? Are permissions ok?".$!;
    $| = 1;
    my $table_name_length = length($table_name);

    # Every table definition starts with a zero
    my $bytes = pack("i i a$table_name_length", 0, $table_name_length, $table_name) or die "Cloud not pack new table name!";
    print $fh $bytes or die " ".$!;

    #column with meta data storing if the row is deleted or not.
    unshift(@$table_schema, {name => "id", type => 1});
    unshift(@$table_schema, {name => "row_status", type => 1});

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
    # The first 4 bytes (integer) determine the position of the last row that started being inserted
    # If it's value is 0 it means that all rows are successfully inserted
    print $fh $bytes or die "".$!;
    close($fh) or die $!;

    #INDEX
    open $fh, ">>", "$$self{root}/$$self{connection}/$table_name"."_index" or die "Cloud not read meta file. Does $$self{connection}_meta exist ? Are permissions ok?".$!;
    close $fh or die "Cloud not close file...\n";
}

sub DropTable($$)
{
    my ($self, $table_name) = @_;

    ASSERT(defined $self);
    ASSERT(defined $table_name, ' No table specified!');
    ASSERT(unlink("$$self{connection}/$table_name"."_index"), ' Error when dropping the table'.$!);
    ASSERT(unlink("$$self{connection}/$table_name"), ' Error when dropping the table'.$!);

}

=documentation
@paramIN values a hashref containing as keys the name of the columns and as values the values
that should be inserted.
No null fields are allowed!
=cut

sub InsertInto($$$)
{
    my ($self, $table, $values) = @_;
    ASSERT(defined $table);
    ASSERT(defined $values);
    ASSERT(ref($values) eq 'HASH', "The passed values is not in the correct format!");

    my $table_info = $self->GetTableDetails($table);
    my $fh;
    my $fh1;
    open($fh, '>>', "$$self{connection}/$table") or die " Cloud not open the table".$!;

    flock($fh, LOCK_EX) or die "Cloud not lock file!";
    ASSERT(defined $table_info, "The table in which an insert is attempted doesn't exist");
    open $fh1, ">>", "$$self{root}/$$self{connection}/$table"."_index" or die "Cloud not read meta file. Does $$self{connection}_meta exist ? Are permissions ok?".$!;
    flock($fh1, LOCK_EX);
    #ASSERT(scalar(keys(%{$$table{columns}}) == scalar(keys(%$values), "The number of columns to insert doesn't match the number of columns in the table");

    # make sure the row isn't inserted as deleted.
    # view PackInternalValue for status definitions
    $$values{row_status} = 2;

    my @bytes = ();
    for my $key (@{$$table_info{columns_names}})
    {
        ASSERT(defined $$values{$key}, "Undefined values are not supported... yet... \n");


        # Handles the index
        if($key eq 'id')
        {

            my @bytes_inner = ();
            push @bytes_inner, PackInteger($$values{$key});
            push @bytes_inner, PackInteger(tell($fh));
            for my $vals_bytes (@bytes_inner)
            {
                for my $bytes (@$vals_bytes)
                {
                    print $fh1 $bytes;
                }
            }
            close $fh1 or die "Cloud not close file...\n";
        }


        # TODO add constraints (again)
        push(@bytes, $$data_types{$$table_info{columns_hash}{$key}}{pack}->($$values{$key}));
    }

    my $row_status_pos = undef;
    for my $vals_bytes (@bytes)
    {
        for my $bytes (@$vals_bytes)
        {
            if(!defined $row_status_pos)
            {
                $row_status_pos = tell($fh);
            }
            print $fh $bytes;
        }
    }

    my $fh_set_status;
    close($fh) or die "Cloud not close table fh!".$!;

    open ($fh_set_status, "+<", "$$self{connection}/$table") or die $!;
    flock($fh_set_status, LOCK_EX) or die "Cloud not lock file (delete)\n";
    seek($fh_set_status, $row_status_pos, 0);
    print $fh_set_status PackInternalValue(0);
    close($fh_set_status) or die "Cloud not close fh!".$!;
}

sub TruncateTable($$)
{
    my ($self, $table_name) = @_;

    ASSERT(defined $self);
    ASSERT(defined $table_name);
    ASSERT($table_name ne '');

    my $fh;
    open($fh, ">", "$$self{connection}/$table_name"."_meta") or die "Cloud not open db for reading!".$!;
    $| = 1;
    print $fh "";

    close $fh or die "Cloud not close fh..".$!;

    open($fh, ">", "$$self{connection}/$table_name") or die "Cloud not open db for reading!".$!;
    $| = 1;
    print $fh "";

    close $fh or die "Cloud not close fh..".$!;
}

sub DeleteRows($$$)
{
    my ($self, $table_name, $filters) = @_;

    ASSERT( defined $self);
    ASSERT(defined $table_name);
    ASSERT($table_name ne '');

    $self->MapEntries($table_name, 'delete', $filters);
}


=documentation
@paramIN filter hashref containing two keys: column_name and desired_value
@paramIN new_row hashref with the new row.
=cut

sub Update($$$$)
{
    my ($self, $table_name, $filter, $new_row) = @_;

    ASSERT(defined $self);
    ASSERT(defined $table_name);
    ASSERT($table_name ne '');
    #ASSERT(defined $filter);
    ASSERT(defined $new_row);

    $self->MapEntries($table_name, 'update', $filter, {new_row => $new_row});
}


=documentation
    @paramin table_name string, the name of the table

    Updates the index of for an entire table. Always reads the entire (index) table.
=cut

sub GetIndex($$)
{
    my ($self, $table_name) = @_;

    ASSERT(defined $self);
    ASSERT(defined $table_name);
    ASSERT($table_name ne '');

    my $rows = [];
    my $fh;

    open($fh, "<", "$$self{connection}/$table_name"."_index") or die "$!";
    my $last_iter = 0;
    while(1)
    {
            # read one row.
            try
            {
                my $row = {};
                $$row{id} = UnpackInteger($fh);
                $$row{record_begin} = UnpackInteger($fh);
                $$row{records} = []; #used when aggregating, needs to ne initialized
                push(@$rows, $row);
            }
            catch
            {
                if(index($_, 'EOF') != -1)
                {
                    $last_iter = 1;
                }
                else
                {
                    die $_;
                }
            };
        if($last_iter)
        {
            close $fh or die "$!";
            $$self{indexes}{$table_name} = [];

            for my $row (@$rows)
            {
                $self->AddSingleIndexEntry($table_name, $row);
            }
            return;
        }
    }
    ASSERT(0);
}


=documentation
    "Aggregates" the index entries to a data structure which is fast to search
    through and can handle multiple entries of the same id.
=cut

sub AddSingleIndexEntry($$$)
{
    my ($self, $table_name, $row) = @_;

    ASSERT(defined $self);
    ASSERT(defined $table_name);
    ASSERT($table_name ne '');
    ASSERT(defined $$row{id});
    ASSERT(defined $$row{record_begin});

    if(!defined $$self{indexes}{$table_name})
    {
        $$self{indexes}{$table_name} = [];
        push @{$$self{indexes}{$table_name}}, $row;
        push (@{$$self{indexes}{$table_name}[0]{records}}, $$row{record_begin});
        return;
    }
    my $times = @{$$self{indexes}{$table_name}};

    my $hash = {};

    my $last_idx = 0;
    for (my $i = 0; $i < $times; $i++)
    {
        ASSERT(defined $$self{indexes}{$table_name}[$i]);
        $last_idx++;
        if($$self{indexes}{$table_name}[$i]{id} == $$row{id})
        {
            push (@{$$self{indexes}{$table_name}[$i]{records}}, $$row{record_begin});
            return;
        }
    }
    push @{$$self{indexes}{$table_name}}, $row;
    push (@{$$self{indexes}{$table_name}[$last_idx]{records}}, $$row{record_begin});
}

=documentation
    @paramin table_name

    Sorts the index for table table_name.
=cut

sub RefreshIndex($$)
{
    my ($self, $table_name) = @_;

    ASSERT(defined $self);
    ASSERT(defined $$self{indexes}{$table_name}, "The index isn't read for this table");

    my @arr =  sort{$$a{id} <=> $$b{id}} @{$$self{indexes}{$table_name}};
    $$self{indexes}{$table_name} = \@arr;
}

=documentation
    @paramin table_name
    Deletes the index for table_name from the memory
=cut
sub ReleaseIndex($$)
{
    my ($self, $table_name) = @_;

    ASSERT(defined $self);
    ASSERT(defined $table_name);

    delete $$self{indexes}{table_name};
}

=pod
    @paramin $table_name  the name of the Table
    @paramin $look_for the value for which is looked for
    @paramout the byte from which the beginning of each row

    Basically a custom version of the binary search;
=cut
sub SearchInIndex($$$)
{
    my ($self, $table_name, $look_for) = @_;

    ASSERT(defined $self);
    ASSERT(defined $table_name);
    ASSERT($table_name ne '');
    ASSERT(defined $look_for);
    ASSERT(defined $$self{indexes}{$table_name});

    my $low = 0;
    my $high = scalar(@{$$self{indexes}{$table_name}});
    while($low <= $high)
    {
        #print "LOW IS $low HIGH IS $high \n\n";
        my $middle = int(($low + $high) / 2);
        #print "MIDDLE IS: $middle\n";
        if ($look_for < $$self{indexes}{$table_name}[$middle]{id})
        {
            $high = $middle - 1;
        }
        elsif ($look_for > $$self{indexes}{$table_name}[$middle]{id})
        {
            $low = $middle + 1;
        }
       else
       {
           #print "FOUND IT \n";
           return $$self{indexes}{$table_name}[$middle]{records};
       }
    }
}


=documentation
    @param table_name the name of the table from which will be selected
    @param rows an array which contains the starting bytes of a each row
=cut

sub IndexSelect($$$;$)
{
    my ($self, $table_name, $id, $filters) = @_;

    ASSERT(defined $self);
    ASSERT(defined $$self{indexes}{$table_name});
    ASSERT(defined $table_name);
    ASSERT($table_name ne '');
    ASSERT(defined $id);

    my $table_data = $self->GetTableDetails($table_name);
    ASSERT(defined $table_data);
    ASSERT(defined $$table_data{columns});

    my $index_matches = $self->SearchInIndex($table_name, $id);

    my $fh;
    open($fh, "<", "$$self{connection}/$table_name") or die "Cloud not open db for reading!".$!;

    my $rows = ();
    my $row;
    my $positions = []; #contains the starting position (in bytes of every value)
    my $last_iter = 0;
    print Dumper @$index_matches;
    # Get one row from the table
    for my $row_position (@$index_matches)
    {
        #print "row position: $row_position \n";
        seek($fh, $row_position, 0);

        $row = {};
        $positions = [];

        # Get one row from the table
        for my $element (@{$$table_data{columns}})
        {
            ASSERT(defined $$element{column_name});
            ASSERT(defined $$element{column_type});
            $$row{$$element{column_name}} = $$data_types{$$element{column_type}}{unpack}->($fh, $positions);

            # You have reached a row which is currently inserted.
            if(defined $$row{row_status} && $$row{row_status} == 2)
            {
                $last_iter = 1;
            }

        }
        if($last_iter)
        {
            close $fh or die "$!";
            return $rows;
        }

        if(!defined $filters)
        {
            push(@$rows, $row);
        }
        else
        {
            for my $look_for (@$filters)
            {
                ASSERT(defined $$look_for{column_name});
                ASSERT(defined $$look_for{desired_value});
                ASSERT(defined $$look_for{compare_by});

                # basically calls the compare function from the data_types object.
                # TODO make it more readable
                if($$data_types{$$table_data{columns_hash}{$$look_for{column_name}}}{compare}->($$row{$$look_for{column_name}},$$look_for{desired_value}, $$look_for{compare_by}))
                {
                    push(@$rows, $row);
                    last;
                }
            }
        }
    }
    return $rows;
}


############################# END API #########################################

############################# PRIVATE FUNCTIONS ###############################

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
    open $$self{meta}, "<", "$$self{root}/$$self{connection}/$$self{connection}"."_meta" or die "Cloud not read meta file. Does $$self{root}/$$self{connection}/$$self{connection}_meta exist ? Are permissions ok?".$!;

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
        #print "Table name length: $table_name_length \n";

        $bytes_read = read($$self{meta}, $bytes, $table_name_length);
        #print "bytes, read = $bytes_read \n";
        ASSERT($bytes_read > 0, " Bad db format!");
        my $table_name = unpack("a$bytes_read", $bytes);
        #print "Table name is: $table_name \n";

        $$table{name} = $table_name;
        my $first_iter = 1;
        my $columns = {};
        my $columns_names = ();
        #read all columns
        while(1)
        {
            $bytes_read = read($$self{meta}, $bytes, 4);
            ASSERT($bytes_read > 0, "Bad db format! (column type)");
            my $column_type = unpack('i', $bytes);
            #print "Column type is : $column_type \n";
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
            #print "Column name is: $column_name \n";

            push(@{$$table{columns}}, {'column_name' => $column_name, 'column_type' => $column_type});
            $$columns{$column_name} = $column_type;
            $first_iter = 0;
            push(@$columns_names, $column_name);
        }

        $$table{columns_hash} = $columns;
        $$table{columns_names} = $columns_names;
        close($$self{meta});
        if($$table{name} eq $search_for_table_name)
        {
            return $table;
        }
    }
}

=documentation
    This function Performs an operation after reading a row from the database
@param filters an arref containing hashrefs with  keys: column_name and desired_value, compare_by which is '<', '>' or '=='
The value is the value which the column must be equal to
If the value is undefined the entire table will be returned.
@param params - hashref which contains some command specific parameters
=cut
sub MapEntries($$$;$$)
{
    my ($self, $table_name, $method, $filters, $params) = @_;

    ASSERT(defined $self);
    ASSERT(defined $table_name);
    ASSERT($table_name ne '');

    my $table_data = $self->GetTableDetails($table_name);
    ASSERT(defined $table_data);
    ASSERT(defined $$table_data{columns});

    my $fh;
    open($fh, "<", "$$self{connection}/$table_name") or die "Cloud not open db for reading!".$!;
    my $last_insert_ok;
    read($fh, $last_insert_ok, 4);

    my $rows = ();
    my $row;
    my $positions = []; #contains the starting position (in bytes of every value)
    my $last_iter = 0;

    while(1)
    {
        $row = {};
        $positions = [];

        # Get one row from the table
        for my $element (@{$$table_data{columns}})
        {
            ASSERT(defined $$element{column_name});
            ASSERT(defined $$element{column_type});
            try
            {
                $$row{$$element{column_name}} = $$data_types{$$element{column_type}}{unpack}->($fh, $positions);
            }
            catch
            {
                if(index($_, 'EOF') != -1)
                {
                    $last_iter = 1;
                }
                else
                {
                    die $_;
                }
            };

            # You have reached a row which is currently inserted.
            if(defined $$row{row_status} && $$row{row_status} == 2)
            {
                $last_iter = 1;
            }

        }
        if($last_iter)
        {
            close $fh or die "$!";
            return $rows;
        }

        # do whatever must be done with the row
        if(lc($method) eq 'select')
        {
            if(!defined $filters)
            {
                push(@$rows, $row);
            }
            else
            {
                for my $look_for (@$filters)
                {
                    ASSERT(defined $$look_for{column_name});
                    ASSERT(defined $$look_for{desired_value});
                    ASSERT(defined $$look_for{compare_by});

                    # basically calls the compare function from the data_types object.
                    # TODO make it more readable
                    if($$data_types{$$table_data{columns_hash}{$$look_for{column_name}}}{compare}->($$row{$$look_for{column_name}},$$look_for{desired_value}, $$look_for{compare_by}))
                    {
                        push(@$rows, $row);
                        last;
                    }
                }
            }
        }
        elsif(lc($method) eq 'update')
        {
            ASSERT(defined $$params{new_row});
           
            my $fh_d;
            if(!defined $filters)
            {
                print "tt1\n";
                open $fh_d, "+<", "$$self{connection}/$table_name" or die $!;
                seek($fh_d, $$positions[0], 0);

                #Stage for deletion
                my $del_stage = pack('i', 3);
                print $fh_d $del_stage or die;
                close $fh_d or die;
            }
            else
            {
                for my $look_for (@$filters)
                {
                    ASSERT(defined $$look_for{column_name});
                    ASSERT(defined $$look_for{desired_value});
                    ASSERT(defined $$look_for{compare_by});

                    open $fh_d, "+<", "$$self{connection}/$table_name" or die $!;
                    seek($fh_d, $$positions[0], 0);

                    my $del = pack('i', 3);
                    print $fh_d $del or die;
                    close $fh_d or die;
                }
            }

            $self->InsertInto($table_name, $$params{new_row});
            
            seek($fh_d, $$positions[0], 0);
            my $del = pack('i', 3);
            print $fh_d $del or die;
            close $fh_d or die;
 
        }
        elsif(lc $method eq 'delete')
        {

            if(!defined $filters)
            {
                my $fh_d;
                open $fh_d, "+<", "$$self{connection}/$table_name" or die $!;
                flock($fh_d, LOCK_EX) or die "Cloud not lock file (delete)\n";
                seek($fh_d, $$positions[0], 0);

                my $del = pack('i', 1);
                print $fh_d $del or die;
                close $fh_d or die;
            }
            else
            {
                for my $look_for (@$filters)
                {
                    ASSERT(defined $$look_for{column_name});
                    ASSERT(defined $$look_for{desired_value});
                    ASSERT(defined $$look_for{compare_by});

                    if($$data_types{$$table_data{columns_hash}{$$look_for{column_name}}}{compare}->($$row{$$look_for{column_name}},$$look_for{desired_value}, $$look_for{compare_by}))
                    {
                        my $fh_d;
                        open $fh_d, "+<", "$$self{connection}/$table_name" or die $!;
                        seek($fh_d, $$positions[0], 0);

                        my $del = pack('i', 1);
                        print $fh_d $del or die;
                        close $fh_d or die;
                        last;
                    }
                }
            }

        }
        elsif(lc $method eq 'delete_stage')
        {

            if(!defined $filters)
            {
                my $fh_d;
                open $fh_d, "+<", "$$self{connection}/$table_name" or die $!;
                flock($fh_d, LOCK_EX) or die "Cloud not lock file (delete)\n";
                seek($fh_d, $$positions[0], 0);

                my $del = pack('i', 3);
                print $fh_d $del or die;
                close $fh_d or die;
            }
            else
            {
                for my $look_for (@$filters)
                {
                    ASSERT(defined $$look_for{column_name});
                    ASSERT(defined $$look_for{desired_value});
                    ASSERT(defined $$look_for{compare_by});

                    if($$data_types{$$table_data{columns_hash}{$$look_for{column_name}}}{compare}->($$row{$$look_for{column_name}},$$look_for{desired_value}, $$look_for{compare_by}))
                    {
                        my $fh_d;
                        open $fh_d, "+<", "$$self{connection}/$table_name" or die $!;
                        seek($fh_d, $$positions[0], 0);

                        my $del = pack('i', 3);
                        print $fh_d $del or die;
                        close $fh_d or die;
                        last;
                    }
                }
            }

        }
        elsif(lc $method eq 'restore_delete_stage')
        {
            my $fh_d;
            open $fh_d, "+<", "$$self{connection}/$table_name" or die $!;
            flock($fh_d, LOCK_EX) or die "Cloud not lock file (delete)\n";
            seek($fh_d, $$positions[0], 0);

            my $del = pack('i', 0);
            print $fh_d $del or die;
            close $fh_d or die;
        }



    }
    close $fh or die "can't close fh".$!;
    return $rows;
}



sub ASSERT($;$)
{
    my ($expression, $message) = @_;

    $message = $message || "";
    if(!$expression)
    {
        die "ASSERT FAILED! Line:", (caller(0))[2], ,"  ", $message, "\n";
    }
}

1;
