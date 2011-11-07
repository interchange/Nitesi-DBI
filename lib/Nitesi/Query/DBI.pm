package Nitesi::Query::DBI;

use strict;
use warnings;

=head1 NAME

Nitesi::Query::DBI - DBI query engine for Nitesi

=head1 SYNOPSIS

    $query = Nitesi::Query::DBI->new(dbh => $dbh);

    $query->select(table => 'products',
                   fields => [qw/sku name price/],
                   where => {price < 5},
                   order => 'name',
                   limit => 10);

    $query->insert('products', {sku => '9780977920150', name => 'Modern Perl'});

    $query->update('products', {media_format => 'CD'}, {media_format => 'CDROM'});

    $query->delete('products', {inactive => 1});

=head1 DESCRIPTION

This query engine is based on L<SQL::Abstract> and L<SQL::Abstract::More>.

=cut

use base 'Nitesi::Object';

__PACKAGE__->attributes(qw/dbh sqla/);

use SQL::Abstract;
use SQL::Abstract::More;

=head1 METHODS

=head2 init

Initializer, embeds L<SQL::Abstract::More> object inside our Nitesi::Query::DBI
object.

=cut

sub init {
    my ($self, %args) = @_;

    $self->{sqla} = SQL::Abstract::More->new();
}

=head2 select

Runs query and returns records as hash references inside a array reference.

    $results = $query->select(table => 'products',
                              fields => [qw/sku name price/],
                              where => {price < 5});

    print "Our cheap offers: \n\n";

    for (@$results) {
        print "$_->{name} (SKU: $_->{sku}), only $_->{price}\n";
    }

=cut

sub select {
    my ($self, %args) = @_;
    my ($stmt, @bind, @fields, %extended, @sql_params);

    if (exists $args{fields}) {
	@fields= ref($args{fields}) eq 'ARRAY' ? @{$args{fields}} : split /\s+/, $args{fields};
    }
    else {
	@fields = ('*');
    }
    
    if ($args{join}) {
	my @join = ref($args{join}) eq 'ARRAY' ? @{$args{join}} : split /\s+/, $args{join};

	$extended{-from} = [-join => @join];
    }

    if ($args{limit}) {
	$extended{-limit} = $args{limit};
    }

    if (keys %extended) {
	# extended syntax for a join / limit
	$extended{-from} ||= $args{table};

	if ($args{order}) {
	    $extended{-order_by} = $args{order};
	}

	@sql_params = (-columns => \@fields,
		       -where => $args{where},
		       %extended,
	    );
    }
    else {
	@sql_params = ($args{table}, \@fields, $args{where}, $args{order});
    }

    eval {
	($stmt, @bind) = $self->{sqla}->select(@sql_params);
    };

    if ($@) {
	die "Failed to parse select parameters (", join(',', @sql_params) , ": $@\n";
    }

    return $self->_run($stmt, \@bind, %args);
}

=head2 select_field

Runs query and returns value for the first field (or undef).

    $name = $query->select_field(table => 'products', 
                                 field => 'name', 
                                 where => {sku => '9780977920150'});

=cut

sub select_field {
    my ($self, %args) = @_;

    if ($args{field}) {
	$args{fields} = [delete $args{field}];
    }

    $args{return_value} = 'value_first';

    return $self->select(%args);
}

=head2 select_list_field

Runs query and returns a list of the first field for all matching records, e.g.:

    @dvd_skus = $query->select_list_field(table => 'products',
                                    field => 'sku',
                                    where => {media_type => 'DVD'});

=cut

sub select_list_field {
    my ($self, %args) = @_;

    if ($args{field}) {
	$args{fields} = [delete $args{field}];
    }

    $args{return_value} = 'array_first';

    return $self->select(%args);
}

=head2 insert

Runs insert query, e.g.:

    $query->insert('products', {sku => '9780977920150', name => 'Modern Perl'});

=cut

sub insert {
    my ($self, @args) = @_;
    my ($stmt, @bind);

    ($stmt, @bind) = $self->{sqla}->insert(@args);

    $self->_run($stmt, \@bind, return_value => 'execute');
}

=head2 update

Runs update query, either with positional or name parameters, e.g.:

    $updates = $query->update('products', {media_format => 'CD'}, {media_format => 'CDROM'});

    $updates = $query->update(table => 'products', 
                              set => {media_format => 'CD'}, 
                              where => {media_format => 'CDROM'});

Returns the number of matched/updated records.

=cut

sub update {
    my $self = shift;
    my ($stmt, @bind);

    if (@_ == 2 || @_ == 3) {
	# positional parameters (table, updates, where)
	($stmt, @bind) = $self->{sqla}->update(@_);
    }
    else {
	# named parameters
	my %args = @_;

	($stmt, @bind) = $self->{sqla}->update($args{table}, $args{set}, $args{where});
    }

    $self->_run($stmt, \@bind, return_value => 'execute');
}

=head2 delete

Runs delete query, e.g.:

    $query->delete('products', {inactive => 1});

=cut

sub delete {
    my $self = shift;
    my ($stmt, @bind);

    if (@_ == 1 || @_ == 2) {
	# positional parameters (table, where)
	($stmt, @bind) = $self->{sqla}->delete(@_);
    }
    else {
	# named parameters
	my %args = @_;

	($stmt, @bind) = $self->{sqla}->delete(table => $args{table}, where => $args{where});
    }

    $self->_run($stmt, \@bind, return_value => 'execute');
}

sub _run {
    my ($self, $stmt, $bind_ref, %args) = @_;
    my ($sth, $row, @result, $ret);

    unless ($sth = $self->{dbh}->prepare($stmt)) {
	die "Failed to prepare $stmt: $DBI::errstr\n";
    }

    unless ($ret = $sth->execute(@$bind_ref)) {
	die "Failed to execute $stmt: $DBI::errstr\n";
    }

    if ($args{return_value}) {
	if ($args{return_value} eq 'execute') {
	    return $ret;
	}
	if ($args{return_value} eq 'array_first') {
	    return map {$_->[0]} @{$sth->fetchall_arrayref()};
	}
	if ($args{return_value} eq 'value_first') {
	    if ($row = $sth->fetch()) {
		return $row->[0];
	    }
	    return;
	}
	
	die "Invalid return_value for SQL query.";
    }

    while ($row = $sth->fetchrow_hashref()) {
	push @result, $row;
    }

    return \@result;
}

# private methods for testing, likely to promoted to public methods in the future
sub _create_table {
    my ($self, $table, $fields) = @_;
    my ($stmt, @bind);

    $stmt = $self->{sqla}->generate('create table', $table, $fields);

    $self->_run($stmt, [], return_value => 'execute');
}

sub _drop_table {
    my ($self, $table, $fields) = @_;
    my ($stmt, @bind);

    $stmt = $self->{sqla}->generate('drop table', $table);

    $self->_run($stmt, [], return_value => 'execute');
}

=head2 dbh

Returns DBI database handle.

=head2 sqla

Returns embedded SQL::Abstract::More object.

=head1 CAVEATS

Please anticipate API changes in this early state of development.

We don't recommend to use Nitesi::Query::DBI with file backed DBI
drivers like L<DBD::DBM>, L<DBD::CSV>, L<DBD::AnyData> or L<DBD::Excel>.
In case you want to do this, please install L<SQL::Statement> first,
as the statements produced by this module are not understood by
L<DBI::SQL::Nano>.

=head1 AUTHOR

Stefan Hornburg (Racke), <racke@linuxia.de>

=head1 LICENSE AND COPYRIGHT

Copyright 2011 Stefan Hornburg (Racke) <racke@linuxia.de>.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.

=cut

1;
