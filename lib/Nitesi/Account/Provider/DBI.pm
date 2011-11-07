package Nitesi::Account::Provider::DBI;

use strict;
use warnings;

use base 'Nitesi::Object';
use Nitesi::Query::DBI;

__PACKAGE__->attributes(qw/dbh crypt fields/);

=head1 NAME

Nitesi::Account:Provider::DBI - DBI Account Provider for Nitesi Shop Machine

=cut

=head1 METHODS

=head2 init

Initializer for this class. Arguments are:

=over 4

=item dbh

DBI handle (required).

=item crypt

L<Account::Manager::Password> instance (required).

=item fields

List of fields (as array reference) to be retrieved from the
database and put into account data return by login method.

=back

=item inactive

Name of field which determines whether user is disabled.

=cut

sub init {
    my ($self, %args) = @_;
    my ($sql);

    $self->{sql} = Nitesi::Query::DBI->new(dbh => $self->{dbh});
}

=head2 login

Check parameters username and password for correct authentication.

Returns hash reference with the following values in case of success:

=over 4

=item uid

User identifier

=item username

Username

=item roles

List of roles for this user.

=item permissions

List of permissions for this user.

=back

=cut

sub login {
    my ($self, %args) = @_;
    my ($results, $ret, $roles_map, @permissions, %conds, @fields, $acct);

    @fields = qw/uid username password/;

    if (defined $self->{fields}) {
	push @fields, @{$self->{fields}};
    }
    if (defined $self->{inactive}) {
	push @fields, $self->{inactive};
    }

    $conds{email} = $args{username};

    $results = $self->{sql}->select(table => 'users', fields => join(',', @fields),
				    where => \%conds);

    $ret = $results->[0];

    if ($ret) {
	if (defined $self->{inactive} && $ret->{$self->{inactive}}) {
	    # disabled user
	    return 0;
	}

	if ($self->{crypt}->check($ret->{password}, $args{password})) {
	    # retrieve permissions
	    $roles_map = $self->roles($ret->{uid}, map => 1);
	    @permissions = $self->permissions($ret->{uid}, [keys %$roles_map]);

	    $acct = {};

	    if (defined $self->{fields}) {
		for my $f (@{$self->{fields}}) {
		    $acct->{$f} = $ret->{$f};
		}
	    }

	    $acct->{uid} = $ret->{uid};
	    $acct->{username} = $ret->{username};
	    $acct->{roles} = [values %$roles_map];
	    $acct->{permissions} = \@permissions;
	    
	    return $acct;
	}
    }

    return 0;
}

=head2 roles

Returns list of roles for supplied user identifier.

=cut

sub roles {
    my ($self, $uid, %args) = @_;
    my (@roles);

    if ($args{map}) {
	my (%map, $record, $role_refs);

	$role_refs = $self->{sql}->select(fields => [qw/roles.rid roles.name/],
			     join => [qw/user_roles rid=rid roles/],
			     where => {uid => $uid});

	for my $record (@$role_refs) {
	    $map{$record->{rid}} = $record->{name};
	}

	return \%map;
    }
    elsif ($args{numeric}) {
	@roles = $self->{sql}->select_list_field(table => 'user_roles', 
					     fields => [qw/rid/], 
					     where => {uid => $uid});
    }
    else {
	@roles = $self->{sql}->select_list_field(fields => [qw/roles.name/],
						 join => [qw/user_roles rid=rid roles/],
						 where => {uid => $uid});
    }

    return @roles;
}

=head2 permissions

Returns list of permissions for supplied user identifier
and array reference with roles.

=cut

sub permissions {
    my ($self, $uid, $roles_ref) = @_;
    my (@records, @permissions, $sth, $row, $roles_str);

    @permissions = $self->{sql}->select_list_field(table => 'permissions',
						   fields => [qw/perm/],
						   where => [{uid => $uid}, {rid => {-in => $roles_ref}}]);
	
    return @permissions;
}

=head2 value

Get or set value.

=cut

sub value {
    my ($self, $username, $name, $value, $uid);

    $self = shift;
    $username = shift;
    $name = shift;

    if ($uid = $self->exists($username)) {
	if (@_) {
	    # set value
	    $value = shift;

	    $self->{sql}->update(table => 'users',
				 set => {$name => $value},
				 where => {uid => $uid}); 

	    return 1;
	}

	# retrieve value
	$value = $self->{sql}->select_field(table => 'users',
					    field => $name,
					    where => {uid => $uid});

	return $value;
    }
    
    return;
}

=head2 password

Set password.

=cut

sub password {
    my ($self, $password, $username) = @_;
    my ($uid);

    if ($username) {
	if ($uid = $self->exists($username)) {
	    $self->{sql}->update('users', 
				 {password => $password}, 
				 {uid => $uid});

	    return 1;
	}
    }
}

=head2 exists

Check whether user exists.

=cut

sub exists {
    my ($self, $username) = @_;
    my ($results);

    $results = $self->{sql}->select_field(table => 'users',
					  fields => ['uid'],
					  where => {username => $username});

    return $results;
}

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
