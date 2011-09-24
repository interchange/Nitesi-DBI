package Nitesi::Account::Provider::DBI;

use strict;
use warnings;

use base 'Nitesi::Object';
use Nitesi::Query::DBI;

__PACKAGE__->attributes(qw/dbh/);

=head1 NAME

Nitesi::Account:Provider::DBI - DBI Account Provider for Nitesi Shop Machine

=cut

=head1 METHODS

=head2 init

Initializer for this class. No arguments.

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
    my ($results, $ret, @roles, @permissions);

    $results = $self->{sql}->select(table => 'users', fields => [qw/uid username password/], 
				    where => {email => $args{username}});

    $ret = $results->[0];

    if ($ret && $args{password} eq $ret->{password}) {
	# retrieve permissions
	@roles = $self->roles($ret->{uid});
	@permissions = $self->permissions($ret->{uid}, \@roles);

	return {uid => $ret->{uid}, username => $ret->{username},
		roles => \@roles, permissions => \@permissions};
    }

    return 0;
}

=head2 roles

Returns list of roles for supplied user identifier.

=cut

sub roles {
    my ($self, $uid) = @_;
    my (@roles);

    @roles = $self->{sql}->select_list_field(table => 'user_roles', 
					     fields => [qw/rid/], 
					     where => {uid => $uid});

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
