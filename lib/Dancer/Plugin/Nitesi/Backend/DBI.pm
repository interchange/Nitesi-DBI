package Dancer::Plugin::Nitesi::Backend::DBI;

use Moo;
use Dancer::Plugin::Database;

=head1 NAME 

Dancer::Plugin::Nitesi::Backend::DBI

=cut

# database handle retrieved from Dancer::Plugin::Database
has dbh => (
    is => 'ro',
    default => sub {database},
    );

sub params {
    my $self = shift;
    my %params;

    $params{dbh} = $self->dbh;
    return \%params;
}

1;
