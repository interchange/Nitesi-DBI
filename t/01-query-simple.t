#! perl

use Test::More;
use Test::Database;

use Nitesi::Query::DBI;

# statements produced by SQL::Abstract are not understood by DBI::SQL::Nano
use SQL::Statement;

my (@handles, $dbh, $q, $ret);

@handles = Test::Database->handles();

if (@handles) {
    # determine number of tests
    plan tests => 2 * @handles;
}
else {
    plan skip_all => 'No test database handles available';
}

# NOTE:
# for some odd reasons tables are create by SQL::Abstract in uppercase :-(

# run tests
for my $testdb (@handles) {
    diag 'Testing with DBI driver ' . $testdb->dbd();

    $dbh = $testdb->dbh();
    $q = Nitesi::Query::DBI->new(dbh => $dbh);

    isa_ok($q, 'Nitesi::Query::DBI');

    # create table
    $q->_create_table('products', ['sku varchar(32)', 'name varchar(255)']);

    # insert
    $q->insert('PRODUCTS', {sku => '9780977920150', name => 'Modern Perl'});

    # select
    $ret = $q->select_field(table => 'PRODUCTS', field => 'name', 
			    where => {sku => '9780977920150'});

    ok($ret eq 'Modern Perl', "Select field result: $ret");

    # drop table
    $q->_drop_table('products');
}

