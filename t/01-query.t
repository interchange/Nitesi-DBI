#! perl

use Test::More;
use Test::Database;

use Nitesi::Query::DBI;

# statements produced by SQL::Abstract are not understood by DBI::SQL::Nano
use SQL::Statement;

my (@handles, $dbh, $dbd, $q, $ret, @set, $limited_handles);

@handles = Test::Database->handles();

$limited_handles = 0;

for (@handles) {
    if ($_->dbd eq 'DBM') {
	# DBM cannot deal with multiple primary keys
	$limited_handles++;
    }
}

if (@handles) {
    # determine number of tests
    plan tests => 7 * @handles - 4 * $limited_handles;
}
else {
    plan skip_all => 'No test database handles available';
}

# run tests
for my $testdb (@handles) {
    diag 'Testing with DBI driver ' . $testdb->dbd();

    $dbh = $testdb->dbh();
    $dbd = $testdb->dbd();

    $q = Nitesi::Query::DBI->new(dbh => $dbh);

    isa_ok($q, 'Nitesi::Query::DBI');

    # create table
    $q->_create_table('products', ['sku varchar(32)', 'name varchar(255)']);

    # insert
    $q->insert('products', {sku => '9780977920150', name => 'Modern Perl'});

    # select field
    $ret = $q->select_field(table => 'products', field => 'name', 
			    where => {sku => '9780977920150'});

    ok($ret eq 'Modern Perl', "select field with $dbd driver");

    # select all
    $ret = $q->select(table => 'products');
    ok(scalar(@$ret) == 1, "select all with $dbd driver");

    # drop table
    $q->_drop_table('products');

    next if $testdb->dbd() eq 'DBM';

    # create table without primary key for testing distinct 
    $q->_create_table('navigation_products', ['sku varchar(32) NOT NULL', 
					      'navigation integer NOT NULL']);

    # insert records
    $q->insert('navigation_products', {sku => '9780977920150', navigation => 1});
    $q->insert('navigation_products', {sku => '9780977920150', navigation => 2});

    # normal select
    @set = $q->select_list_field(table => 'navigation_products', field => 'navigation');
    ok(scalar(@set) == 2, "select list field from navigation_products with $dbd driver");

    # distinct select (SQL::Abstract::More syntax)
    $ret = $q->select(table => 'navigation_products', fields => [-distinct => 'sku']);
    ok(scalar(@$ret) == 1, "select distinct from navigation_products with $dbd driver and original syntax");

    # distinct select (Nitesi::Query::DBI syntax)
    $ret = $q->select(table => 'navigation_products', fields => 'sku', distinct => 1);
    ok(scalar(@$ret) == 1, "select distinct from navigation_products with $dbd driver and our syntax");

    # distinct select list field
    @set = $q->select_list_field(table => 'navigation_products', field => 'sku', distinct => 1);
    ok(scalar(@set) == 1, "select distinct list field from navigation_products with $dbd driver")
	|| diag scalar(@set) . " results instead on one";

    $q->_drop_table('navigation_products');
}

