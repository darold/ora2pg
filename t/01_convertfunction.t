use strict;
use warnings;
use Test::Simple tests => 1;
use Ora2Pg;

my $function = "CREATE FUNCTION get_bal(acc_no IN NUMBER)
   RETURN NUMBER
   IS acc_bal NUMBER(11,2);
   BEGIN
      SELECT order_total
      INTO acc_bal
      FROM orders
      WHERE customer_id = acc_no;
      RETURN(acc_bal);
    END;
/";
my $ora2pg = new Ora2Pg(datasource => 'DBI:Mock:', user => 'mock', password => 'mock');
my $converted = $ora2pg->_convert_function("foo", $function, "");

ok($converted eq "", "Convert function failed with statement: " . $converted);