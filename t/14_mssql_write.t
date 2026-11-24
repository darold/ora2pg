use utf8;
use strict;
use warnings;
use Test::More;
use FindBin qw($Bin);
use lib "$Bin/lib";
use DblinkTest qw(convert norm);

binmode(Test::More->builder->$_, ':utf8') for qw(output failure_output todo_output);

my %ex = (dblink_exclude => { EXT1 => 1 });
my $todo = '-- TODO MS SQL-write: write path not selected';
my ($out);

# 1. simple UPDATE
($out) = convert("BEGIN\n  UPDATE s.t\@ext1 SET a = 1 WHERE b = 2;\n  x := 1;\nEND;", %ex);
like($out, qr/\Q$todo\E\n-- UPDATE s\.t\@ext1 SET a = 1 WHERE b = 2;\nNULL;/, 'UPDATE commented, NULL; added');
like(norm($out), qr/NULL; x := 1;/, 'following statement is intact');
unlike($out, qr/TODO dblink/, 'no inline TODO inside the comment');

# 2. multi-line UPDATE with a subquery and a ';' in a string literal
($out) = convert("BEGIN\n  UPDATE s.t\@ext1 d\n     SET a = (SELECT max(z) FROM s.u\@ext1 WHERE k = ';'),\n         b = 'x;y'\n   WHERE d.id = 5;\n  y := 2;\nEND;", %ex);
like($out, qr/^-- \s+SET a = \(SELECT max\(z\).*k = ';'\),$/m, 'multi-line: every line commented');
like($out, qr/^-- \s+WHERE d\.id = 5;$/m, 'multi-line: ends at the real ;');
like(norm($out), qr/NULL; y := 2;/, 'multi-line: tail intact');

# 3. inside IF: the block does not become empty
($out) = convert("BEGIN\n  IF a = 1 THEN\n    DELETE FROM s.t\@ext1 WHERE id = 1;\n  END IF;\nEND;", %ex);
like(norm($out), qr/THEN .*-- DELETE FROM s\.t\@ext1 WHERE id = 1; NULL; END IF;/, 'IF: commented, NULL; keeps the block valid');

# 4. INSERT into MS SQL is a write, INSERT ... SELECT from MS SQL is a read
($out) = convert("BEGIN\n  INSERT INTO s.t\@ext1 (a) VALUES (1);\nEND;", %ex);
like($out, qr/^-- INSERT INTO s\.t\@ext1/m, 'INSERT commented');
($out) = convert("BEGIN\n  INSERT INTO loc_t (a) SELECT a FROM s.t\@ext1;\nEND;", %ex);
unlike($out, qr/^--/m, 'INSERT ... SELECT from MS SQL is not commented');
like($out, qr/TODO dblink EXT1/, 'read keeps the TODO');

# 5. non-excluded link is not touched by T10
($out) = convert("BEGIN\n  UPDATE s.t\@rdb1 SET a = 1;\nEND;", %ex);
unlike($out, qr/MS SQL-write/, 'other link: not commented');

done_testing;
