-- Predicates on INCLUDE-only index columns
--
-- A predicate that references only INCLUDE ("payload") columns must never be
-- turned into an index condition: the executor rejects index quals on non-key
-- attributes ("bogus index qualification", "btree index keys must be ordered
-- by attribute").  Boolean column references and IS [NOT] NULL tests used to
-- slip through ORCA's index predicate extraction because those shortcuts skip
-- the opfamily validation, so cover them explicitly.
--
-- See https://github.com/apache/cloudberry/issues/1948

LOAD 'pg_orca';
SET pg_orca.enable_orca = on;
SET client_min_messages = warning;

create table covering_include (k int, pay int, flag boolean);
insert into covering_include
  select i,
         case when i % 997 = 0 then null else i % 50 end,
         i % 7 = 0
  from generate_series(1, 100000) i;
create index covering_include_i on covering_include (k) include (pay, flag);
analyze covering_include;

-- KEYS: [k]    INCLUDED: [pay, flag]
-- Only the key predicate may become an index condition; the predicate on the
-- INCLUDE column has to stay a filter.

EXPLAIN (COSTS OFF) select * from covering_include where k < 30 and flag;
select * from covering_include where k < 30 and flag order by k;

EXPLAIN (COSTS OFF) select * from covering_include where k between 990 and 1000 and pay is null;
select * from covering_include where k between 990 and 1000 and pay is null order by k;

EXPLAIN (COSTS OFF) select * from covering_include where k <= 50 and pay = 7;
select * from covering_include where k <= 50 and pay = 7 order by k;

EXPLAIN (COSTS OFF) select * from covering_include where k = 42 and pay is not null;
select * from covering_include where k = 42 and pay is not null order by k;

-- Predicates that reference INCLUDE columns only.  With table scans disabled
-- the index is the only access path left, which is exactly the situation that
-- used to produce a plan with a bogus index qualification.  Plan shapes here
-- depend on whether ORCA finds any alternative at all, so only check results.
set enable_seqscan = off;

select count(*) from covering_include where flag;
select count(*) from covering_include where not flag;
select count(*) from covering_include where pay is null;
select count(*) from covering_include where pay is not null;
select count(*) from covering_include where flag and pay is null;

-- same, through the bitmap path
set enable_indexscan = off;

select count(*) from covering_include where flag;
select count(*) from covering_include where pay is null;

reset enable_indexscan;
reset enable_seqscan;

drop table covering_include;
