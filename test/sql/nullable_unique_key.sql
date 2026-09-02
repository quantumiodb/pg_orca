-- Keys derived from UNIQUE constraints must not admit duplicate rows.
--
-- A UNIQUE constraint only forbids duplicates among non-NULL values, so a
-- nullable UNIQUE column may hold any number of NULL rows. ORCA reads a key
-- as "the input is already distinct on these columns" and drops DISTINCT /
-- GROUP BY over it (CXformSimplifyGbAgg). get_relation_keys therefore only
-- reports a UNIQUE constraint as a key when every column carries a validated
-- NOT NULL constraint, or the index is NULLS NOT DISTINCT.

LOAD 'pg_orca';
SET pg_orca.enable_orca = on;
SET client_min_messages = warning;

-- 1. nullable UNIQUE: the aggregate must stay and the NULLs collapse
create table nuk_nullable (c0 numeric unique, c1 int);
insert into nuk_nullable values (1, 1), (2, 2), (null, 3), (null, 3), (null, 4);
analyze nuk_nullable;

explain (costs off) select distinct c0 from nuk_nullable;
select distinct c0 from nuk_nullable order by c0;

explain (costs off) select c0 from nuk_nullable group by c0;
select c0 from nuk_nullable group by c0 order by c0;

select c0, count(*) from nuk_nullable group by c0 order by c0;
select c0, c1 from nuk_nullable group by c0, c1 order by 1, 2;

-- 2. UNIQUE NOT NULL and PRIMARY KEY are real keys: the aggregate is dropped
create table nuk_notnull (c0 numeric unique not null, c1 int);
insert into nuk_notnull values (1, 1), (2, 2), (3, 3);
analyze nuk_notnull;

explain (costs off) select distinct c0 from nuk_notnull;
select distinct c0 from nuk_notnull order by c0;

create table nuk_pk (c0 int primary key, c1 int);
insert into nuk_pk values (1, 1), (2, 1);
analyze nuk_pk;

explain (costs off) select c0, c1 from nuk_pk group by c0, c1;
select c0, c1 from nuk_pk group by c0, c1 order by 1, 2;

-- 3. UNIQUE NULLS NOT DISTINCT admits at most one NULL row: still a key
create table nuk_nnd (c0 int unique nulls not distinct, c1 int);
insert into nuk_nnd values (1, 1), (null, 2);
analyze nuk_nnd;

explain (costs off) select distinct c0 from nuk_nnd;
select distinct c0 from nuk_nnd order by c0;

-- 4. multi-column UNIQUE with one nullable column is not a key
create table nuk_multi (a int not null, b int, c int, unique (a, b));
insert into nuk_multi values (1, 1, 1), (1, null, 2), (1, null, 3);
analyze nuk_multi;

explain (costs off) select distinct a, b from nuk_multi;
select distinct a, b from nuk_multi order by 1, 2;

-- 5. NOT NULL ... NOT VALID proves nothing about existing rows: the column
--    stays nullable (and the UNIQUE constraint a non-key) until validated
create table nuk_notvalid (c0 int unique, c1 int);
insert into nuk_notvalid values (1, 1), (null, 2), (null, 3);
alter table nuk_notvalid add constraint nuk_notvalid_c0_nn not null c0 not valid;
analyze nuk_notvalid;

explain (costs off) select distinct c0 from nuk_notvalid;
select distinct c0 from nuk_notvalid order by c0;
select count(*) from nuk_notvalid where c0 is null;

delete from nuk_notvalid where c0 is null;
alter table nuk_notvalid validate constraint nuk_notvalid_c0_nn;

explain (costs off) select distinct c0 from nuk_notvalid;
select distinct c0 from nuk_notvalid order by c0;

-- 6. partitioned tables: unique (a, b) with nullable b is not a key,
--    primary key (a, b) is
create table nuk_part (a int not null, b int, c int, unique (a, b))
    partition by list (a);
create table nuk_part_1 partition of nuk_part for values in (1);
create table nuk_part_2 partition of nuk_part for values in (2);
insert into nuk_part values (1, null, 1), (1, null, 2), (2, 1, 3);
analyze nuk_part;

explain (costs off) select distinct a, b from nuk_part;
select distinct a, b from nuk_part order by 1, 2;

create table nuk_part_pk (a int, b int, c int, primary key (a, b))
    partition by list (a);
create table nuk_part_pk_1 partition of nuk_part_pk for values in (1);
create table nuk_part_pk_2 partition of nuk_part_pk for values in (2);
insert into nuk_part_pk values (1, 1, 1), (1, 2, 2), (2, 1, 3);
analyze nuk_part_pk;

explain (costs off) select a, b, c from nuk_part_pk group by a, b, c;
select a, b, c from nuk_part_pk group by a, b, c order by 1, 2, 3;

-- 7. a scalar aggregate's single row is still a key: no redundant aggregate
explain (costs off) select distinct max(c0) from nuk_nullable;
select distinct max(c0) from nuk_nullable;

drop table nuk_nullable, nuk_notnull, nuk_pk, nuk_nnd, nuk_multi,
           nuk_notvalid, nuk_part, nuk_part_pk;
