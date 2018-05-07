/* contrib/bloom/cbtree--1.0.sql on postgres 10.1 */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION cbtree" to load this file. \quit

CREATE FUNCTION cbthandler(internal)
RETURNS index_am_handler
AS 'MODULE_PATHNAME'
LANGUAGE C;

-- Access method
 CREATE ACCESS METHOD cbtree TYPE INDEX HANDLER cbthandler;
 COMMENT ON ACCESS METHOD cbtree IS 'cbtree index access method';

-- Opclasses

CREATE OPERATOR CLASS int4_ops
DEFAULT FOR TYPE int4 USING cbtree AS
	OPERATOR	1	=(int4, int4),
	FUNCTION	1	hashint4(int4);

-- Delta functions
create table delta (pos int, tabid oid, attr text);

create function delta_actual_pos(new_pos integer, id oid, attr_name text) returns integer
    as $$
        DECLARE
            p1 integer := 0;
            p2 integer := 0;
            diff integer := 0;

        BEGIN
            select count(pos) into p2 from delta where pos <= new_pos and tabid = id and attr = attr_name;
            diff := p2 - p1;
            WHILE diff > 0 LOOP
                p1 := p2;
                select count(pos) into p2 from delta where pos <= (new_pos + p1) and tabid = id and attr = attr_name;
                diff := p2 - p1;
            END LOOP;

            RETURN p2 + new_pos;
        END;
    $$
language plpgsql;

create function delta_sel(sel_pos integer, id oid, attr_name text) returns setof tid
    as $$
        DECLARE
            actual_pos integer;
            tabname text;
        BEGIN
            actual_pos := delta_actual_pos(sel_pos, id, attr_name);
            select relname into tabname from pg_class where oid = id;
            return QUERY
                EXECUTE ('select ctid from ' || tabname || ' where ' || attr_name || ' = ' || actual_pos::text);
        END;
    $$
language plpgsql;

create function delta_del(del_pos integer, id oid, attr_name text) returns void
    as $$
        DECLARE
            actual_pos integer;
            tabname text;
        BEGIN
            SELECT relname INTO tabname FROM pg_class WHERE oid = id;
            actual_pos := delta_actual_pos(del_pos, id, attr_name);
            INSERT INTO delta VALUES (actual_pos, id, attr_name);
            EXECUTE ('DELETE FROM ' || tabname ||' WHERE ' || attr_name || ' = ' || del_pos::text);
        END;
    $$
language plpgsql;

create function delta_ins(ins_pos integer, id oid, attr_name text) returns void
    as $$
        DECLARE
            actual_pos integer;

        BEGIN
            actual_pos := delta_actual_pos(ins_pos, id, attr_name);
            UPDATE delta SET pos = pos + 1 WHERE pos >= actual_pos and tabid = id;
        END;
    $$
language plpgsql;

create function auto_vacuum() RETURNS trigger
    as $$
    DECLARE
        tabid   oid;
    BEGIN
        IF ((select count(*) from delta) > 1000)
        THEN
            FOR tabid IN (SELECT DISTINCT tabid FROM delta) LOOP
                EXECUTE ('VACUUM ' || (select relname from pg_class where oid = id));
            END LOOP;
            TRUNCATE delta;
        END IF;
        RETURN NEW;
    END;
    $$
language plpgsql;

create trigger auto_vacuum_trigger
    after insert on delta
    execute procedure auto_vacuum();