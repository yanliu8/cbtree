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