/*--------------------------------------------------------
 *
 * cbtree.c
 *		Implementation of counted btree.
 *
 * IDENTIFICATION
 *		contrib/cbtree/cbtree.c
 *
 *--------------------------------------------------------
 */

#include "postgres.h"

#include "cbtree.h"
#include "fmgr.h"
#include "access/amapi.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(cbthandler);

Datum cbthandler(PG_FUNCTION_ARGS);

/*
 * Counted btree handler function: return IndexAmRoutine with access method parameters
 * and callbacks.
 */
Datum
cbthandler(PG_FUNCTION_ARGS)
{
	IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

	amroutine->amstrategies = CBTREE_NSTRATEGIES;
	amroutine->amsupport = CBTREE_NPROC;
	amroutine->amcanorder = false;
	amroutine->amcanorderbyop = false;
	amroutine->amcanbackward = false;
	amroutine->amcanunique = false;
	amroutine->amcanmulticol = true;
	amroutine->amoptionalkey = true;
	amroutine->amsearcharray = false;
	amroutine->amsearchnulls = false;
	amroutine->amstorage = false;
	amroutine->amclusterable = false;
	amroutine->ampredlocks = false;
	amroutine->amcanparallel = false;
	amroutine->amkeytype = InvalidOid;

	amroutine->ambuild = cbtbuild;
	amroutine->ambuildempty = cbtbuildempty;
	amroutine->aminsert = cbtinsert;
	amroutine->ambulkdelete = cbtbulkdelete;
	amroutine->amvacuumcleanup = cbtvacuumcleanup;
	amroutine->amcanreturn = cbtcanreturn;
	amroutine->amcostestimate = cbtcostestimate;
	amroutine->amoptions = cbtoptions;
	amroutine->amproperty = NULL;
	amroutine->amvalidate = cbtvalidate;
	amroutine->ambeginscan = cbtbeginscan;
	amroutine->amrescan = cbtrescan;
	amroutine->amgettuple = cbtgettuple;
	amroutine->amgetbitmap = NULL;
	amroutine->amendscan = cbtendscan;
	amroutine->ammarkpos = NULL;
	amroutine->amrestrpos = NULL;
	amroutine->amestimateparallelscan = NULL;
	amroutine->aminitparallelscan = NULL;
	amroutine->amparallelrescan = NULL;

	PG_RETURN_POINTER(amroutine);
}


