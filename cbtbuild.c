/*--------------------------------------------------------
 *
 * cbtbuild.c
 *		Implementation of initialization of a counted btree.
 *
 * IDENTIFICATION
 *		contrib/cbtree/cbtbuild.c
 *
 *--------------------------------------------------------
 */

#include "postgres.h"

#include "cbtree.h"
#include "access/genam.h"
#include "catalog/index.h"
#include "storage/smgr.h"
#include "access/xloginsert.h"
#include "utils/rel.h"
#include "utils/memutils.h"
#include "storage/bufmgr.h"
#include "access/reloptions.h"
#include "storage/bufpage.h"
#include "utils/elog.h"

typedef struct CBTPageState
{
    struct CBTPageState *cbtps_parent;
    Page                cbtps_page;
    Size                cbtps_maxfill;
    BlockNumber         cbtps_blockno;
    OffsetNumber        cbtps_lastoff;
    uint32              cbtps_level;
    uint32              total_count;
}CBTPageState;


typedef struct
{
    Relation        heap;
    int             indtuples;
    Relation	    index;
    bool		    cbtbs_use_wal;	/* dump pages to WAL? */
    BlockNumber     cbtbs_pages_alloced; /* # pages allocated */
    BlockNumber     cbtbs_pages_written; /* # pages written out */
    Page          cbtbs_zero_page;
    CBTPageState    *leaf_pagestate;
    MemoryContext   context;
}CBTBuildState;

static void cbtbuildCallback(Relation index, HeapTuple htup, Datum *values,
                 bool *isnull, bool tupleIsAlive, void *state);
static void cbt_finish_upper_level(CBTBuildState *buildstate);
static void cbt_build_add_tuple(CBTBuildState *state, CBTPageState *pagestate, CBTTuple newtuple);
static void cbt_writepage(CBTBuildState *buildstate, Page page, BlockNumber blkno);
static void cbt_init_pagestate(CBTPageState *pagestate, CBTBuildState *bstate, uint32 level);
static Page cbt_newpage(uint32 level);
static void CBTFillMetaPage(Page metapage, BlockNumber root, uint32 level);

/*
 * build a new counted btree index.
 */
IndexBuildResult *
cbtbuild(Relation heap, Relation index, IndexInfo *indexInfo)
{

    CBTBuildState       buildstate;
    double              reltuples;
    IndexBuildResult    *result;

    if (RelationGetNumberOfBlocks(index) != 0)
        elog(ERROR, "index \"%s\" already contains data",
             RelationGetRelationName(index));

    buildstate.leaf_pagestate = NULL;
    buildstate.indtuples = 0;

    /* Initialize the buildstate that is passed into IndexBuildHeapScan. */
    buildstate.cbtbs_pages_alloced = CBT_METAPAGE;
    buildstate.cbtbs_pages_written = 0;
    buildstate.heap = heap;
    buildstate.index = index;
    buildstate.cbtbs_use_wal = XLogIsNeeded() && RelationNeedsWAL(index);
    buildstate.cbtbs_zero_page = NULL;
    buildstate.context = AllocSetContextCreate(CurrentMemoryContext,
                                                "Counted b tree build temporary context",
                                                ALLOCSET_DEFAULT_SIZES);

    /* Call the interface to loop over heap tuples. */
    reltuples = IndexBuildHeapScan(heap, index, indexInfo, false, cbtbuildCallback, (void *) &buildstate);

    /* Finish upper level and build meta page */
    cbt_finish_upper_level(&buildstate);
    MemoryContextDelete(buildstate.context);
    result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));
    result->heap_tuples = reltuples;
    result->index_tuples = buildstate.indtuples;

    return result;
}

/*
 *  The call back function when iteration through tuples in heap.
 *  Construct a new cbt tuple from the current heap tuple and
 *  insert it into the counted B tree.
 */
void
cbtbuildCallback(Relation index,
                 HeapTuple htup,
                 Datum *values,
                 bool *isnull,
                 bool tupleIsAlive,
                 void *state)
{
    CBTBuildState *buildstate = (CBTBuildState *) state;
    MemoryContext oldcontext;
    CBTTuple      itup;

    oldcontext = MemoryContextSwitchTo(buildstate->context);

    itup = palloc(sizeof(CBTTupleData));
    CBTFormTuple(&htup->t_self, itup, 1);
    cbt_build_add_tuple(buildstate, buildstate->leaf_pagestate, itup);

    MemoryContextSwitchTo(oldcontext);
}

/*
 *  Add the last page at each level to their parents and free
 *  the page states alloced duing cbtbuild. This is only called
 *  after all heap tuples have been traversed and inserted into
 *  the counted B tree.
 */
void
cbt_finish_upper_level(CBTBuildState *buildstate)
{
    uint32      level = 0;
    uint32      rootblkno = InvalidBlockNumber;
    CBTPageState *pagestate;
    CBTPageState *opagestate;
    Page        metapage;
    CBTPageOpaque rootopaque;
    CBTTuple    parenttuple;
    ItemPointerData self_itemptr;

    pagestate = buildstate->leaf_pagestate;

    while (pagestate != NULL)
    {
        level++;
        if (pagestate->cbtps_parent == NULL)
        {
            rootopaque = (CBTPageOpaque )PageGetSpecialPointer(pagestate->cbtps_page);
            rootopaque->cbto_flags = rootopaque->cbto_flags | CBT_ROOT;
        }
        else
        {
            parenttuple = palloc(sizeof(CBTTupleData));
            ItemPointerSet(&self_itemptr, pagestate->cbtps_blockno, P_FIRSTOFFSET);
            CBTFormTuple(&self_itemptr, parenttuple, pagestate->total_count);
            cbt_build_add_tuple(buildstate, pagestate->cbtps_parent, parenttuple);
        }
        cbt_writepage(buildstate, pagestate->cbtps_page, pagestate->cbtps_blockno);
        rootblkno = pagestate->cbtps_blockno;
        opagestate = pagestate;
        pagestate = pagestate->cbtps_parent;
        pfree(opagestate);
    }

	metapage = (Page) palloc(BLCKSZ);
    CBTFillMetaPage(metapage, rootblkno, level);

    /*
	 * Write the page and log it.
	 */
    smgrwrite(buildstate->index->rd_smgr, MAIN_FORKNUM, CBT_METAPAGE,
              (char *) metapage, true);
    log_newpage(&(buildstate->index->rd_smgr->smgr_rnode.node), INIT_FORKNUM,
                CBT_METAPAGE, metapage, false);
}

/*
 * Add a cbt tuple into the existing counted B tree.
 */
void
cbt_build_add_tuple(CBTBuildState *state, CBTPageState *pagestate, CBTTuple newtuple)
{
    Page            page;
    Size            tuplesz;
    Size            pgspace;

    if (pagestate == NULL)
    {
        state->leaf_pagestate = palloc(sizeof(CBTPageState));
        state->leaf_pagestate->cbtps_parent = NULL;
        cbt_init_pagestate(state->leaf_pagestate, state, CBT_LEAF_LEVEL);
        pagestate = state->leaf_pagestate;
    }

    page = pagestate->cbtps_page;
    tuplesz = MAXALIGN(sizeof(CBTTupleData));
    pgspace = PageGetFreeSpace(page);


    if (pgspace < tuplesz || (pgspace < pagestate->cbtps_maxfill && pagestate->cbtps_lastoff > 1))
    {
        /*
         * Page is already full. Write the page to disk, conenct it to
         * its parent, and request a new page.
         */

        Page		    opage = page;
        Page            npage;
        CBTPageState    *opagestate = pagestate;
        CBTPageOpaque   opaque = (CBTPageOpaque )PageGetSpecialPointer(page);
        BlockNumber     oblkno = pagestate->cbtps_blockno;
        ItemPointerData self_itemptr;
        CBTTuple        parenttuple;
        BlockNumber     nblkno;

        /*
		 * Link the old page into its parent, using its minimum key. If we
		 * don't have a parent, we have to create one; this adds a new btree
		 * level.
		 */
        if (opagestate->cbtps_parent == NULL)
        {
            opagestate->cbtps_parent = palloc(sizeof(CBTPageState));
            cbt_init_pagestate(opagestate->cbtps_parent, state, opagestate->cbtps_level + 1);
        }

        parenttuple = palloc(sizeof(CBTTupleData));
        ItemPointerSet(&self_itemptr, oblkno, P_FIRSTOFFSET);
        CBTFormTuple(&self_itemptr, parenttuple, opagestate->total_count);
        cbt_build_add_tuple(state, opagestate->cbtps_parent, parenttuple);
        ItemPointerSet(&opaque->cbto_parent, opagestate->cbtps_parent->cbtps_blockno, opagestate->cbtps_parent->cbtps_lastoff);

        /* Create new page of same level and update pagestate at this level*/
        cbt_init_pagestate(pagestate, state, pagestate->cbtps_level);
        npage = pagestate->cbtps_page;

        /* and assign it a page position */
        nblkno = pagestate->cbtps_blockno;
        CBTPageOpaque oopaque = (CBTPageOpaque) PageGetSpecialPointer(opage);
        CBTPageOpaque nopaque = (CBTPageOpaque) PageGetSpecialPointer(npage);

        oopaque->cbto_next = nblkno;
        nopaque->cbto_prev = oblkno;
        nopaque->cbto_next = InvalidBlockNumber;	/* redundant */

        /*
         * Write out the old page.  We never need to touch it again, so we can
         * free the opage workspace too.
         */
        cbt_writepage(state, opage, oblkno);
    }

    pagestate->cbtps_lastoff = OffsetNumberNext(pagestate->cbtps_lastoff);
    if (PageAddItem(pagestate->cbtps_page, (Item) newtuple, tuplesz, pagestate->cbtps_lastoff, false, false)
            == InvalidOffsetNumber)
        elog(ERROR, "failed to add item to the index page");

    state->indtuples++;
    pagestate->total_count += newtuple->childcnt;
}

/*
 * Write a page of counted B tree to disk.
 */
void
cbt_writepage(CBTBuildState *buildstate, Page page, BlockNumber blkno)
{
    /* Ensure rd_smgr is open (could have been closed by relcache flush!) */
    RelationOpenSmgr(buildstate->index);

    /* XLOG stuff */
    if (buildstate->cbtbs_use_wal)
    {
        /* We use the heap NEWPAGE record type for this */
        log_newpage(&buildstate->index->rd_node, MAIN_FORKNUM, blkno, page, true);
    }

    /*
     * If we have to write pages nonsequentially, fill in the space with
     * zeroes until we come back and overwrite.  This is not logically
     * necessary on standard Unix filesystems (unwritten space will read as
     * zeroes anyway), but it should help to avoid fragmentation. The dummy
     * pages aren't WAL-logged though.
     */
    while (blkno > buildstate->cbtbs_pages_written)
    {
        if (!buildstate->cbtbs_zero_page)
            buildstate->cbtbs_zero_page = (Page) palloc0(BLCKSZ);
        /* don't set checksum for all-zero page */
        smgrextend(buildstate->index->rd_smgr, MAIN_FORKNUM,
                   buildstate->cbtbs_pages_written++,
                   (char *) buildstate->cbtbs_zero_page,
                   true);
    }

    PageSetChecksumInplace(page, blkno);

    /*
     * Now write the page.  There's no need for smgr to schedule an fsync for
     * this write; we'll do it ourselves before ending the build.
     */
    if (blkno == buildstate->cbtbs_pages_written)
    {
        /* extending the file... */
        smgrextend(buildstate->index->rd_smgr, MAIN_FORKNUM, blkno,
                   (char *) page, true);
        buildstate->cbtbs_pages_written++;
    }
    else
    {
        /* overwriting a block we zero-filled before */
        smgrwrite(buildstate->index->rd_smgr, MAIN_FORKNUM, blkno,
                  (char *) page, true);
    }

    pfree(page);
}

/*
 * Initialize attributes in page state.
 */
void
cbt_init_pagestate(CBTPageState *pagestate, CBTBuildState *bstate, uint32 level)
{
    pagestate->cbtps_page = cbt_newpage(level);
    pagestate->cbtps_blockno = ++bstate->cbtbs_pages_alloced;
    pagestate->cbtps_lastoff = P_FIRSTOFFSET - 1;
    pagestate->total_count = 0;
    pagestate->cbtps_level = level;
    if (level > CBT_LEAF_LEVEL)
        pagestate->cbtps_maxfill = (BLCKSZ * (100 - CBTREE_NONLEAF_FILLFACTOR) / 100);
    else
        pagestate->cbtps_maxfill = (Size) RelationGetTargetPageFreeSpace(bstate->index,
                                                                         CBTREE_DEFAULT_FILLFACTOR);

    if (level == CBT_LEAF_LEVEL)
        bstate->leaf_pagestate = pagestate;
}

/*
 * Create a new cbt page on a level.
 */
Page
cbt_newpage(uint32 level)
{
    Page		page;
    CBTPageOpaque opaque;

    page = (Page) palloc(BLCKSZ);

    /* Zero the page and set up standard page header info */
    CBTInitPage(page, (uint16) ((level > CBT_LEAF_LEVEL) ? 0 : CBT_LEAF));

    /* Initialize BT opaque state */
    opaque = (CBTPageOpaque) PageGetSpecialPointer(page);
    opaque->cbto_prev = opaque->cbto_next = InvalidBlockNumber;
    ItemPointerSet(&opaque->cbto_parent, InvalidBlockNumber, InvalidOffsetNumber);
    opaque->level = level;

    return page;
}

/*
 * Build a cbt tuple with given item pointer and children count.
 */
void
CBTFormTuple(ItemPointer itptr,
             CBTTuple itup,
             uint32 childcount)
{
    itup->childcnt = childcount;
    ItemPointerSet(&itup->itemptr, ItemPointerGetBlockNumber(itptr), ItemPointerGetOffsetNumber(itptr));
}

/*
 * Initialize a cbt page with given flag.
 */
void
CBTInitPage(Page page, uint16 flags)
{
    CBTPageOpaque opaque;

    PageInit(page, BLCKSZ, sizeof(CBTPageOpaqueData));

    opaque = (CBTPageOpaque) PageGetSpecialPointer(page);
    memset(opaque, 0, sizeof(CBTPageOpaqueData));
    opaque->cbto_flags = flags;
}

/*
 * Fill information of the meta page.
 */
void
CBTFillMetaPage(Page metapage, BlockNumber root, uint32 level)
{
    CBTMetaPageData *metadata;

    CBTInitPage(metapage, CBT_META);
    metadata = CBTPageGetMeta(metapage);
    memset(metadata, 0, sizeof(CBTMetaPageData));
    metadata->cbtm_magic = CBT_MAGIC;
    metadata->cbtm_level = level;
    metadata->cbtm_root = root;
}

/*
 * Build a new empty counted btree index.
 */
void cbtbuildempty(Relation index)
{
    Page        metapage;

    /* Construct metapage. */
    metapage = (Page) palloc(BLCKSZ);
    CBTFillMetaPage(metapage, InvalidBlockNumber, 0);

    /*
	 * Write the page and log it.  It might seem that an immediate sync would
	 * be sufficient to guarantee that the file exists on disk, but recovery
	 * itself might remove it while replaying, for example, an
	 * XLOG_DBASE_CREATE or XLOG_TBLSPC_CREATE record.  Therefore, we need
	 * this even when wal_level=minimal.
	 */
    PageSetChecksumInplace(metapage, CBT_METAPAGE);
    smgrwrite(index->rd_smgr, INIT_FORKNUM, CBT_METAPAGE,
              (char *) metapage, true);
    log_newpage(&index->rd_smgr->smgr_rnode.node, INIT_FORKNUM,
                CBT_METAPAGE, metapage, false);

    /*
     * An immediate sync is required even if we xlog'd the page, because the
     * write did not go through shared_buffers and therefore a concurrent
     * checkpoint may have moved the redo pointer past our xlog record.
     */
    smgrimmedsync(index->rd_smgr, INIT_FORKNUM);
}

bool
cbtcanreturn(Relation index, int attno)
{
    return false;
}

bytea *
cbtoptions(Datum reloptions, bool validate)
{
    return NULL;
}

bool
cbtvalidate(Oid opclassoid)
{
    return true;
}
