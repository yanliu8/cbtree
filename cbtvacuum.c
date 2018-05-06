#include "postgres.h"

#include "access/genam.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "storage/indexfsm.h"
#include "storage/bufmgr.h"
#include "miscadmin.h"
#include "commands/vacuum.h"
#include "cbtree.h"
#include "utils/memutils.h"

typedef struct
{
    IndexVacuumInfo *info;
    IndexBulkDeleteResult *stats;
    IndexBulkDeleteCallback callback;
    void	   *callback_state;
    BlockNumber lastBlockVacuumed;	/* highest blkno actually vacuumed */
    BlockNumber lastBlockLocked;	/* highest blkno we've cleanup-locked */
    BlockNumber totFreePages;	/* true total # of free pages */
    MemoryContext pagedelcontext;
} CBTVacState;

static void cbtvacuumpage(CBTVacState *vstate, BlockNumber blkno);
static void cbtvacuumscan(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
              IndexBulkDeleteCallback callback, void *callback_state);
static void cbt_delitem_vacuum(Relation rel, Buffer buf, OffsetNumber itemindex, CBTVacState *vstate);
static void cbt_reduce_parent(Relation rel, ItemPointer parent, int change);
static void cbt_delpage_vacuum(Relation rel, Buffer buf, CBTVacState *vstate);
void cbt_start_vacuum(Relation rel);
void cbt_end_vacuum(Relation rel);
void cbt_end_vacuum_callback(int code, Datum arg);

void
cbt_end_vacuum(Relation rel)
{
    if RELATION_IS_LOCAL(rel)
        UnlockRelationForExtension(rel, ExclusiveLock);
}

void
cbt_start_vacuum(Relation rel)
{
    if RELATION_IS_LOCAL(rel)
        LockRelationForExtension(rel, ExclusiveLock);
}

/*
 * cbt_end_vacuum wrapped as an on_shmem_exit callback function
 */
void
cbt_end_vacuum_callback(int code, Datum arg)
{
    cbt_end_vacuum((Relation) DatumGetPointer(arg));
}

IndexBulkDeleteResult *
cbtbulkdelete(IndexVacuumInfo *info,
                                            IndexBulkDeleteResult *stats, IndexBulkDeleteCallback callback,
                                            void *callback_state)
{
    Relation	rel = info->index;

    /* allocate stats if first time through, else re-use existing struct */
    if (stats == NULL)
        stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));

    /* Establish the vacuum cycle ID to use for this scan */
    /* The ENSURE stuff ensures we clean up shared memory on failure */
    PG_ENSURE_ERROR_CLEANUP(cbt_end_vacuum_callback, PointerGetDatum(rel));
    {
        cbt_start_vacuum(rel);

        cbtvacuumscan(info, stats, callback, callback_state);
    }
    PG_END_ENSURE_ERROR_CLEANUP(cbt_end_vacuum_callback, PointerGetDatum(rel));
    cbt_end_vacuum(rel);

    return stats;
}

IndexBulkDeleteResult *
cbtvacuumcleanup(IndexVacuumInfo *info,
                                               IndexBulkDeleteResult *stats)
{
    BlockNumber npages,
                blkno;
    Relation	index = info->index;

    /* No-op in ANALYZE ONLY mode */
    if (info->analyze_only)
        return stats;

    if (stats == NULL)
        stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));

    npages = RelationGetNumberOfBlocks(index);
    stats->num_pages = npages;
    stats->pages_free = 0;
    stats->num_index_tuples = 0;

    for (blkno = CBT_METAPAGE + 1; blkno < npages; blkno++)
    {
        Buffer		buffer;
        Page		page;

        vacuum_delay_point();

        buffer = ReadBufferExtended(index, MAIN_FORKNUM, blkno,
                                    RBM_NORMAL, info->strategy);
        LockBuffer(buffer, BUFFER_LOCK_SHARE);
        page = (Page) BufferGetPage(buffer);

        if (PageIsNew(page) || CBTPageIsDeleted(page))
        {
            RecordFreeIndexPage(index, blkno);
            stats->pages_free++;
        }
        else
        {
            stats->num_index_tuples += PageGetMaxOffsetNumber(page);
        }

        UnlockReleaseBuffer(buffer);
    }

    /* Finally, vacuum the FSM */
    IndexFreeSpaceMapVacuum(info->index);

    return stats;
}

/*
 * cbtvacuumscan --- scan the index for VACUUMing purposes
 *
 * This combines the functions of looking for leaf tuples that are deletable
 * according to the vacuum callback, looking for empty pages that can be
 * deleted, and looking for old deleted pages that can be recycled.  Both
 * btbulkdelete and btvacuumcleanup invoke this (the latter only if no
 * btbulkdelete call occurred).
 *
 * The caller is responsible for initially allocating/zeroing a stats struct
 * and for obtaining a vacuum cycle ID if necessary.
 */
void
cbtvacuumscan(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
             IndexBulkDeleteCallback callback, void *callback_state)
{
    Relation	rel = info->index;
    CBTVacState	vstate;
    BlockNumber num_pages;
    BlockNumber blkno;
    bool		needLock;

    /*
     * Reset counts that will be incremented during the scan; needed in case
     * of multiple scans during a single VACUUM command
     */
    stats->estimated_count = false;
    stats->num_index_tuples = 0;
    stats->pages_deleted = 0;

    /* Set up info to pass down to btvacuumpage */
    vstate.info = info;
    vstate.stats = stats;
    vstate.callback = callback;
    vstate.callback_state = callback_state;
    vstate.lastBlockVacuumed = CBT_METAPAGE;	/* Initialise at first block */
    vstate.lastBlockLocked = CBT_METAPAGE;
    vstate.totFreePages = 0;

    /* Create a temporary memory context to run _bt_pagedel in */
    vstate.pagedelcontext = AllocSetContextCreate(CurrentMemoryContext,
                                                  "_bt_pagedel",
                                                  ALLOCSET_DEFAULT_SIZES);

    /*
     * The outer loop iterates over all index pages except the metapage, in
     * physical order (we hope the kernel will cooperate in providing
     * read-ahead for speed).  It is critical that we visit all leaf pages,
     * including ones added after we start the scan, else we might fail to
     * delete some deletable tuples.  Hence, we must repeatedly check the
     * relation length.  We must acquire the relation-extension lock while
     * doing so to avoid a race condition: if someone else is extending the
     * relation, there is a window where bufmgr/smgr have created a new
     * all-zero page but it hasn't yet been write-locked by _bt_getbuf(). If
     * we manage to scan such a page here, we'll improperly assume it can be
     * recycled.  Taking the lock synchronizes things enough to prevent a
     * problem: either num_pages won't include the new page, or _bt_getbuf
     * already has write lock on the buffer and it will be fully initialized
     * before we can examine it.  (See also vacuumlazy.c, which has the same
     * issue.)	Also, we need not worry if a page is added immediately after
     * we look; the page splitting code already has write-lock on the left
     * page before it adds a right page, so we must already have processed any
     * tuples due to be moved into such a page.
     *
     * We can skip locking for new or temp relations, however, since no one
     * else could be accessing them.
     */

    blkno = CBT_METAPAGE + 1;
    for (;;)
    {
        /* Get the current relation length */
        num_pages = RelationGetNumberOfBlocks(rel);

        /* Quit if we've scanned the whole relation */
        if (blkno >= num_pages)
            break;
        /* Iterate over pages, then loop back to recheck length */
        for (; blkno < num_pages; blkno++)
        {
            cbtvacuumpage(&vstate, blkno);
        }
    }

    MemoryContextDelete(vstate.pagedelcontext);

    /* update statistics */
    stats->num_pages = num_pages;
}

void
cbtvacuumpage(CBTVacState *vstate, BlockNumber blkno)
{
    IndexVacuumInfo *info = vstate->info;
    IndexBulkDeleteCallback callback = vstate->callback;
    void	   *callback_state = vstate->callback_state;
    Relation	rel = info->index;
    Buffer		buf;
    Page		page;
    CBTPageOpaque opaque = NULL;

    buf = cbt_get_buffer(rel, blkno, CBT_READ);
    page = BufferGetPage(buf);
    opaque = (CBTPageOpaque) PageGetSpecialPointer(page);

    if (P_ISLEAF(opaque))
    {
        OffsetNumber offnum,
                minoff,
                maxoff,
                tupledeleted;


        /*
         * Trade in the initial read lock for a super-exclusive write lock on
         * this page.  We must get such a lock on every leaf page over the
         * course of the vacuum scan, whether or not it actually contains any
         * deletable tuples.
         */
        LockBuffer(buf, BUFFER_LOCK_UNLOCK);
        LockBufferForCleanup(buf);

        /*
         * Scan over all items to see which ones need deleted according to the
         * callback function.
         */
        minoff = P_FIRSTOFFSET;
        maxoff = PageGetMaxOffsetNumber(page);
        tupledeleted = 0;
        if (callback)
        {
            for (offnum = minoff;
                 offnum <= maxoff;
                 offnum = OffsetNumberNext(offnum))
            {
                CBTTuple	itup;
                ItemPointer htup;

                itup = (CBTTuple) PageGetItem(page,
                                                PageGetItemId(page, offnum - tupledeleted));
                htup = &(itup->itemptr);
                if (callback(htup, callback_state)) {
                    cbt_delitem_vacuum(rel, buf, offnum - tupledeleted, vstate);
                    tupledeleted++;
                }
            }
        }
    }

    UnlockReleaseBuffer(buf);
}

void
cbt_delitem_vacuum(Relation rel, Buffer buf, OffsetNumber itemindex, CBTVacState *vstate)
{
    Page		page = BufferGetPage(buf);
    CBTPageOpaque opaque = (CBTPageOpaque )PageGetSpecialPointer(page);

    cbt_reduce_parent(rel, &opaque->cbto_parent, -1);

    START_CRIT_SECTION();

    PageIndexTupleDelete(page, itemindex);
    vstate->stats->tuples_removed++;

    MarkBufferDirty(buf);

    END_CRIT_SECTION();

    if (PageGetMaxOffsetNumber(page) == 0)
    {
        MemoryContext oldcontext;
        int			ndel;

        /* Run pagedel in a temp context to avoid memory leakage */
        MemoryContextReset(vstate->pagedelcontext);
        oldcontext = MemoryContextSwitchTo(vstate->pagedelcontext);

        cbt_delpage_vacuum(rel, buf, vstate);

        MemoryContextSwitchTo(oldcontext);
    }

}

void
cbt_reduce_parent(Relation rel, ItemPointer parent, int change)
{
    ItemId      itemid;
    Buffer      buf;
    Page        page;
    CBTTuple    tuple;
    CBTPageOpaque opaque;

    if (!ItemPointerIsValid(parent))
        return;

    buf = cbt_get_buffer(rel, ItemPointerGetBlockNumber(parent), CBT_WRITE);
    page = BufferGetPage(buf);
    opaque = (CBTPageOpaque) PageGetSpecialPointer(page);
    itemid = PageGetItemId(page, ItemPointerGetOffsetNumber(parent));
    tuple = (CBTTuple )PageGetItem(page, itemid);

    START_CRIT_SECTION();

    tuple->childcnt += change;
    MarkBufferDirty(buf);

    END_CRIT_SECTION();

    UnlockReleaseBuffer(buf);
    cbt_reduce_parent(rel, &opaque->cbto_parent, change);
}


void
cbt_delpage_vacuum(Relation rel, Buffer buf, CBTVacState *vstate)
{
    Page                page = BufferGetPage(buf);
    CBTPageOpaque       opaque = (CBTPageOpaque )PageGetSpecialPointer(page);
    ItemPointerData     parentptr = opaque->cbto_parent;
    Buffer              parentbuf;

    if (! ItemPointerIsValid(&parentptr))
    {
        Buffer              metabuf;
        Page                metapg;
        CBTMetaPageData     *metad;

        /* The root is empty, update meta page */
        metabuf = cbt_get_buffer(rel, CBT_METAPAGE, CBT_READ);
        metapg = BufferGetPage(metabuf);
        metad = CBTPageGetMeta(metapg);

        Assert(metad->cbtm_root == BufferGetBlockNumber(buf));

        START_CRIT_SECTION();

        metad->cbtm_root = InvalidBlockNumber;
        MarkBufferDirty(metabuf);

        END_CRIT_SECTION();

        UnlockReleaseBuffer(metabuf);
    }
    else
    {
        parentbuf = cbt_get_buffer(rel, ItemPointerGetBlockNumber(&parentptr), CBT_WRITE);
        cbt_delitem_vacuum(rel, parentbuf, ItemPointerGetOffsetNumber(&parentptr), vstate);
        UnlockReleaseBuffer(parentbuf);
    }

    START_CRIT_SECTION();

    opaque->cbto_flags |= CBT_DELETED;
    MarkBufferDirty(buf);

    END_CRIT_SECTION();

    vstate->stats->pages_deleted++;
}

