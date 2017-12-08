#include "postgres.h"

#include "cbtree.h"
#include "access/genam.h"
#include "utils/rel.h"
#include "storage/bufmgr.h"
#include "access/relscan.h"
#include "storage/predicate.h"
#include "miscadmin.h"

CBTStack cbt_search_in_page(Buffer pagebuf, uint32 pos, CBTStack stack);
bool cbt_first(IndexScanDesc scan, ScanDirection dir);


typedef struct CBTScanOpaqueData
{
    ScanKey     keyData;
    bool        first_scan;
} CBTScanOpaqueData;

typedef CBTScanOpaqueData *CBTScanOpaque;

/*
 * Search in the current page. Find the node that contains the
 * target leaf.
 */
CBTStack
cbt_search_in_page(Buffer pagebuf, uint32 pos, CBTStack stack)
{
    Page            page;
    uint32          leftcount;
    CBTPageOpaque   opaque;
    OffsetNumber    maxoff;
    OffsetNumber    offset;
    CBTStack        newstack;

    page = BufferGetPage(pagebuf);
    opaque = (CBTPageOpaque) PageGetSpecialPointer(page);
    maxoff = PageGetMaxOffsetNumber(page);
    offset = P_FIRSTOFFSET;
    if(maxoff < P_FIRSTOFFSET)
        return NULL;

    if (stack == NULL)
        leftcount = 0;
    else
        leftcount = stack->total_count;

    for (;;)    {
        ItemId      curid;
        CBTTuple    tuple;

        curid = PageGetItemId(page, offset);

        if (!ItemIdIsDead(curid))
        {
            tuple = (CBTTuple) PageGetItem(page, curid);
            leftcount += tuple->childcnt;

            if (leftcount >= pos)
            {
                newstack = palloc(sizeof(CBTStackData));
                newstack->total_count = leftcount - tuple->childcnt;
                newstack->cbts_blkno = BufferGetBlockNumber(pagebuf);
                newstack->cbts_offset = offset;
                newstack->cbts_parent = stack;

                return newstack;
            }
        }

        if (offset == maxoff) {
            return NULL;
        }

        offset = OffsetNumberNext(offset);
    }
}

/*
 * Begin a scan. Initialize the scan opaque.
 */
IndexScanDesc
cbtbeginscan(Relation rel, int nkeys, int norderbys)
{
    IndexScanDesc  scan;
    CBTScanOpaque  so;

    /* No order by operators allowed */
    Assert(norderbys == 0);
    /* The only key should be the position */
    Assert(nkeys == 1);

    scan = RelationGetIndexScan(rel, nkeys, norderbys);

    so = (CBTScanOpaque) palloc(sizeof(CBTScanOpaqueData));
    so->keyData = (ScanKey) palloc(sizeof(ScanKeyData));
    so->first_scan = true;
    scan->xs_itupdesc = RelationGetDescr(rel);
    scan->opaque = so;

    return scan;
}

/*
 * End a scan. Free all the memory allocated during scan.
 */
void cbtendscan(IndexScanDesc scan)
{
    CBTScanOpaque so = (CBTScanOpaque) scan->opaque;

    /* Release storage */
    if (so->keyData != NULL)
        pfree(so->keyData);

    pfree(so);
}

/*
 * Restart a scan with new scankey.
 */
void
cbtrescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
               ScanKey orderbys, int norderbys)
{
    /* cbtree only support 1 position as argument */
    Assert(nscankeys == 1);
    Assert(orderbys == NULL);

    if (scankey && scan->numberOfKeys > 0)
        memmove(scan->keyData,
                scankey,
                scan->numberOfKeys * sizeof(ScanKeyData));
}

/*
 * Get the next tuple in the current scan.
 */
bool
cbtgettuple(IndexScanDesc scan, ScanDirection dir)
{
    bool        res;

    scan->xs_recheck = false;
    res = cbt_first(scan, dir);
    return res;
}

/*
 * Get the root of the current counted btree.
 */
Buffer
cbt_getroot(Relation rel, int access)
{
    Buffer		metabuf;
    Page		metapg;
    CBTPageOpaque metaopaque;
    Buffer		rootbuf = InvalidBlockNumber;
    Page		rootpage;
    CBTPageOpaque rootopaque;
    BlockNumber rootblkno = InvalidBlockNumber;
    uint32		rootlevel;
    CBTMetaPageData *metad;

    /*
     * Try to use previously-cached metapage data to find the root.  This
     * normally saves one buffer access per index search, which is a very
     * helpful savings in bufmgr traffic and hence contention.
     */
    if (rel->rd_amcache != NULL)
    {
        metad = (CBTMetaPageData *) rel->rd_amcache;
        /* We shouldn't have cached it if any of these fail */
        Assert(metad->btm_magic == CBTREE_MAGIC);
        Assert(metad->btm_root != P_NONE);

        rootblkno = metad->cbtm_root;
        Assert(rootblkno != P_NONE);
        rootlevel = metad->cbtm_level;

        rootbuf = cbt_get_buffer(rel, rootblkno, CBT_READ);
        rootpage = BufferGetPage(rootbuf);
        rootopaque = (CBTPageOpaque) PageGetSpecialPointer(rootpage);

        /*
         * Since the cache might be stale, we check the page more carefully
         * here than normal.  We *must* check that it's not deleted. If it's
         * not alone on its level, then we reject too --- this may be overly
         * paranoid but better safe than sorry.  Note we don't check P_ISROOT,
         * because that's not set in a "fast root".
         */
        if (!P_IGNORE(rootopaque) &&
            rootopaque->level == rootlevel &&
            P_LEFTMOST(rootopaque) &&
            P_RIGHTMOST(rootopaque))
        {
            /* OK, accept cached page as the root */
            return rootbuf;
        }
        UnlockReleaseBuffer(rootbuf);
        /* Cache is stale, throw it away */
        if (rel->rd_amcache)
            pfree(rel->rd_amcache);
        rel->rd_amcache = NULL;
    }

    metabuf = cbt_get_buffer(rel, CBT_METAPAGE, CBT_READ);
    metapg = BufferGetPage(metabuf);
    metaopaque = (CBTPageOpaque) PageGetSpecialPointer(metapg);
    metad = CBTPageGetMeta(metapg);

    /* sanity-check the metapage */
    if (!P_ISMETA(metaopaque) ||
        metad->cbtm_magic != CBT_MAGIC)
        ereport(ERROR,
                (errcode(ERRCODE_INDEX_CORRUPTED),
                        errmsg("index \"%s\" is not a cbtree",
                               RelationGetRelationName(rel))));

    /* if no root page initialized yet, do it */
    if (metad->cbtm_root == InvalidBlockNumber)
    {
        /* If access = BT_READ, caller doesn't want us to create root yet */
        if (access == CBT_READ)
        {
            UnlockReleaseBuffer(metabuf);
            return InvalidBuffer;
        }

        /* trade in our read lock for a write lock */
        LockBuffer(metabuf, BUFFER_LOCK_UNLOCK);
        LockBuffer(metabuf, CBT_WRITE);

        /*
         * Race condition:	if someone else initialized the metadata between
         * the time we released the read lock and acquired the write lock, we
         * must avoid doing it again.
         */
        if (metad->cbtm_root != InvalidBlockNumber)
        {
            /*
             * Metadata initialized by someone else.  In order to guarantee no
             * deadlocks, we have to release the metadata page and start all
             * over again.  (Is that really true? But it's hardly worth trying
             * to optimize this case.)
             */
            UnlockReleaseBuffer(metabuf);
            return cbt_getroot(rel, access);
        }

        /*
         * Get, initialize, write, and leave a lock of the appropriate type on
         * the new root page.  Since this is the first page in the tree, it's
         * a leaf as well as the root.
         */
        rootbuf = cbt_get_buffer(rel, InvalidBlockNumber, CBT_WRITE);
        rootblkno = BufferGetBlockNumber(rootbuf);
        rootpage = BufferGetPage(rootbuf);
        rootopaque = (CBTPageOpaque) PageGetSpecialPointer(rootpage);
        rootopaque->cbto_prev = rootopaque->cbto_next = InvalidBlockNumber;
        rootopaque->cbto_flags = (CBT_LEAF | CBT_ROOT);
        rootopaque->level = CBT_LEAF_LEVEL;

        /* NO ELOG(ERROR) till meta is updated */
        START_CRIT_SECTION();

        metad->cbtm_root = rootblkno;
        metad->cbtm_level = 1;

        MarkBufferDirty(rootbuf);
        MarkBufferDirty(metabuf);

        END_CRIT_SECTION();

        /*
         * swap root write lock for read lock.  There is no danger of anyone
         * else accessing the new root page while it's unlocked, since no one
         * else knows where it is yet.
         */
        LockBuffer(rootbuf, BUFFER_LOCK_UNLOCK);
        LockBuffer(rootbuf, CBT_READ);

        /* okay, metadata is correct, release lock on it */
        UnlockReleaseBuffer(metabuf);
    }
    else
    {
        rootblkno = metad->cbtm_root;
        Assert(rootblkno != InvalidBlockNumber);
        rootlevel = metad->cbtm_level;

        /*
         * Cache the metapage data for next time
         */
        rel->rd_amcache = MemoryContextAlloc(rel->rd_indexcxt,
                                             sizeof(CBTMetaPageData));
        memcpy(rel->rd_amcache, metad, sizeof(CBTMetaPageData));

        /* Set metabuf to rootbuf to use in for loop */
        rootbuf = metabuf;

        for (;;)
        {
            UnlockReleaseBuffer(rootbuf);
            rootbuf = ReadBuffer(rel, rootblkno);
            LockBuffer(rootbuf, CBT_READ);
            rootpage = BufferGetPage(rootbuf);
            rootopaque = (CBTPageOpaque) PageGetSpecialPointer(rootpage);

            if (!P_IGNORE(rootopaque))
                break;

            /* it's dead, Jim.  step right one page */
            if (P_RIGHTMOST(rootopaque))
                elog(ERROR, "no live root page found in index \"%s\"",
                     RelationGetRelationName(rel));
            rootblkno = rootopaque->cbto_next;
        }

        /* Note: can't check btpo.level on deleted pages */
        if (rootopaque->level != rootlevel)
            elog(ERROR, "root page %u of index \"%s\" has level %u, expected %u",
                 rootblkno, RelationGetRelationName(rel),
                 rootopaque->level, rootlevel);
    }

    /*
     * By here, we have a pin and read lock on the root page, and no lock set
     * on the metadata page.  Return the root page's buffer.
     */
    return rootbuf;
}

/*
 * Search the cbtree for a particular scankey. A CBTStack will be returned
 * with the scanning path stored in the stack. The last element in stack is
 * the target item found by search. If the scankey is not in the tree then
 * return NULL.
 */
CBTStack
cbt_search(Relation rel, uint32 pos, Buffer *bufptr, int access)
{
    CBTStack        stack = NULL;

    *bufptr = cbt_getroot(rel, access);

    if (!BufferIsValid(*bufptr))
        return (CBTStack) NULL;

    for (;;)
    {
        Page        page;
        CBTPageOpaque opaque;
        OffsetNumber offnum;
        CBTTuple    cbttuple;
        ItemId      itemid;
        BlockNumber blkno;

        page = BufferGetPage(*bufptr);
        opaque = (CBTPageOpaque) PageGetSpecialPointer(page);
        stack = cbt_search_in_page(*bufptr, pos, stack);

        if (stack == NULL)
        {
            UnlockReleaseBuffer(*bufptr);
            return NULL;
        }

        if (P_ISLEAF(opaque))
           break;

        offnum = stack->cbts_offset;
        itemid = PageGetItemId(page, offnum);
        cbttuple = (CBTTuple) PageGetItem(page, itemid);
        blkno = ItemPointerGetBlockNumber(&(cbttuple->itemptr));

        LockBuffer(*bufptr, BUFFER_LOCK_UNLOCK);
        *bufptr = ReleaseAndReadBuffer(*bufptr, rel, blkno);
        LockBuffer(*bufptr, CBT_READ);
    }

    /* upgrade to write lock if necessary */
    if (access == CBT_WRITE) {
        LockBuffer(*bufptr, BUFFER_LOCK_UNLOCK);
        LockBuffer(*bufptr, access);
    }

    return stack;
}

/*
 * Find the first item in cbtree that satisfy the scan key.
 * Store the result in the index scan descriptor.
 */
bool
cbt_first(IndexScanDesc scan, ScanDirection dir)
{
    ScanKey     sk;
    uint32      pos;
    Buffer      buf;
    CBTStack    stack;
    OffsetNumber offnum;
    Page         page;
    ItemId      id;
    CBTTuple    tuple;
    CBTScanOpaque so = scan->opaque;

    if (!so->first_scan)
        return false;

    sk = scan->keyData;
    if (sk->sk_flags & SK_SEARCHNULL)
        return false;

    pos = (uint32)sk->sk_argument;

    stack = cbt_search(scan->indexRelation, pos, &buf, CBT_READ);

    if (!BufferIsValid(buf) || stack == NULL)
    {
        PredicateLockRelation(scan->indexRelation, scan->xs_snapshot);
        return false;
    }
    else
        PredicateLockPage(scan->indexRelation, BufferGetBlockNumber(buf), scan->xs_snapshot);

    offnum = stack->cbts_offset;
    cbt_freestack(stack);

    /* read data from the page and read heap tid */
    page = BufferGetPage(buf);
    id = PageGetItemId(page, offnum);
    tuple = (CBTTuple) PageGetItem(page, id);
    scan->xs_ctup.t_self = tuple->itemptr;
    UnlockReleaseBuffer(buf);
    so->first_scan = false;

    return true;
}

/*
 *  Recursively free the CBTStack and its parents created when searching.
 */
void
cbt_freestack(CBTStack stack)
{
    CBTStack parent;

    if(stack == NULL)
        return;

    parent = stack->cbts_parent;
    pfree(stack);
    cbt_freestack(parent);
}