/*--------------------------------------------------------
 *
 * cbtbuild.c
 *		Implementation of insertion in a counted btree.
 *
 * IDENTIFICATION
 *		contrib/cbtree/cbtinsert.c
 *
 *--------------------------------------------------------
 */

#include "postgres.h"

#include "cbtree.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "miscadmin.h"
#include "storage/indexfsm.h"
#include "storage/lmgr.h"
#include "access/genam.h"

Buffer cbt_split_page(Relation rel, Buffer origbuf, CBTTuple newitem, CBTStack stack);
void cbt_insert_tuple(Relation index, uint32 position, ItemPointer itmptr);
void cbt_insert_on_page(Relation index, CBTStack stack, CBTTuple newtup, Buffer *buf);
void cbt_change_parent(CBTStack stack, Relation rel, int change);
uint32 cbt_find_totalcnt(Relation index);


/*
 * Insert function in the access method routine.
 */
bool
cbtinsert(Relation index, Datum *values, bool *isnull,
               ItemPointer ht_ctid, Relation heapRel,
               IndexUniqueCheck checkUnique,
               struct IndexInfo *indexInfo)
{
    cbt_insert_tuple(index, DatumGetUInt32(values[0]), ht_ctid);
    return true;
}

/*
 * Find total number of tuples in the counted B tree.
 */
uint32
cbt_find_totalcnt(Relation index)
{
    Buffer      rootbuf;
    Page        rootpage;
    OffsetNumber    maxoff;
    OffsetNumber    offset;
    uint32      total_cnt = 0;

    rootbuf = cbt_getroot(index, CBT_READ);

    if (BufferIsInvalid(rootbuf))
        return 0;
    rootpage = BufferGetPage(rootbuf);
    maxoff = PageGetMaxOffsetNumber(rootpage);

    if (maxoff == 0)
    {
        UnlockReleaseBuffer(rootbuf);
        return 0;
    }

    offset = P_FIRSTOFFSET;

    for (;;)
    {
        ItemId      curid;
        CBTTuple    tuple;

        curid = PageGetItemId(rootpage, offset);

        if (!ItemIdIsDead(curid))
        {
            tuple = (CBTTuple) PageGetItem(rootpage, curid);
            total_cnt += tuple->childcnt;
        }

        if (offset == maxoff) {
            UnlockReleaseBuffer(rootbuf);
            return total_cnt;
        }

        offset = OffsetNumberNext(offset);
    }
}

/*
 * Insert a tuple pointing to itmptr into the tree at specified position.
 */
void
cbt_insert_tuple(Relation index, uint32 position, ItemPointer itmptr)
{
    Buffer              insertionbuf;
    CBTTuple            itup;
    CBTStack            stack;

	/* Sanity check that position is a positive integer. */
	Assert (position > 0);

    stack = cbt_search(index, position, &insertionbuf, CBT_WRITE);

    if (BufferIsInvalid(insertionbuf) || stack == NULL)
    {
        uint32      newpos;
        /*
         * If the position is larger than total number of tuples,
         * then insert the tuple to the last in the sequence
         */
        newpos = cbt_find_totalcnt(index);

        /* If the index is empty then directly build the stack */
        if (newpos == 0)
        {
            insertionbuf = cbt_getroot(index, CBT_READ);
            LockBuffer(insertionbuf, BUFFER_LOCK_UNLOCK);
            LockBuffer(insertionbuf, CBT_WRITE);
            stack = palloc(sizeof(CBTStackData));
            stack->cbts_parent = NULL;
            stack->cbts_blkno = BufferGetBlockNumber(insertionbuf);
            stack->cbts_offset = P_FIRSTOFFSET;
            stack->total_count = 0;
        } else
        {
            stack = cbt_search(index, newpos, &insertionbuf, CBT_WRITE);
            stack->cbts_offset++;
        }
    }

    itup = palloc(sizeof(CBTTupleData));
    CBTFormTuple(itmptr, itup, 1);
    cbt_change_parent(stack->cbts_parent, index, 1);
    cbt_insert_on_page(index, stack, itup, &insertionbuf);
    UnlockReleaseBuffer(insertionbuf);
    cbt_freestack(stack);
}

/*
 * Insert a tuple on page. The buffer is assumed to have write lock and will not be
 * freed. Stack must contain the insertion position and its parents.
 */
void
cbt_insert_on_page(Relation index, CBTStack stack, CBTTuple newtup, Buffer *buf)
{
    Page        page;
    size_t      itemsz;

    if (stack == NULL)
    {
        /* a new root is should be constructed */
        page = BufferGetPage(*buf);
        CBTInitPage(page, CBT_ROOT);
        stack = palloc(sizeof(CBTStackData));
        stack->cbts_blkno = BufferGetBlockNumber(*buf);
        stack->cbts_offset = P_FIRSTOFFSET;
    } else
    {
        page = BufferGetPage(*buf);
    }

    itemsz = MAXALIGN(sizeof(CBTTupleData));
    if (PageGetFreeSpace(page) < itemsz)
    {
        *buf = cbt_split_page(index, *buf, newtup, stack);
    }
    else
    {
        PageAddItem(page, (Item) newtup, itemsz, stack->cbts_offset, false, false);
        START_CRIT_SECTION();

        MarkBufferDirty(*buf);

        END_CRIT_SECTION();
    }
}

void
cbt_change_children(Relation rel, ItemPointer child, BlockNumber parentblkno, OffsetNumber parentoffset)
{
    Buffer          childbuf;
    Page            page;
    CBTPageOpaque   opaque;

    childbuf = cbt_get_buffer(rel, ItemPointerGetBlockNumber(child), CBT_WRITE);
    page = BufferGetPage(childbuf);
    opaque = (CBTPageOpaque )PageGetSpecialPointer(page);

    START_CRIT_SECTION();

    ItemPointerSet(&opaque->cbto_parent, parentblkno, parentoffset);
    MarkBufferDirty(childbuf);

    END_CRIT_SECTION();

    UnlockReleaseBuffer(childbuf);
}

/*
 * Change children count in parents.
 * This function will trace down stack and add change
 * to the children count in parent tuples.
 */
void
cbt_change_parent(CBTStack stack, Relation rel, int change)
{
    ItemId      itemid;
    Buffer      buf;
    Page        page;
    CBTTuple    tuple;

    if (stack == NULL)
        return;

    buf = cbt_get_buffer(rel, stack->cbts_blkno, CBT_WRITE);
    page = BufferGetPage(buf);
    itemid = PageGetItemId(page, stack->cbts_offset);
    tuple = (CBTTuple )PageGetItem(page, itemid);

    START_CRIT_SECTION();

    tuple->childcnt += change;
    MarkBufferDirty(buf);

    END_CRIT_SECTION();

    UnlockReleaseBuffer(buf);
    cbt_change_parent(stack->cbts_parent, rel, change);
}

/*
 * Split a page and insert a new tuple into the correct page.
 * Return the Buffer the new tuple is at and update the stack.
 */
Buffer
cbt_split_page(Relation rel, Buffer origbuf, CBTTuple newitem, CBTStack stack)
{
    Buffer		rbuf;
    Buffer      parent;
    Page        parentpage;
    Page		origpage;
    Page		leftpage,
                rightpage;
    BlockNumber origpagenumber,
                rightpagenumber,
                parentblkno;
    CBTPageOpaque ropaque,
                lopaque,
                oopaque,
                parentopaque;
    Buffer		sbuf = InvalidBuffer;
    Page		spage = NULL;
    CBTPageOpaque sopaque = NULL;
    Size		itemsz;
    ItemId		itemid;
    ItemId      parentitemid;
    CBTTuple	item;
    OffsetNumber leftoff,
                rightoff;
    OffsetNumber maxoff;
    OffsetNumber i;
    OffsetNumber firstright;
    OffsetNumber newitemoff = stack->cbts_offset;
    OffsetNumber newitemoffonpage = InvalidOffsetNumber;
    bool        newitemonleft;
    bool        is_leaf;
    CBTTuple    parenttuple;
    CBTTuple    newparenttuple;
    ItemPointerData ritemptr;
    uint32      leftcount, rightcount;

    /* Acquire a new page to split into */
    rbuf = cbt_get_buffer(rel, InvalidBlockNumber, CBT_WRITE);

    /*
     * origpage is the original page to be split.  leftpage is a temporary
     * buffer that receives the left-sibling data, which will be copied back
     * into origpage on success.  rightpage is the new page that receives the
     * right-sibling data.  If we fail before reaching the critical section,
     * origpage hasn't been modified and leftpage is only workspace. In
     * principle we shouldn't need to worry about rightpage either, because it
     * hasn't been linked into the btree page structure; but to avoid leaving
     * possibly-confusing junk behind, we are careful to rewrite rightpage as
     * zeroes before throwing any error.
     */
    origpage = BufferGetPage(origbuf);
    leftpage = PageGetTempPage(origpage);
    rightpage = BufferGetPage(rbuf);

    origpagenumber = BufferGetBlockNumber(origbuf);
    rightpagenumber = BufferGetBlockNumber(rbuf);

    /*
     * The flag here does not matter since we are going
     * to reset it later.
     * Right page has already been initialized by cbt_set_buffer.
     */
    CBTInitPage(leftpage, CBT_LEAF);

    /*
     * Copy the original page's LSN into leftpage, which will become the
     * updated version of the page.  We need this because XLogInsert will
     * examine the LSN and possibly dump it in a page image.
     */
    PageSetLSN(leftpage, PageGetLSN(origpage));

    /* init cbtree private data */
    oopaque = (CBTPageOpaque) PageGetSpecialPointer(origpage);
    lopaque = (CBTPageOpaque) PageGetSpecialPointer(leftpage);
    ropaque = (CBTPageOpaque) PageGetSpecialPointer(rightpage);

    /* if we're splitting this page, it won't be the root when we're done */
    /* also, clear the SPLIT_END and HAS_GARBAGE flags in both pages */
    lopaque->cbto_flags = oopaque->cbto_flags;
    lopaque->cbto_flags &= ~CBT_ROOT;
    ropaque->cbto_flags = lopaque->cbto_flags;
    /* set flag in left page indicating that the right page has no downlink */
    //lopaque->cbto_flags |= CBT_INCOMPLETE_SPLIT;
    lopaque->cbto_prev = oopaque->cbto_prev;
    lopaque->cbto_next = rightpagenumber;
    ropaque->cbto_prev = origpagenumber;
    ropaque->cbto_next = oopaque->cbto_next;
    lopaque->level = ropaque->level = oopaque->level;

    /*
     * Now transfer all the data items to the appropriate page.
     *
     * Note: we *must* insert at least the right page's items in item-number
     * order, for the benefit of _bt_restore_page().
     */
    //@TODO: handle children's backward pointer when splitting
    maxoff = PageGetMaxOffsetNumber(origpage);
    newitemonleft = (newitemoff <= (maxoff / 2));
    firstright = maxoff / 2 + 1;
    itemsz = sizeof(CBTTupleData);
    itemsz = MAXALIGN(itemsz);
    leftoff = rightoff = P_FIRSTOFFSET;
    leftcount = rightcount = 0;
    is_leaf = P_ISLEAF(oopaque);

    for (i = P_FIRSTOFFSET; i <= maxoff; i = OffsetNumberNext(i))
    {
        itemid = PageGetItemId(origpage, i);
        item = (CBTTuple) PageGetItem(origpage, itemid);

        /*
         * Insert new tuples to the correct page
         * Updating children backward pointer is not necessary since it will
         * be done in children's iteration
         */
        if (i == newitemoff)
        {
            if (newitemonleft)
            {
                PageAddItem(leftpage, (Item) newitem, itemsz, leftoff, false, false);
                newitemoff = leftoff;
                leftoff = OffsetNumberNext(leftoff);
                leftcount += newitem->childcnt;
            }
            else
            {
                PageAddItem(rightpage, (Item) newitem, itemsz, rightoff, false, false);
                newitemoff = rightoff;
                rightoff = OffsetNumberNext(rightoff);
                rightcount += newitem->childcnt;
            }
        }

        /* decide which page to put it on */
        if (i < firstright)
        {
            PageAddItem(leftpage, (Item) item, itemsz, leftoff, false, false);
            /* Change the backward pointer in children's page */
            if (!is_leaf)
                cbt_change_children(rel, &item->itemptr, origpagenumber, leftoff);
            leftoff = OffsetNumberNext(leftoff);
            leftcount += item->childcnt;
        }
        else
        {
            PageAddItem(rightpage, (Item) item, itemsz, rightoff, false, false);
            /* Change the backward pointer in children's page */
            if (!is_leaf)
                cbt_change_children(rel, &item->itemptr, rightpagenumber, rightoff);
            rightoff = OffsetNumberNext(rightoff);
            rightcount += item->childcnt;
        }
    }

    /* cope with possibility that newitem goes at the end */
    if (i <= newitemoff)
    {
        /*
         * Can't have newitemonleft here; that would imply we were told to put
         * *everything* on the left page, which cannot fit (if it could, we'd
         * not be splitting the page).
         */
        PageAddItem(rightpage, (Item) newitem, itemsz, rightoff, false, false);
        newitemoff = rightoff;
        rightoff = OffsetNumberNext(rightoff);
        rightcount += item->childcnt;
    }

    /* now update the parent tuple */
    if (stack->cbts_parent == NULL)
    {
        Buffer              metabuf;
        Page                metapg;
        CBTMetaPageData     *metad;
        ItemPointerData     litemptr;

        /* If the current page is the original root,
         * then build a new root and update the meta page
         */
        metabuf = cbt_get_buffer(rel, CBT_METAPAGE, CBT_WRITE);
        metapg = BufferGetPage(metabuf);
        metad = CBTPageGetMeta(metapg);

        parent = cbt_get_buffer(rel, InvalidBlockNumber, CBT_WRITE);
        parentblkno = BufferGetBlockNumber(parent);
        parentopaque = (CBTPageOpaque) PageGetSpecialPointer(BufferGetPage(parent));
        parentopaque->cbto_prev = parentopaque->cbto_next = InvalidBlockNumber;
        parentopaque->cbto_flags = CBT_ROOT;
        parentopaque->level = oopaque->level + 1;

        START_CRIT_SECTION();

        metad->cbtm_root = parentblkno;
        metad->cbtm_level = parentopaque->level;

        MarkBufferDirty(parent);
        MarkBufferDirty(metabuf);

        END_CRIT_SECTION();
        UnlockReleaseBuffer(metabuf);

        /* Insert the left page into the new root */
        parenttuple = (CBTTuple) palloc(sizeof(CBTTupleData));
        ItemPointerSet(&litemptr, origpagenumber, P_FIRSTOFFSET);
        CBTFormTuple(&litemptr, parenttuple, (uint32)leftcount);
        stack->cbts_parent = palloc(sizeof(CBTStackData));
        stack->cbts_parent->cbts_offset = P_FIRSTOFFSET;
        stack->cbts_parent->cbts_blkno = parentblkno;
        stack->cbts_parent->cbts_parent = NULL;
        cbt_insert_on_page(rel, stack->cbts_parent, parenttuple, &parent);
        ItemPointerSet(&oopaque->cbto_parent, parentblkno, P_FIRSTOFFSET);
    }
    else
    {
        parent = cbt_get_buffer(rel, stack->cbts_parent->cbts_blkno, CBT_WRITE);
        parentpage = BufferGetPage(parent);
        parentitemid = PageGetItemId(parentpage, stack->cbts_parent->cbts_offset);
        parenttuple = (CBTTuple) PageGetItem(parentpage, parentitemid);
        parenttuple->childcnt = (uint32) leftcount;
    }
    newparenttuple = palloc(sizeof(CBTTupleData));
    ItemPointerSet(&ritemptr, BufferGetBlockNumber(rbuf), P_FIRSTOFFSET);
    CBTFormTuple(&ritemptr, newparenttuple, (uint32) rightcount);
    stack->cbts_parent->cbts_offset++;
    cbt_insert_on_page(rel, stack->cbts_parent, newparenttuple, &parent);
    ItemPointerSet(&ropaque->cbto_parent, stack->cbts_parent->cbts_blkno, stack->cbts_parent->cbts_offset);
    UnlockReleaseBuffer(parent);

    /*
     * We have to grab the right sibling (if any) and fix the prev pointer
     * there. We are guaranteed that this is deadlock-free since no other
     * writer will be holding a lock on that page and trying to move left, and
     * all readers release locks on a page before trying to fetch its
     * neighbors.
     */

    if (!P_RIGHTMOST(oopaque))
    {
        sbuf = cbt_get_buffer(rel, oopaque->cbto_next, CBT_WRITE);
        spage = BufferGetPage(sbuf);
        sopaque = (CBTPageOpaque) PageGetSpecialPointer(spage);
        if (sopaque->cbto_prev != origpagenumber)
        {
            memset(rightpage, 0, BufferGetPageSize(rbuf));
            elog(ERROR, "right sibling's left-link doesn't match: "
                    "block %u links to %u instead of expected %u in index \"%s\"",
                 oopaque->cbto_next, sopaque->cbto_prev, origpagenumber,
                 RelationGetRelationName(rel));
        }

        /*
         * Check to see if we can set the SPLIT_END flag in the right-hand
         * split page; this can save some I/O for vacuum since it need not
         * proceed to the right sibling.  We can set the flag if the right
         * sibling has a different cycleid: that means it could not be part of
         * a group of pages that were all split off from the same ancestor
         * page.  If you're confused, imagine that page A splits to A B and
         * then again, yielding A C B, while vacuum is in progress.  Tuples
         * originally in A could now be in either B or C, hence vacuum must
         * examine both pages.  But if D, our right sibling, has a different
         * cycleid then it could not contain any tuples that were in A when
         * the vacuum started.
         *
         * if (sopaque->btpo_cycleid != ropaque->btpo_cycleid)
         *   ropaque->btpo_flags |= BTP_SPLIT_END;
         */
    }

    /*
     * Right sibling is locked, new siblings are prepared, but original page
     * is not updated yet.
     *
     * NO EREPORT(ERROR) till right sibling is updated.  We can get away with
     * not starting the critical section till here because we haven't been
     * scribbling on the original page yet; see comments above.
     */
    START_CRIT_SECTION();

    /*
     * By here, the original data page has been split into two new halves, and
     * these are correct.  The algorithm requires that the left page never
     * move during a split, so we copy the new left page back on top of the
     * original.  Note that this is not a waste of time, since we also require
     * (in the page management code) that the center of a page always be
     * clean, and the most efficient way to guarantee this is just to compact
     * the data by reinserting it into a new left page.  (XXX the latter
     * comment is probably obsolete; but in any case it's good to not scribble
     * on the original page until we enter the critical section.)
     *
     * We need to do this before writing the WAL record, so that XLogInsert
     * can WAL log an image of the page if necessary.
     */
    PageRestoreTempPage(leftpage, origpage);
    /* leftpage, lopaque must not be used below here */

    MarkBufferDirty(origbuf);
    MarkBufferDirty(rbuf);

    if (!P_RIGHTMOST(ropaque))
    {
        sopaque->cbto_prev = rightpagenumber;
        MarkBufferDirty(sbuf);
    }

    /*
     * Clear INCOMPLETE_SPLIT flag on child if inserting the new item finishes
     * a split.
     */
    //if (!isleaf)
    //{
        //Page		cpage = BufferGetPage(cbuf);
        //CBTPageOpaque cpageop = (CBTPageOpaque) PageGetSpecialPointer(cpage);

        //cpageop->cbto_flags &= ~CBT_INCOMPLETE_SPLIT;
        //MarkBufferDirty(cbuf);
    //}

    END_CRIT_SECTION();

    /* release the old right sibling */
    if (!P_RIGHTMOST(ropaque))
        UnlockReleaseBuffer(sbuf);

    /* release the child */
    //if (!isleaf)
       // UnlockReleaseBuffer(cbuf);

    if (newitemonleft)
    {
        stack->cbts_blkno = origpagenumber;
        stack->cbts_offset = newitemoff;
        UnlockReleaseBuffer(rbuf);
        return origbuf;
    }
    else
    {
        stack->cbts_blkno = rightpagenumber;
        stack->cbts_offset = newitemoff;
        UnlockReleaseBuffer(origbuf);
        return rbuf;
    }
}

/*
 * Get a buffer of on the specified page. If blkno is not valid,
 * then request a new buffer if access is CBT_WRITE.
 */
Buffer
cbt_get_buffer(Relation rel, BlockNumber blkno, int access)
{
    Buffer      buf;
    if (blkno != InvalidBlockNumber)
    {
        /* Read a existing block of the relation */
        buf = ReadBuffer(rel, blkno);
        LockBuffer(buf, access);
    }
    else
    {
        bool        needLock;
        Page        page;

        Assert(access == CBT_WRITE);

        buf = ReadBuffer(rel, P_NEW);

        /* Acquire buffer lock on new page */
        LockBuffer(buf, CBT_WRITE);

        /* Initialize the new page before returning it */
        page = BufferGetPage(buf);
        Assert(PageIsNew(page));
        CBTInitPage(page, CBT_LEAF);
    }

    /* ref count and lock type are correct */
    return buf;
}
