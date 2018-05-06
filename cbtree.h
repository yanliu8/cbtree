/*-------------------------------------------------------------------------
 *
 * cbtree.h
 *		Header for counted btree index.
 *
 * IDENTIFICATION
 *		contrib/cbtree/cbtree.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _CBTREE_H_
#define _CBTREE_H_

#include "postgres.h"

#include "storage/bufpage.h"
#include "storage/itemptr.h"
#include "utils/relcache.h"
#include "access/genam.h"
#include "nodes/relation.h"

#define CBTREE_NSTRATEGIES		 1
#define CBTREE_EQUAL_STRATEGY	 1

#define CBTREE_NPROC			 1

typedef struct CBTPageOpaqueData
{
    BlockNumber cbto_prev;
    BlockNumber cbto_next;
	ItemPointerData cbto_parent;
    uint32      level;
    uint16      cbto_flags;

} CBTPageOpaqueData;

typedef CBTPageOpaqueData *CBTPageOpaque;

#define CBT_LEAF        (1 << 0)
#define CBT_ROOT        (1 << 1)
#define CBT_META        (1 << 2)
#define CBT_DELETED     (1 << 3)
#define CBT_HALF_DEAD	(1 << 4)

#define CBTPageGetOpaque(page) ((CBTPageOpaque) PageGetSpecialPointer(page))
#define CBTPageIsMeta(page) \
	((CBTPageGetOpaque(page)->cbto_flags & CBT_META) != 0)
#define CBTPageIsDeleted(page) \
	((CBTPageGetOpaque(page)->cbto_flags & CBT_DELETED) != 0)
#define CBTPageSetDeleted(page) \
	(CBTPageGetOpaque(page)->cbto_flags |= CBT_DELETED)

typedef struct CBTMetaPageData
{
    uint32     cbtm_magic;
    BlockNumber cbtm_root;
    uint32      cbtm_level;

	//BlockNumber cbtm_fastroot;
	//uint32 		cbtm_fastlevel;
} CBTMetaPageData;

#define CBTPageGetMeta(p) \
	((CBTMetaPageData *) PageGetContents(p))

#define CBT_METAPAGE    0
#define CBT_MAGIC       0x0451253

#define P_LEFTMOST(opaque)		((opaque)->cbto_prev == InvalidBlockNumber)
#define P_RIGHTMOST(opaque)		((opaque)->cbto_next == InvalidBlockNumber)
#define P_ISLEAF(opaque)		(((opaque)->cbto_flags & CBT_LEAF) != 0)
#define P_ISROOT(opaque)		(((opaque)->cbto_flags & CBT_ROOT) != 0)
#define P_ISDELETED(opaque)		(((opaque)->cbto_flags & CBT_DELETED) != 0)
#define P_ISHALFDEAD(opaque)	(((opaque)->cbto_flags & CBT_HALF_DEAD) != 0)
#define P_IGNORE(opaque)		(((opaque)->cbto_flags & (CBT_DELETED|CBT_HALF_DEAD)) != 0)
#define P_ISMETA(opaque)		(((opaque)->cbto_flags & CBT_META) != 0)

typedef struct CBTTupleData
{
    ItemPointerData itemptr;
    uint32          childcnt;
}  CBTTupleData;

typedef CBTTupleData *CBTTuple;

#define CBT_READ			BUFFER_LOCK_SHARE
#define CBT_WRITE		BUFFER_LOCK_EXCLUSIVE

#define P_FIRSTOFFSET   1

typedef struct CBTStackData
{
    BlockNumber cbts_blkno;
    OffsetNumber cbts_offset;
    uint32      total_count;
    struct CBTStackData *cbts_parent;
} CBTStackData;

typedef CBTStackData *CBTStack;

#define CBTREE_MIN_FILLFACTOR		10
#define CBTREE_DEFAULT_FILLFACTOR	90
#define CBTREE_NONLEAF_FILLFACTOR	70

#define CBT_LEAF_LEVEL              1


#define CBTScanPosIsPinned(scanpos) \
( \
	AssertMacro(BlockNumberIsValid((scanpos).currPage) || \
				!BufferIsValid((scanpos).buf)), \
	BufferIsValid((scanpos).buf) \
)
#define CBTScanPosUnpin(scanpos) \
	do { \
		ReleaseBuffer((scanpos).buf); \
		(scanpos).buf = InvalidBuffer; \
	} while (0)
#define CBTScanPosUnpinIfPinned(scanpos) \
	do { \
		if (CBTScanPosIsPinned(scanpos)) \
			CBTScanPosUnpin(scanpos); \
	} while (0)

#define CBTScanPosIsValid(scanpos) \
( \
	AssertMacro(BlockNumberIsValid((scanpos).currPage) || \
				!BufferIsValid((scanpos).buf)), \
	BlockNumberIsValid((scanpos).currPage) \
)
#define CBTScanPosInvalidate(scanpos) \
	do { \
		(scanpos).currPage = InvalidBlockNumber; \
		(scanpos).nextPage = InvalidBlockNumber; \
		(scanpos).buf = InvalidBuffer; \
		(scanpos).lsn = InvalidXLogRecPtr; \
		(scanpos).nextTupleOffset = 0; \
	} while (0);

#define MaxCBTTuplesPerPage	\
	((int) ((BLCKSZ - SizeOfPageHeaderData) / \
			(MAXALIGN(sizeof(CBTTupleData) + 1) + sizeof(ItemIdData))))


extern void CBTInitPage(Page page, uint16 flags);
extern void CBTFormTuple(ItemPointer itptr, CBTTuple itup, uint32 childcnt);

extern bool cbtvalidate(Oid opclassoid);

/* index access method interface functions */
extern bool cbtinsert(Relation index, Datum *values, bool *isnull,
                     ItemPointer ht_ctid, Relation heapRel,
                     IndexUniqueCheck checkUnique,
                     struct IndexInfo *indexInfo);
extern IndexScanDesc cbtbeginscan(Relation r, int nkeys, int norderbys);
extern bool cbtgettuple(IndexScanDesc scan, ScanDirection dir);
extern void cbtrescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
                     ScanKey orderbys, int norderbys);
extern void cbtendscan(IndexScanDesc scan);
extern IndexBuildResult *cbtbuild(Relation heap, Relation index,
                                 struct IndexInfo *indexInfo);
extern void cbtbuildempty(Relation index);
extern IndexBulkDeleteResult *cbtbulkdelete(IndexVacuumInfo *info,
                                           IndexBulkDeleteResult *stats, IndexBulkDeleteCallback callback,
                                           void *callback_state);
extern IndexBulkDeleteResult *cbtvacuumcleanup(IndexVacuumInfo *info,
                                              IndexBulkDeleteResult *stats);
extern bytea *cbtoptions(Datum reloptions, bool validate);
extern void cbtcostestimate(struct PlannerInfo *root, struct IndexPath *path,
                           double loop_count, Cost *indexStartupCost,
                           Cost *indexTotalCost, Selectivity *indexSelectivity,
                           double *indexCorrelation, double *indexPages);
extern Buffer cbt_get_buffer(Relation rel, BlockNumber blkno, int access);
extern bool cbtcanreturn(Relation index, int attno);
extern Buffer cbt_getroot(Relation rel, int access);
extern CBTStack cbt_search(Relation rel, uint32 pos, Buffer *bufptr, int access);
extern void cbt_freestack(CBTStack stack);

#endif