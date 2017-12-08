#include "postgres.h"

#include "access/genam.h"

IndexBulkDeleteResult *
cbtbulkdelete(IndexVacuumInfo *info,
                                            IndexBulkDeleteResult *stats, IndexBulkDeleteCallback callback,
                                            void *callback_state)
{
    return NULL;
}

IndexBulkDeleteResult *
cbtvacuumcleanup(IndexVacuumInfo *info,
                                               IndexBulkDeleteResult *stats)
{
    return NULL;
}
