#include "postgres.h"

#include "nodes/relation.h"

void cbtcostestimate (PlannerInfo *root,
                IndexPath *path,
                double loop_count,
                Cost *indexStartupCost,
                Cost *indexTotalCost,
                Selectivity *indexSelectivity,
                double *indexCorrelation, double *indexPages)
{

}