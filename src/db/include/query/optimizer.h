#ifndef OPTIMIZER_H
#define OPTIMIZER_H

#include "operators.h"
#include "utils.h"

// For handling index creation
void create_idx_on(Column *col, message *send_message);
void cluster_idx_on(Table *table, Column *primary_col, message *send_message);

#endif /*  OPTIMIZER_H */
