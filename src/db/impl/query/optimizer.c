#include "optimizer.h"

#include "algorithms.h"
#include "btree.h"

void init_column_index(Column *col, message *send_message) {
  if (!col->index) {
    handle_error(send_message,
                 "Column index should have been initialized before loading data");
    return;
  }

  // Allocate and copy the data from the column to the index (Not sorted yet)
  col->index->sorted_data = malloc(sizeof(int) * col->num_elements);
  col->index->positions = malloc(sizeof(int) * col->num_elements);
  if (!col->index->sorted_data || !col->index->positions) {
    handle_error(send_message, "Failed to allocate memory for sorted data");
    log_err("init_column_index: Failed to allocate memory for sorted data\n");
    return;
  }
  // Copy the data from the column to the index
  memcpy(col->index->sorted_data, col->data, sizeof(int) * col->num_elements);

  // Sort the data and keep track of the original positions
  if (sort(col->index->sorted_data, col->num_elements, col->index->positions) != 0) {
    handle_error(send_message, "Failed to sort data");
    log_err("init_column_index: Failed to sort data\n");
    return;
  }
}
void create_idx_on(Column *col, message *send_message) {
  if (!col->index || col->index->idx_type == NONE) return;

  // Any column with an index needs to have ColumnIndex initialized
  init_column_index(col, send_message);
  cs165_log(stdout, "Initialized column index for column %s\n", col->name);

  IndexType idx_type = col->index->idx_type;
  if (idx_type == BTREE_CLUSTERED || idx_type == BTREE_UNCLUSTERED) {
    // Create the btree index
    col->root = init_btree(col->index->sorted_data, col->num_elements, BTREE_FANOUT);
    if (!col->root) {
      log_err("Failed to create btree index for column %s\n", col->name);
      return;
    }
    cs165_log(stdout, "Created btree index for column %s\n", col->name);
  }
}

void cluster_idx_on(Table *table, Column *primary_col, message *send_message) {
  (void)table;
  (void)primary_col;
  (void)send_message;
  log_err(
      "cluster_idx: clustering index not implemented yet; Got table: %s, primary column: "
      "%s\n",
      table->name, primary_col->name);
}
