#include "optimizer.h"

#include "algorithms.h"
#include "btree.h"

void reorder_nums(int *data, size_t n_elements, int *idx_order);

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
  } else {
    col->root = NULL;
  }
}

void cluster_idx_on(Table *table, Column *primary_col, message *send_message) {
  log_err(
      "cluster_idx: clustering index not implemented yet; Got table: %s, primary column: "
      "%s\n",
      table->name, primary_col->name);

  // Cluster the primary column if it exists
  int *idx_order = primary_col->index->positions;

  for (size_t i = 0; i < table->num_cols; i++) {
    Column *col = &table->columns[i];
    if (col == primary_col) continue;
    reorder_nums(col->data, col->num_elements, idx_order);
  }
  memcpy(primary_col->data, primary_col->index->sorted_data,
         sizeof(int) * primary_col->num_elements);

  // erase old positions
  for (size_t i = 0; i < primary_col->num_elements; i++) {
    primary_col->index->positions[i] = i;
  }
}

size_t idx_lookup_left(Column *col, int value) {
  if (!col->index || col->index->idx_type == NONE) {
    log_err("idx_lookup: Column %s does not have an index\n", col->name);
    return 0;
  }

  if (value < col->min_value) {
    return -1;
  } else if (value > col->max_value) {
    return -2;
  }

  int *sorted_data = col->index->sorted_data;
  size_t num_elements = col->num_elements;
  IndexType idx_type = col->index->idx_type;
  ssize_t match_idx = 0;

  // Handle edge cases
  if (value <= sorted_data[0]) return 0;
  if (value >= sorted_data[num_elements - 1]) return num_elements - 1;

  if (idx_type == BTREE_CLUSTERED || idx_type == BTREE_UNCLUSTERED) {
    match_idx = lookup(value, col->root);
  } else if (idx_type == SORTED_CLUSTERED || idx_type == SORTED_UNCLUSTERED) {
    // Binary search for sorted index
    size_t left = 0;
    size_t right = num_elements - 1;

    while (left < right) {
      size_t mid = left + (right - left + 1) / 2;  // Ceiling division

      if (sorted_data[mid] <= value) {
        left = mid;
      } else {
        right = mid - 1;
      }
    }
    match_idx = left;
  }

  //   Worst case that the sorted_data[match_idx] = value = sorted_data[match_idx]
  //    we want idx s.t. col->data[i - 1] < value
  log_err("Optimizer: should linear search after index lookup? %s\n",
          match_idx > 0 && sorted_data[match_idx] == value ? "Yes" : "No");

  while (match_idx > 0 && sorted_data[match_idx] == value) {
    match_idx--;
  }
  return match_idx;
}

void reorder_nums(int *data, size_t n_elements, int *idx_order) {
  // Handle empty array case
  if (n_elements == 0) return;

  // Allocate temporary array
  int *temp = malloc(n_elements * sizeof(int));
  if (!temp) return;  // Handle allocation failure

  for (size_t i = 0; i < n_elements; i++) {
    // idx_order[i] = j means value at data[j] should go to position i
    temp[i] = data[idx_order[i]];
  }

  // Copy back to original array
  for (size_t i = 0; i < n_elements; i++) {
    data[i] = temp[i];
  }

  free(temp);
}