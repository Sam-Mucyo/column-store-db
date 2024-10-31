#include <string.h>

#include "query_exec.h"
#include "utils.h"

void exec_insert(DbOperator *query, message *send_message) {
  cs165_log(stdout, "Executing insert query.\n");
  InsertOperator *insert_op = &query->operator_fields.insert_operator;

  Column *cols = insert_op->table->columns;
  size_t num_cols = insert_op->table->num_cols;
  int *values = query->operator_fields.insert_operator.values;

  for (size_t i = 0; i < num_cols; i++) {
    cs165_log(stdout, "adding %d to col %s\n", values[i], cols[i].name);
    // Update values at offset 100, extending if needed
    int *new_region = extend_and_update_mmap(cols[i].data, &cols[i].mmap_size,
                                             cols[i].num_elements, &values[i], 1);
    if (new_region == NULL) {
      send_message->status = EXECUTION_ERROR;
      send_message->payload = "Failed to extend and update mmap";
      send_message->length = strlen(send_message->payload);
      return;
    }
    cols[i].data = new_region;

    // Update metadata
    cols[i].num_elements++;
    cols[i].min_value = values[i] < cols[i].min_value ? values[i] : cols[i].min_value;
    cols[i].max_value = values[i] > cols[i].max_value ? values[i] : cols[i].max_value;
    cols[i].sum += values[i];
    cols[i].is_dirty = 1;
  }

  log_info("successfully added new values in table");
  send_message->status = OK_DONE;
  send_message->payload = "Done";
  send_message->length = strlen(send_message->payload);
  return;
}
