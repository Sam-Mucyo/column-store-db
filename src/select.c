#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

#include "operators.h"
#include "query_exec.h"
#include "utils.h"

// Function prototypes
size_t select_values_basic(const int *data, size_t num_elements, ComparatorType type1,
                           int p_low, ComparatorType type2, int p_high,
                           int *result_indices);
/**
 * @brief exec_select
 * Executes a select query and returns the status of the query.
 * In doing so, it will update the global variable pool that's managed by the
 * client_context.c
 *
 *
 * @param query (DbOperator*): a DbOperator of type SELECT.
 * @return Status
 */
void exec_select(DbOperator *query, message *send_message) {
  SelectOperator *select_op = &query->operator_fields.select_operator;
  Comparator *comparator = select_op->comparator;
  GeneralizedColumn *gen_col = comparator->gen_col;

  if (gen_col->column_type != COLUMN) {
    send_message->status = QUERY_UNSUPPORTED;
    send_message->payload = "exec_select: select operator only supports column type";
    send_message->length = strlen(send_message->payload);
    return;
  }
  Column *column = gen_col->column_pointer.column;

  // Create a new Result to store the selection result
  Result *result = malloc(sizeof(Result));
  result->data_type = INT;
  result->num_tuples = 0;

  //   For simplicity, we will allocate the maximum possible size (for now).
  //   TODO: replace this with a dynamic array after implementing such a data structure.
  result->payload = malloc(sizeof(int) * column->num_elements);

  cs165_log(stdout, "exec_select: Starting to scan\n");
  result->num_tuples = select_values_basic(
      column->data, column->num_elements, comparator->type1, comparator->p_low,
      comparator->type2, comparator->p_high, result->payload);

  cs165_log(stdout, "Selected %d indices: \n", result->num_tuples);
  for (size_t i = 0; i < result->num_tuples; i++) {
    printf("%d, ", ((int *)result->payload)[i]);
  }
  printf("\n");

  if (g_client_context->variables_in_use >= MAX_VARIABLES) {
    free(result->payload);
    free(result);
    send_message->status = EXECUTION_ERROR;
    send_message->payload = "Maximum number of variables reached";
    send_message->length = strlen(send_message->payload);
    return;
  }

  GeneralizedColumnHandle *handle =
      &g_client_context->variable_pool[g_client_context->variables_in_use];
  snprintf(handle->name, HANDLE_MAX_SIZE, "%s", comparator->handle);
  handle->generalized_column.column_type = RESULT;
  handle->generalized_column.column_pointer.result = result;

  g_client_context->variables_in_use++;

  // If a handle was provided in the query, update the chandle_table
  if (comparator->handle != NULL) {
    if (add_handle(comparator->handle, &handle->generalized_column).code != OK) {
      free(result->payload);
      free(result);
      send_message->status = EXECUTION_ERROR;
      send_message->payload = "Failed to add handle to chandle_table";
      send_message->length = strlen(send_message->payload);
      return;
    }
  }
  //   Add logging info
  log_info("exec_select: Selection operation completed successfully.\n");

  //   set send_message
  send_message->status = OK_DONE;
  send_message->payload = "Done";
  send_message->length = strlen(send_message->payload);
  return;
}

// Basic initial comparison function using switch-case
int compare(ComparatorType type, long int p, int value) {
  switch (type) {
    case LESS_THAN:
      return value < p;
    case GREATER_THAN:
      return value > p;
    case EQUAL:
      return value == p;
    case LESS_THAN_OR_EQUAL:
      return value <= p;
    case GREATER_THAN_OR_EQUAL:
      return value >= p;
    default:
      return 0;
  }
}

// Basic selection function (initial version)
size_t select_values_basic(const int *data, size_t num_elements, ComparatorType type1,
                           int p_low, ComparatorType type2, int p_high,
                           int *result_indices) {
  size_t result_count = 0;
  if (result_indices == NULL) {
    log_err("select_values_basic: result_indices is NULL\n");
    return -1;
  }

  for (size_t i = 0; i < num_elements; i++) {
    int value = data[i];
    bool include = false;

    // cs165_log(stdout, "comparing value %d with low %ld and high %ld\n", value,
    //           comparator->p_low, comparator->p_high);
    // cs165_log(stdout, "Type1: %d, Type2: %d\n", comparator->type1,
    // comparator->type2);
    if (type1 == NO_COMPARISON && type2 != NO_COMPARISON &&
        compare(type2, p_high, value)) {
      include = true;
    }
    if (type2 == NO_COMPARISON && type1 != NO_COMPARISON &&
        compare(type1, p_low, value)) {
      include = true;
    }
    if (type1 == GREATER_THAN_OR_EQUAL && type2 == LESS_THAN &&
        compare(type1, p_low, value) && compare(type2, p_high, value)) {
      include = true;
    }

    if (include) {
      //   cs165_log(stdout, "including value %d\n", value);
      result_indices[result_count++] = i;
    }
  }

  return result_count;
}
