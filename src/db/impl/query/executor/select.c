#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

#include "client_context.h"
#include "operators.h"
#include "query_exec.h"
#include "utils.h"

// Function prototypes
size_t select_values_basic(const int *data, size_t num_elements, Comparator *comparator,
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
  Column *column = comparator->col;

  // Create a new Column to store the result indices
  Column *result;
  if (create_new_handle(select_op->res_handle, &result) != 0) {
    log_err("exec_select: Failed to create new handle\n");
    send_message->status = EXECUTION_ERROR;
    send_message->length = 0;
    send_message->payload = NULL;
    return;
  }
  result->data_type = INT;  // Select returns an array of indices/integers

  // Allocate memory for the result data
  //   For simplicity, we will allocate the maximum possible size (for now).
  //   TODO: replace this with a dynamic array after implementing such a data structure.
  result->data = malloc(sizeof(int) * column->num_elements);
  if (!result->data) {
    log_err("exec_select: Failed to allocate memory for result data\n");
    send_message->status = EXECUTION_ERROR;
    send_message->length = 0;
    send_message->payload = NULL;
    return;
  }

  cs165_log(stdout, "exec_select: Starting to scan\n");
  result->num_elements =
      select_values_basic(column->data, column->num_elements, comparator, result->data);

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
size_t select_values_basic(const int *data, size_t num_elements, Comparator *comparator,
                           int *result_indices) {
  if (result_indices == NULL) {
    log_err("select_values_basic: result_indices is NULL\n");
    return -1;
  }

  ComparatorType type1 = comparator->type1;
  ComparatorType type2 = comparator->type2;
  long int p_low = comparator->p_low;
  long int p_high = comparator->p_high;
  int *ref_posns = comparator->ref_posns;
  if (ref_posns) {
    cs165_log(stdout, "select_values_basic: ref_posns: %d, %d, ...\n", ref_posns[0],
              ref_posns[1]);
  }

  size_t result_count = 0;

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
      result_indices[result_count++] = ref_posns ? (size_t)ref_posns[i] : i;
    }
  }

  return result_count;
}
