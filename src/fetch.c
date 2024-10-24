#include <string.h>

#include "query_exec.h"
#include "utils.h"

void exec_fetch(DbOperator *query, message *send_message) {
  cs165_log(stdout, "Executing fetch query.\n");
  FetchOperator *fetch_op = &query->operator_fields.fetch_operator;

  // Get the Result from the select handle
  GeneralizedColumn *select_gen_col = get_handle(fetch_op->select_handle);
  if (!select_gen_col || select_gen_col->column_type != RESULT) {
    send_message->status = EXECUTION_ERROR;
    send_message->payload = "Invalid select handle or not a Result type";
    send_message->length = strlen(send_message->payload);
    log_err("L%d in exec_fetch: %s\n", __LINE__, send_message->payload);
    return;
  }

  cs165_log(stdout, "Got select handle from variable pool/client context.\n");

  Result *select_result = select_gen_col->column_pointer.result;

  // Get the Column to fetch from
  Column *fetch_col = fetch_op->col;
  if (!fetch_col) {
    send_message->status = EXECUTION_ERROR;
    send_message->payload = "Invalid fetch column";
    send_message->length = strlen(send_message->payload);
    log_err("L%d in exec_fetch: %s\n", __LINE__, send_message->payload);
    return;
  }
  cs165_log(stdout, "Got fetch column from catalog.\n");

  // Create a new Result to store the fetched values
  Result *fetch_result = malloc(sizeof(Result));
  if (!fetch_result) {
    send_message->status = EXECUTION_ERROR;
    send_message->payload = "Failed to allocate memory for fetch result";
    send_message->length = strlen(send_message->payload);
    log_err("L%d in exec_fetch: %s\n", __LINE__, send_message->payload);
    return;
  }

  fetch_result->num_tuples = select_result->num_tuples;
  fetch_result->payload = malloc(sizeof(int) * fetch_result->num_tuples);
  fetch_result->data_type = INT;
  if (!fetch_result->payload) {
    send_message->status = EXECUTION_ERROR;
    send_message->payload = "Failed to allocate memory for fetch result payload";
    send_message->length = strlen(send_message->payload);
    log_err("L%d in exec_fetch: %s\n", __LINE__, send_message->payload);
    return;
  }

  // Fetch the values
  //   cs165_log(stdout, "Fetched values: ");
  for (size_t i = 0; i < select_result->num_tuples; i++) {
    size_t index = ((int *)select_result->payload)[i];
    if (index >= fetch_col->num_elements) {
      free(fetch_result->payload);
      free(fetch_result);
      send_message->status = EXECUTION_ERROR;
      send_message->payload = "Index out of bounds in fetch operation";
      send_message->length = strlen(send_message->payload);
      log_err("L%d in exec_fetch: %s\n", __LINE__, send_message->payload);
      return;
    }
    // cs165_log(stdout, "%d ", fetch_col->data[index]);
    ((int *)fetch_result->payload)[i] = fetch_col->data[index];
  }
  //   cs165_log(stdout, "\n");

  // Store the result in the client context
  if (g_client_context->variables_in_use >= MAX_VARIABLES) {
    free(fetch_result->payload);
    free(fetch_result);
    send_message->status = EXECUTION_ERROR;
    send_message->payload = "Maximum number of variables reached";
    send_message->length = strlen(send_message->payload);
    log_err("L%d in exec_fetch: %s\n", __LINE__, send_message->payload);
    return;
  }

  GeneralizedColumnHandle *handle =
      &g_client_context->variable_pool[g_client_context->variables_in_use];
  snprintf(handle->name, HANDLE_MAX_SIZE, "%s", fetch_op->fetch_handle);
  handle->generalized_column.column_type = RESULT;
  handle->generalized_column.column_pointer.result = fetch_result;

  g_client_context->variables_in_use++;

  // Add the new handle to the chandle_table
  if (add_handle(fetch_op->fetch_handle, &handle->generalized_column).code != OK) {
    send_message->status = EXECUTION_ERROR;
    send_message->payload = "Server Failed to add handle to chandle_table";
    send_message->length = strlen(send_message->payload);
    log_err("L%d in exec_fetch: %s\n", __LINE__, send_message->payload);
  }

  log_info("Fetch operation completed successfully.\n");
  send_message->status = OK_DONE;
  send_message->payload = "Done";
  send_message->length = strlen(send_message->payload);
  return;
}
