#include "query_handler.h"

#include <string.h>

#include "client_context.h"
#include "include/operators.h"
#include "query_exec.h"
#include "utils.h"

void handle_avg(DbOperator *query, message *send_message);
void handle_print(DbOperator *query, message *send_message);

/**
 * @brief execute_DbOperator
 * Executes a query and returns the result to be sent back to the client.
 *
 * @param query (DbOperator*): the query to execute on the database
 * @return char*
 */
void handle_dbOperator(DbOperator *query, message *send_message) {
  switch (query->type) {
    case CREATE:
      exec_create(query, send_message);
      break;
    case SELECT:
      exec_select(query, send_message);
      break;
    case FETCH:
      exec_fetch(query, send_message);
      break;
    case PRINT:
      handle_print(query, send_message);
      break;
    case AVG:
      handle_avg(query, send_message);
      break;
    case INSERT:
      exec_insert(query, send_message);
      break;
    default:
      cs165_log(stdout, "execute_DbOperator: Unknown query type\n");
      break;
  }
  db_operator_free(query);
}

void handle_avg(DbOperator *query, message *send_message) {
  cs165_log(stdout, "Executing avg query:\navg_handle: %s\nfetch_handle: %s\n",
            query->operator_fields.avg_operator.avg_handle,
            query->operator_fields.avg_operator.fetch_handle);

  // Get the Column from the fetch handle
  AvgOperator *avg_op = &query->operator_fields.avg_operator;
  Column *col = get_handle(avg_op->fetch_handle);
  if (!col) {
    handle_error(send_message, "Invalid fetch handle\n");
    log_err("L%d in exec_avg: %s\n", __LINE__, send_message->payload);
  }

  // Create a new Column to store the result
  Column *avg_col;
  if (create_new_handle(avg_op->avg_handle, &avg_col) != 0) {
    handle_error(send_message, "Failed to create new handle\n");
    log_err("L%d in exec_avg: %s\n", __LINE__, send_message->payload);
  }
  cs165_log(stdout, "added new handle: %s\n", avg_op->avg_handle);

  avg_col->data = malloc(sizeof(double));
  if (!avg_col->data) {
    free(avg_col);
    handle_error(send_message, "Failed to allocate memory for result data");
  }
  // Set the result of the average operation
  *((double *)avg_col->data) =
      col->num_elements == 0 ? 0.0 : (double)col->sum / col->num_elements;

  avg_col->data_type = FLOAT;
  avg_col->num_elements = 1;

  log_info("Average operation completed successfully. Average: %f\n",
           *((double *)avg_col->data));
  send_message->status = OK_DONE;
  send_message->payload = "Done";
  send_message->length = strlen(send_message->payload);
}

void handle_print(DbOperator *query, message *send_message) {
  PrintOperator *print_op = &query->operator_fields.print_operator;
  cs165_log(stdout, "Executing print query:\nhandle_to_print: %s\n",
            print_op->handle_to_print);

  // Get the GeneralizedColumn from the handle
  Column *col = get_handle(print_op->handle_to_print);
  if (!col) {
    handle_error(send_message, "Invalid handle\n");
    log_err("L%d in handle_print: %s\n", __LINE__, send_message->payload);
    return;
  }
  if (col->data_type != INT && col->data_type != FLOAT) {
    handle_error(send_message, "Unsupported column data type");
    log_err("L%d in handle_print: %s\n", __LINE__, send_message->payload);
    return;
  }

  // Allocate an initial buffer for the result string
  size_t buffer_size = 1024;  // Initial size, will be increased if needed
  char *result_string = malloc(buffer_size);
  if (!result_string) {
    handle_error(send_message, "Failed to allocate memory for result string");
    log_err("L%d in handle_print: %s\n", __LINE__, send_message->payload);
    return;
  }

  size_t current_pos = 0;

  for (size_t i = 0; i < col->num_elements; i++) {
    // Ensure we have enough space in the buffer
    if (current_pos + 20 > buffer_size) {
      buffer_size *= 2;
      char *new_buffer = realloc(result_string, buffer_size);
      if (!new_buffer) {
        free(result_string);
        handle_error(send_message, "Failed to reallocate memory for result string");
        return;
      }
      result_string = new_buffer;
    }

    // Add the number to the string
    if (col->data_type == INT) {
      current_pos += sprintf(result_string + current_pos, "%d\n", ((int *)col->data)[i]);
    } else if (col->data_type == FLOAT) {
      current_pos +=
          sprintf(result_string + current_pos, "%f\n", ((double *)col->data)[i]);
    }
  }

  // Null-terminate the string
  result_string[current_pos] = '\0';

  // Store the result string in the status
  send_message->payload = result_string;
  send_message->length = strlen(send_message->payload);

  log_info("Print operation completed successfully.\n");
  return;
}
