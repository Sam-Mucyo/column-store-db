#include "query_handler.h"

#include <string.h>

#include "client_context.h"
#include "include/operators.h"
#include "query_exec.h"
#include "utils.h"

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
    case MIN:
    case MAX:
    case SUM:
      exec_aggr(query, send_message);
      break;
    case ADD:
    case SUB:
      exec_arithmetic(query, send_message);
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
          sprintf(result_string + current_pos, "%g\n", ((double *)col->data)[i]);
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
