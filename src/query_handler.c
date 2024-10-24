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

void handle_print(DbOperator *query, message *send_message) {
  PrintOperator *print_op = &query->operator_fields.print_operator;

  // Get the GeneralizedColumn from the handle
  GeneralizedColumn *gen_col = get_handle(print_op->handle_to_print);
  if (!gen_col) {
    send_message->status = EXECUTION_ERROR;
    send_message->payload = "Invalid handle for print operation";
    send_message->length = strlen(send_message->payload);
    return;
  }

  // Allocate an initial buffer for the result string
  size_t buffer_size = 1024;  // Initial size, will be increased if needed
  char *result_string = malloc(buffer_size);
  if (!result_string) {
    send_message->status = EXECUTION_ERROR;
    send_message->payload = "Memory allocation failed";
    send_message->length = strlen(send_message->payload);
    return;
  }
  size_t current_pos = 0;

  switch (gen_col->column_type) {
    case RESULT: {
      Result *result = gen_col->column_pointer.result;
      for (size_t i = 0; i < result->num_tuples; i++) {
        // Ensure we have enough space in the buffer
        if (current_pos + 20 >
            buffer_size) {  // NOTE:assume int is at most 20 characters long
          buffer_size *= 2;
          char *new_buffer = realloc(result_string, buffer_size);
          if (!new_buffer) {
            free(result_string);
            send_message->status = EXECUTION_ERROR;
            send_message->payload = "Memory reallocation failed";
            send_message->length = strlen(send_message->payload);
            return;
          }
          result_string = new_buffer;
        }

        if (result->data_type == INT) {
          // Add the number to the string
          int chars_written =
              snprintf(result_string + current_pos, buffer_size - current_pos, "%d\n",
                       ((int *)result->payload)[i]);
          current_pos += chars_written;
        } else if (result->data_type == FLOAT) {
          // Add the number to the string
          int chars_written =
              snprintf(result_string + current_pos, buffer_size - current_pos, "%f\n",
                       ((double *)result->payload)[i]);
          current_pos += chars_written;
        } else {
          free(result_string);
          send_message->status = EXECUTION_ERROR;
          send_message->payload = "Unknown data type for print operation";
          send_message->length = strlen(send_message->payload);
          return;
        }
      }
      break;
    }
    case COLUMN: {
      Column *column = gen_col->column_pointer.column;
      for (size_t i = 0; i < column->num_elements; i++) {
        // Ensure we have enough space in the buffer
        if (current_pos + 20 > buffer_size) {
          buffer_size *= 2;
          char *new_buffer = realloc(result_string, buffer_size);
          if (!new_buffer) {
            free(result_string);
            send_message->status = EXECUTION_ERROR;
            send_message->payload = "Memory reallocation failed";
            send_message->length = strlen(send_message->payload);
            return;
          }
          result_string = new_buffer;
        }

        // Add the number to the string
        int chars_written = snprintf(result_string + current_pos,
                                     buffer_size - current_pos, "%d\n", column->data[i]);
        current_pos += chars_written;
      }
      break;
    }
    default: {
      free(result_string);
      send_message->status = EXECUTION_ERROR;
      send_message->payload = "Unknown column type for print operation";
      send_message->length = strlen(send_message->payload);
      return;
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

void handle_avg(DbOperator *query, message *send_message) {
  cs165_log(stdout, "Executing avg query:\navg_handle: %s\nfetch_handle: %s\n",
            query->operator_fields.avg_operator.avg_handle,
            query->operator_fields.avg_operator.fetch_handle);
  Status status = {OK, NULL};
  AvgOperator *avg_op = &query->operator_fields.avg_operator;

  // Get the Column from the fetch handle
  GeneralizedColumn *fetch_gen_col = get_handle(avg_op->fetch_handle);
  if (!fetch_gen_col || fetch_gen_col->column_type != RESULT) {
    send_message->status = EXECUTION_ERROR;
    send_message->payload = "Invalid fetch handle or not a Result type";
    log_err("L%d in exec_avg: %s\n", __LINE__, send_message->payload);
    send_message->length = strlen(send_message->payload);
    return;
  }

  Result *fetch_result = fetch_gen_col->column_pointer.result;

  // Calculate the average
  double sum = 0.0;
  for (size_t i = 0; i < fetch_result->num_tuples; i++) {
    sum += ((int *)fetch_result->payload)[i];
  }

  double average = sum / fetch_result->num_tuples;

  // Create a new Result to store the average
  Result *avg_result = malloc(sizeof(Result));
  avg_result->data_type = FLOAT;
  if (!avg_result) {
    send_message->status = EXECUTION_ERROR;
    send_message->payload = "Memory allocation failed for avg_result";
    send_message->length = strlen(send_message->payload);
    return;
  }
  avg_result->num_tuples = 1;
  avg_result->payload = malloc(sizeof(double));
  if (!avg_result->payload) {
    free(avg_result);
    send_message->status = EXECUTION_ERROR;
    send_message->payload = "Memory allocation failed for avg_result payload";
    send_message->length = strlen(send_message->payload);
    return;
  }
  *((double *)avg_result->payload) = average;

  // Store the result in the client context
  if (g_client_context->variables_in_use >= MAX_VARIABLES) {
    free(avg_result->payload);
    free(avg_result);
    send_message->status = EXECUTION_ERROR;
    send_message->payload = "Maximum number of variables reached";
    send_message->length = strlen(send_message->payload);
    return;
  }

  GeneralizedColumnHandle *handle =
      &g_client_context->variable_pool[g_client_context->variables_in_use];
  snprintf(handle->name, HANDLE_MAX_SIZE, "%s", avg_op->avg_handle);
  handle->generalized_column.column_type = RESULT;
  handle->generalized_column.column_pointer.result = avg_result;

  g_client_context->variables_in_use++;

  // Add the new handle to the chandle_table
  status = add_handle(avg_op->avg_handle, &handle->generalized_column);
  if (status.code != OK) {
    free(avg_result->payload);
    free(avg_result);
    return;
  }

  log_info("Average operation completed successfully. Average: %f\n", average);
  return;
}
