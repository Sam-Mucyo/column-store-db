#include "query_handler.h"

#include <string.h>

#include "catalog_manager.h"
#include "client_context.h"
#include "operators.h"
#include "utils.h"

void exec_select(DbOperator *query, message *send_message);
void exec_fetch(DbOperator *query, message *send_message);
void exec_avg(DbOperator *query, message *send_message);
void exec_insert(DbOperator *query, message *send_message);
void exec_print(DbOperator *query, message *send_message);

/**
 * @brief execute_DbOperator
 * Executes a query and returns the result to be sent back to the client.
 *c
 * @param query (DbOperator*): the query to execute on the database
 * @return char*
 */
void execute_DbOperator(DbOperator *query, message *send_message) {
  switch (query->type) {
    case CREATE: {
      // TODO: Make `create_` functions take in `send_message` and set the status there.
      //   Just for consistency and cleaner code.
      char *res_msg = NULL;
      if (query->operator_fields.create_operator.create_type == _DB) {
        if (create_db(query->operator_fields.create_operator.name).code == OK) {
          res_msg = "-- Database created.";
        } else {
          res_msg = "Database creation failed.";
        }
      } else if (query->operator_fields.create_operator.create_type == _TABLE) {
        Status create_status;
        create_table(query->operator_fields.create_operator.db,
                     query->operator_fields.create_operator.name,
                     query->operator_fields.create_operator.col_count, &create_status);
        if (create_status.code != OK) {
          log_err("L%d: in execute_DbOperator: %s\n", __LINE__,
                  create_status.error_message);
          res_msg = "Table creation failed.";
        } else {
          res_msg = "-- Table created.";
        }
      } else if (query->operator_fields.create_operator.create_type == _COLUMN) {
        Status status;
        create_column(query->operator_fields.create_operator.table,
                      query->operator_fields.create_operator.name, false, &status);
        if (status.code == OK) {
          res_msg = "-- Column created.";
        } else {
          res_msg = "Column creation failed.";
        }
      }
      send_message->status = OK_DONE;
      send_message->length = strlen(res_msg);
      send_message->payload = res_msg;
    } break;

    case SELECT:
      exec_select(query, send_message);
      break;
    case FETCH:
      exec_fetch(query, send_message);
      break;
    case PRINT:
      exec_print(query, send_message);
      break;
    case AVG:
      exec_avg(query, send_message);
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
  int *result_indices = NULL;
  int result_count = 0;

  int selected_count = 0;

  cs165_log(stdout, "going through indices from 0 to %d\n", column->num_elements);
  // Perform the selection based on the comparator
  for (size_t i = 0; i < column->num_elements; i++) {
    int value = column->data[i];
    bool include = false;

    // cs165_log(stdout, "comparing value %d with low %ld and high %ld\n", value,
    //           comparator->p_low, comparator->p_high);
    // cs165_log(stdout, "Type1: %d, Type2: %d\n", comparator->type1,
    // comparator->type2);
    if (comparator->type1 == NO_COMPARISON && comparator->type2 != NO_COMPARISON &&
        compare(comparator->type2, comparator->p_high, value)) {
      include = true;
    }
    if (comparator->type2 == NO_COMPARISON && comparator->type1 != NO_COMPARISON &&
        compare(comparator->type1, comparator->p_low, value)) {
      include = true;
    }
    if (comparator->type1 == GREATER_THAN_OR_EQUAL && comparator->type2 == LESS_THAN &&
        compare(comparator->type1, comparator->p_low, value) &&
        compare(comparator->type2, comparator->p_high, value)) {
      include = true;
    }

    if (include) {
      cs165_log(stdout, "including value %d\n", value);
      selected_count++;
      result_indices = realloc(result_indices, (result_count + 1) * sizeof(int));
      result_indices[result_count++] = i;
    }
  }

  //   cs165_log(stdout, "Selected %d values. finished scanning\n", selected_count);

  // Create a new Result to store the selection result
  Result *result = malloc(sizeof(Result));
  result->num_tuples = result_count;
  result->data_type = INT;
  result->payload = result_indices;

  // print result
  //   for (int i = 0; i < result_count; i++) {
  //     printf("%d ", result_indices[i]);
  //   }
  //   printf("\n");

  cs165_log(stdout, "Number of handles in use: %d\n", g_client_context->variables_in_use);
  //   print numbers in handle table for debugging
  for (int i = 0; i < g_client_context->variables_in_use; i++) {
    cs165_log(stdout, "Handle %d: %s\n", i, g_client_context->variable_pool[i].name);
    GeneralizedColumn *gen_col = &g_client_context->variable_pool[i].generalized_column;

    if (gen_col->column_type == RESULT) {
      Result *res = gen_col->column_pointer.result;
      cs165_log(stdout, "Result %d has %d tuples\n", i, res->num_tuples);
      //   cs165_log(stdout, "Result %d has payload %p\n", i, res->payload);
      //   //   print payload
      //   for (int j = 0; j < res->num_tuples; j++) {
      //     printf("%d ", ((int *)res->payload)[j]);
      //   }
      //   printf("\n");
    }
  }
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

void exec_print(DbOperator *query, message *send_message) {
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

void exec_avg(DbOperator *query, message *send_message) {
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

}
