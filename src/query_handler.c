#include "query_handler.h"

#include <string.h>

#include "client_context.h"
#include "include/operators.h"
#include "query_exec.h"
#include "utils.h"

char *handle_print(DbOperator *query);

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
    case PRINT: {
      char *result = handle_print(query);
      if (!result) {
        handle_error(send_message, "Failed to print columns");
        return;
      }
      send_message->status = OK_DONE;
      send_message->length = strlen(result);
      send_message->payload = result;
    } break;
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

/**
 * @brief Executes a print operation by constructing a string representation
 * of the columns in row-major format
 *
 * @param query DbOperator containing the print operation
 * @return char* Allocated string containing the formatted output
 */
char *handle_print(DbOperator *query) {
  cs165_log(stdout, "handle_print: starting\n");
  PrintOperator *print_op = &query->operator_fields.print_operator;
  if (!print_op || !print_op->columns || print_op->num_columns == 0) {
    log_err("L%d: handle_print failed. No columns to print\n", __LINE__);
    return NULL;
  }

  //   cs165_log(stdout, "handle_print: num_columns: %zu\n", print_op->num_columns);
  //   cs165_log(stdout, "handle_print: print_op->columns[0].name: %s\n",
  //             print_op->columns[0]->name);
  // All columns should have the same number of elements
  size_t num_rows = print_op->columns[0]->num_elements;

  // Calculate required buffer size (estimate)
  // Assume max 20 chars per number plus separator and newline
  size_t buffer_size = (num_rows * print_op->num_columns * 21) + 1;
  char *result = malloc(buffer_size);
  if (!result) return NULL;

  char *current = result;
  size_t remaining = buffer_size;

  //   cs165_log(stdout, "handle_print: scanning columns\n");
  // Print each row
  for (size_t row = 0; row < num_rows; row++) {
    // Print each column's value in the current row
    for (size_t col = 0; col < print_op->num_columns; col++) {
      cs165_log(stdout, "handle_print: col: %zu, row: %zu\n", col, row);
      Column *column = print_op->columns[col];
      size_t printed;

      // Print value based on data type
      if (column->data_type == INT) {
        int *data = (int *)column->data;
        printed = snprintf(current, remaining, "%d", data[row]);
      } else if (column->data_type == LONG) {
        long *data = (long *)column->data;
        printed = snprintf(current, remaining, "%ld", data[row]);
      } else if (column->data_type == FLOAT) {
        float *data = (float *)column->data;
        printed = snprintf(current, remaining, "%f", data[row]);
      } else {
        log_err("handle_print: Unsupported data type\n");
        free(result);
        return NULL;
      }

      if (printed >= remaining) {
        free(result);
        return NULL;
      }

      current += printed;
      remaining -= printed;

      // Add separator (except for last column)
      if (col < print_op->num_columns - 1 && remaining > 1) {
        *current++ = ',';
        remaining--;
      }
    }

    // Add newline (except for last row)
    if (row < num_rows - 1 && remaining > 1) {
      *current++ = '\n';
      remaining--;
    }
  }
  *current = '\0';
  cs165_log(stdout, "handle_print: done\n");
  return result;
}
