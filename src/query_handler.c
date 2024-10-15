#include "query_handler.h"

#include "catalog_manager.h"
#include "client_context.h"
#include "utils.h"

Status exec_select(DbOperator *query);

/**
 * @brief execute_DbOperator
 * Executes a query and returns the result to be sent back to the client.
 *
 * @param query (DbOperator*): the query to execute on the database
 * @return char*
 */
char *execute_DbOperator(DbOperator *query) {
  char *res_msg = "Unsurpported query.";
  if (!query) {
    cs165_log(stdout, "query executor received a null query.\n");
    return "Unsurpported query.";
  }
  switch (query->type) {
    case SHUTDOWN:
      db_shutdown();
      res_msg = "Db shutdown.";
      break;
    case CREATE:
      if (query->operator_fields.create_operator.create_type == _DB) {
        if (create_db(query->operator_fields.create_operator.name).code == OK) {
          res_msg = "Database created.";
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
          res_msg = "Table created.";
        }
      } else if (query->operator_fields.create_operator.create_type == _COLUMN) {
        Status status;
        create_column(query->operator_fields.create_operator.table,
                      query->operator_fields.create_operator.name, false, &status);
        if (status.code == OK) {
          res_msg = "Column created.";
        } else {
          res_msg = "Column creation failed.";
        }
      }
      break;

    case SELECT: {
      cs165_log(stdout, "Executing select query.\n");
      Status select_status = exec_select(query);
      if (select_status.code == OK) {
        res_msg = "Select operation completed successfully.";
      } else {
        res_msg = select_status.error_message;
      }
    } break;

    case FETCH:
      res_msg = "Fetch operation not supported yet.";
      break;

    case PRINT:
      res_msg = "Print operation not supported yet.";
      break;
    case AVG:
      res_msg = "Avg operation not supported yet.";
      break;

    default:
      cs165_log(stdout, "query was correctly parsed, but not handled by Executor.\n");
      break;
  }
  db_operator_free(query);
  return res_msg;
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

Status exec_select(DbOperator *query) {
  Status status = {OK, NULL};
  SelectOperator *select_op = &query->operator_fields.select_operator;
  Comparator *comparator = select_op->comparator;
  GeneralizedColumn *gen_col = comparator->gen_col;

  if (gen_col->column_type != COLUMN) {
    status.code = ERROR;
    status.error_message = "Select operation is only supported on columns";
    return status;
  }

  Column *column = gen_col->column_pointer.column;
  int *result_indices = NULL;
  int result_count = 0;

  int selected_count = 0;

  cs165_log(stdout, "going through indices from 0 to %d\n", column->num_elements);
  // Perform the selection based on the comparator
  for (int i = 0; i < column->num_elements; i++) {
    int value = column->data[i];
    bool include = false;

    switch (comparator->type1) {
      case LESS_THAN:
        include = (value < comparator->p_low);
        break;
      case GREATER_THAN:
        include = (value > comparator->p_low);
        break;
      case EQUAL:
        include = (value == comparator->p_low);
        break;
      case LESS_THAN_OR_EQUAL:
        include = (value <= comparator->p_low);
        break;
      case GREATER_THAN_OR_EQUAL:
        include = (value >= comparator->p_low);
        break;
      default:
        // For range queries
        include = (value >= comparator->p_low && value <= comparator->p_high);
        break;
    }

    if (include) {
      cs165_log(stdout, "including value %d\n", value);
      selected_count++;
      result_indices = realloc(result_indices, (result_count + 1) * sizeof(int));
      result_indices[result_count++] = i;
    }
  }

  cs165_log(stdout, "Selected %d values. finished scanning\n", selected_count);

  // Create a new Result to store the selection result
  Result *result = malloc(sizeof(Result));
  result->num_tuples = result_count;
  result->payload = result_indices;

  // print result
  for (int i = 0; i < result_count; i++) {
    printf("%d ", result_indices[i]);
  }
  printf("\n");

  cs165_log(stdout, "Number of handles in use: %d\n", g_client_context->variables_in_use);
  //   print numbers in handle table for debugging
  for (int i = 0; i < g_client_context->variables_in_use; i++) {
    cs165_log(stdout, "Handle %d: %s\n", i, g_client_context->variable_pool[i].name);
    GeneralizedColumn *gen_col = &g_client_context->variable_pool[i].generalized_column;

    if (gen_col->column_type == RESULT) {
      Result *res = gen_col->column_pointer.result;
      cs165_log(stdout, "Result %d has %d tuples\n", i, res->num_tuples);
      cs165_log(stdout, "Result %d has payload %p\n", i, res->payload);
      //   print payload
      for (int j = 0; j < res->num_tuples; j++) {
        printf("%d ", ((int *)res->payload)[j]);
      }
      printf("\n");
    }
  }
  if (g_client_context->variables_in_use >= MAX_VARIABLES) {
    free(result->payload);
    free(result);
    status.code = ERROR;
    status.error_message = "Maximum number of variables reached";
    return status;
  }

  GeneralizedColumnHandle *handle =
      &g_client_context->variable_pool[g_client_context->variables_in_use];
  snprintf(handle->name, HANDLE_MAX_SIZE, "%s", comparator->handle);
  handle->generalized_column.column_type = RESULT;
  handle->generalized_column.column_pointer.result = result;

  g_client_context->variables_in_use++;

  // If a handle was provided in the query, update the chandle_table
  if (comparator->handle != NULL) {
    status = add_handle(comparator->handle, &handle->generalized_column);
    if (status.code != OK) {
      return status;
    }
  }
  //   Add logging info
  log_info("Select operation completed successfully.\n");
  return status;
}

Status exec_fetch(DbOperator *query) {
  log_err("TODO: Implement fetch operation.\n");
  return (Status){.code = ERROR, .error_message = "Fetch operation not supported yet."};
}

Status exec_print(DbOperator *query) {
  log_err("TODO: Implement print operation.\n");
  return (Status){.code = ERROR, .error_message = "Print operation not supported yet."};
}

Status exec_avg(DbOperator *query) {
  log_err("TODO: Implement avg operation.\n");
  return (Status){.code = ERROR, .error_message = "Avg operation not supported yet."};
}