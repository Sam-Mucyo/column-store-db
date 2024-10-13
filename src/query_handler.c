#include "query_handler.h"

#include "catalog_manager.h"
#include "utils.h"

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
      }
      break;
    default:
      cs165_log(stdout, "query was correctly parsed, but not handled by Executor.\n");
      break;
  }
  db_operator_free(query);
  return res_msg;
}
