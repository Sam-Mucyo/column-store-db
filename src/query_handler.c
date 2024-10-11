#include "query_handler.h"

#include "utils.h"

/**
 * @brief execute_DbOperator
 * Executes a query and returns the result to be sent back to the client.
 *
 * @param query (DbOperator*): the query to execute on the database
 * @return char*
 */
char *execute_DbOperator(DbOperator *query) {
  char *res_msg;
  if (!query) {
    cs165_log(stdout, "query executor received a null query.\n");
    return "Unsurpported query.";
  }
  switch (query->type) {
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
          cs165_log(stdout, "adding a table failed.\n");
          res_msg = "Table creation failed.";
        }
        res_msg = "Table created.";
      }
      break;
    default:
      cs165_log(stdout, "query was correctly parsed, but not handled by Executor.\n");
      break;
  }
  res_msg = "Unsurpported query.";
  db_operator_free(query);
  return res_msg;
}
