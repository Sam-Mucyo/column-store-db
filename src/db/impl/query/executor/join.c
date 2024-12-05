#include <limits.h>

#include "client_context.h"
#include "query_exec.h"
#include "utils.h"

void exec_join(DbOperator *query, message *send_message) {
  char *join_type_name = NULL;
  switch (query->operator_fields.join_operator.join_type) {
    case GRACE_HASH:
      join_type_name = "GRACE_HASH";
      break;
    case NAIVE_HASH:
      join_type_name = "NAIVE_HASH";
      break;
    case NESTED_LOOP:
      join_type_name = "NESTED_LOOP";
      break;
    case HASH:
      join_type_name = "HASH";
      break;
    default:
      break;
  }
  log_err(
      "exec_join: Not yet implemented; received psn1: %s, psn2: %s, vals1: %s, vals2: "
      "%s, join_type: %s\n",
      query->operator_fields.join_operator.posn1->name,
      query->operator_fields.join_operator.posn2->name,
      query->operator_fields.join_operator.vals1->name,
      query->operator_fields.join_operator.vals2->name, join_type_name);
}
