#include <limits.h>

#include "client_context.h"
#include "query_exec.h"
#include "utils.h"

void exec_grace_hash_join(Column *psn1_col, Column *psn2_col, Column *vals1_col,
                          Column *vals2_col, Column *resL, Column *resR);
void exec_naive_hash_join(Column *psn1_col, Column *psn2_col, Column *vals1_col,
                          Column *vals2_col, Column *resL, Column *resR);
void exec_nested_loop_join(Column *psn1_col, Column *psn2_col, Column *vals1_col,
                           Column *vals2_col, Column *resL, Column *resR);
void exec_hash_join(Column *psn1_col, Column *psn2_col, Column *vals1_col,
                    Column *vals2_col, Column *resL, Column *resR);

void exec_join(DbOperator *query, message *send_message) {
  log_info("exec_join: executing join\n");
  JoinOperator join_op = query->operator_fields.join_operator;
  Column *psn1_col = join_op.posn1;
  Column *psn2_col = join_op.posn2;
  Column *vals1_col = join_op.vals1;
  Column *vals2_col = join_op.vals2;

  // Make column handles to store results
  Column *resL_col = NULL;
  Column *resR_col = NULL;

  if (create_new_handle(join_op.res_handle1, &resL_col) != 0 ||
      create_new_handle(join_op.res_handle2, &resR_col) != 0) {
    send_message->status = EXECUTION_ERROR;
    send_message->payload = "Failed to create result handles";
    send_message->length = strlen(send_message->payload);
    return;
  }

  resL_col->data_type = INT;
  resR_col->data_type = INT;
  resL_col->num_elements = 0;
  resR_col->num_elements = 0;

  switch (join_op.join_type) {
    case GRACE_HASH:
      exec_grace_hash_join(psn1_col, psn2_col, vals1_col, vals2_col, resL_col, resR_col);
      break;
    case NAIVE_HASH:
      exec_naive_hash_join(psn1_col, psn2_col, vals1_col, vals2_col, resL_col, resR_col);
      break;
    case NESTED_LOOP:
      exec_nested_loop_join(psn1_col, psn2_col, vals1_col, vals2_col, resL_col, resR_col);
      break;
    case HASH:
      exec_hash_join(psn1_col, psn2_col, vals1_col, vals2_col, resL_col, resR_col);
      break;
    default:
      send_message->status = EXECUTION_ERROR;
      send_message->payload = "Invalid join type";
      send_message->length = strlen(send_message->payload);
  }
}

void exec_nested_loop_join(Column *psn1_col, Column *psn2_col, Column *vals1_col,
                           Column *vals2_col, Column *resL, Column *resR) {
  log_debug("exec_nested_loop_join: executing nested loop join\n");
  size_t l_N = psn1_col->num_elements;
  size_t r_N = psn2_col->num_elements;

  int *l_psn = (int *)psn1_col->data;
  int *r_psn = (int *)psn2_col->data;

  int *l_vals = (int *)vals1_col->data;
  int *r_vals = (int *)vals2_col->data;

  size_t max_res_size = psn1_col->num_elements * psn2_col->num_elements;
  resL->data = malloc(sizeof(int) * max_res_size);
  resR->data = malloc(sizeof(int) * max_res_size);

  size_t k = 0;
  for (size_t i = 0; i < l_N; i++) {
    for (size_t j = 0; j < r_N; j++) {
      if (l_vals[i] == r_vals[j]) {
        ((int *)resL->data)[k] = l_psn[i];
        ((int *)resR->data)[k] = r_psn[j];
        k++;
      }
    }
  }
  resL->num_elements = k;
  resR->num_elements = k;
  log_info("exec_nested_loop_join: done\n");
}

void exec_grace_hash_join(Column *psn1_col, Column *psn2_col, Column *vals1_col,
                          Column *vals2_col, Column *resL, Column *resR) {
  log_debug("exec_grace_hash_join: Not implemented\n");
}

void exec_naive_hash_join(Column *psn1_col, Column *psn2_col, Column *vals1_col,
                          Column *vals2_col, Column *resL, Column *resR) {
  log_debug("exec_naive_hash_join: Not implemented\n");
}

void exec_hash_join(Column *psn1_col, Column *psn2_col, Column *vals1_col,
                    Column *vals2_col, Column *resL, Column *resR) {
  log_debug("exec_hash_join: Not implemented\n");
}
