/*
 * This file contains methods necessary to parse input from the client.
 * Mostly, functions in parse.c will take in string input and map these
 * strings into database operators. This will require checking that the
 * input from the client is in the correct format and maps to a valid
 * database operator.
 */

#define _DEFAULT_SOURCE
#include "parse.h"

#include <ctype.h>
#include <limits.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

#include "catalog_manager.h"
#include "client_context.h"
#include "utils.h"

// Function prototypes
DbOperator *parse_create(char *create_arguments);
DbOperator *parse_insert(char *insert_arguments, message *send_message);
DbOperator *parse_select(char *select_arguments, char *handle);
DbOperator *parse_fetch(char *fetch_arguments, char *handle);
DbOperator *parse_aggr(char *aggr_arguments, char *handle, OperatorType type);
DbOperator *parse_arithmetic(char *arithmetic_arguments, char *handle, OperatorType type);
DbOperator *parse_print(char *print_arguments);

/**
 * @brief parse_command
 * This method takes in a string representing the initial raw input from the client,
 * uses the first word to determine its category: create, insert, select, fetch, etc.
 * and then passes the arguments to the appropriate parsing function.
 *
 * @param query_command
 * @param send_message
 * @param client_socket
 * @param context
 * @return DbOperator* the database operator to be executed. NULL if the command is not
 * recognized.
 */
DbOperator *parse_command(char *query_command, message *send_message, int client_socket,
                          ClientContext *context) {
  // a second option is to malloc the dbo here (instead of inside the parse
  // commands). Either way, you should track the dbo and free it when the variable is
  // no longer needed.
  DbOperator *dbo = NULL;  // = malloc(sizeof(DbOperator));
  send_message->status = OK_WAIT_FOR_RESPONSE;

  if (strncmp(query_command, "--", 2) == 0) {
    send_message->status = OK_DONE;
    // The -- signifies a comment line, no operator needed.
    return NULL;
  }

  char *equals_pointer = strchr(query_command, '=');
  char *handle = query_command;
  if (equals_pointer != NULL) {
    // handle exists, store here.
    *equals_pointer = '\0';
    cs165_log(stdout, "FILE HANDLE: %s\n", handle);
    query_command = ++equals_pointer;
  } else {
    handle = NULL;
  }

  cs165_log(stdout, "QUERY: %s\n", query_command);

  // by default, set the status to acknowledge receipt of command,
  //   indication to client to now wait for the response from the server.
  //   Note, some commands might want to relay a different status back to the client.
  send_message->status = OK_WAIT_FOR_RESPONSE;
  query_command = trim_whitespace(query_command);
  // check what command is given.
  if (strncmp(query_command, "create", 6) == 0) {
    query_command += 6;
    // TODO: Make all `parse_` function take in `send_message` and set the status there.
    //  Currently, commands with just `INCORRECT_FORMAT` will be treated as
    //  `UNKNOWN_COMMAND`
    dbo = parse_create(query_command);
  } else if (strncmp(query_command, "relational_insert", 17) == 0) {
    query_command += 17;
    dbo = parse_insert(query_command, send_message);
  } else if (strncmp(query_command, "select", 6) == 0) {
    query_command += 6;
    dbo = parse_select(query_command, handle);
  } else if (strncmp(query_command, "fetch", 5) == 0) {
    query_command += 5;
    dbo = parse_fetch(query_command, handle);
  } else if (strncmp(query_command, "avg", 3) == 0) {
    query_command += 3;
    dbo = parse_aggr(query_command, handle, AVG);
  } else if (strncmp(query_command, "sum", 3) == 0) {
    query_command += 3;
    dbo = parse_aggr(query_command, handle, SUM);
  } else if (strncmp(query_command, "min", 3) == 0) {
    query_command += 3;
    dbo = parse_aggr(query_command, handle, MIN);
  } else if (strncmp(query_command, "max", 3) == 0) {
    query_command += 3;
    dbo = parse_aggr(query_command, handle, MAX);
  } else if (strncmp(query_command, "sub", 3) == 0) {
    query_command += 3;
    dbo = parse_arithmetic(query_command, handle, SUB);
  } else if (strncmp(query_command, "add", 3) == 0) {
    query_command += 3;
    dbo = parse_arithmetic(query_command, handle, ADD);
  } else if (strncmp(query_command, "print", 5) == 0) {
    query_command += 5;
    dbo = parse_print(query_command);
  } else {
    send_message->status = UNKNOWN_COMMAND;
  }
  if (dbo == NULL) return dbo;

  dbo->client_fd = client_socket;
  dbo->context = context;
  return dbo;
}

/**
 * Takes a pointer to a string.
 * This method returns the original string truncated to where its first comma lies.
 * In addition, the original string now points to the first character after that
 *comma. This method destroys its input.
 **/
char *next_token(char **tokenizer, message_status *status) {
  char *token = strsep(tokenizer, ",");
  if (token == NULL) {
    *status = INCORRECT_FORMAT;
  }
  return token;
}

/**
 * @brief parse_create_column
 * This method takes in a string representing the arguments to create a column, parses
 * them, and returns a DbOperator if the arguments are valid. Otherwise, it returns NULL.
 * Example original query:
 *      - create(col,"col1",db1.tbl1)
 *      - create(col,"col2",db1.tbl1)
 *
 * @param create_arguments the string representing the arguments to create a column
 * @return DbOperator*
 */
DbOperator *parse_create_column(char *create_arguments) {
  message_status status = OK_DONE;
  char **create_arguments_index = &create_arguments;
  char *column_name = next_token(create_arguments_index, &status);
  char *db_and_table_name = next_token(create_arguments_index, &status);

  // not enough arguments
  if (status == INCORRECT_FORMAT) {
    log_err("L%d: parse_create_column failed. Not enough arguments\n", __LINE__);
    return NULL;
  }

  // split db and table name
  char *db_name = strsep(&db_and_table_name, ".");
  char *table_name = db_and_table_name;

  // last character should be a ')', replace it with a null-terminating character
  int last_char = strlen(table_name) - 1;
  if (table_name[last_char] != ')') {
    log_err("L%d: parse_create_column failed. incorrect format\n", __LINE__);
    return NULL;
  }
  table_name[last_char] = '\0';
  // Get the column name free of quotation marks
  column_name = trim_quotes(column_name);
  // check that the database argument is the current active database
  if (!current_db || strcmp(current_db->name, db_name) != 0) {
    log_err("L%d: parse_create_column failed. Bad db name\n", __LINE__);
    return NULL;
  }
  // check that the table argument is in the current active database
  Table *table = get_table_from_catalog(table_name);
  if (!table) {
    log_err("L%d: parse_create_column failed. Bad table name\n", __LINE__);
    return NULL;
  }
  // make create dbo for column
  DbOperator *dbo = malloc(sizeof(DbOperator));
  dbo->type = CREATE;
  dbo->operator_fields.create_operator.create_type = _COLUMN;
  strcpy(dbo->operator_fields.create_operator.name, column_name);
  dbo->operator_fields.create_operator.db = current_db;
  dbo->operator_fields.create_operator.table = table;
  return dbo;
}

/**
 * @brief parse_create_tbl
 * This method takes in a string representing the arguments to create a table, parses
 * them, and returns a DbOperator if the arguments are valid. Otherwise, it returns NULL.
 * Example original query:
 *     - create(tbl,"tbl1",db1,3)
 *
 * @param create_arguments
 * @return DbOperator*
 */
DbOperator *parse_create_tbl(char *create_arguments) {
  message_status status = OK_DONE;
  char **create_arguments_index = &create_arguments;
  char *table_name = next_token(create_arguments_index, &status);
  char *db_name = next_token(create_arguments_index, &status);
  char *col_cnt = next_token(create_arguments_index, &status);

  // not enough arguments
  if (status == INCORRECT_FORMAT) {
    return NULL;
  }
  // Get the table name free of quotation marks
  table_name = trim_quotes(table_name);
  // read and chop off last char, which should be a ')'
  int last_char = strlen(col_cnt) - 1;
  if (col_cnt[last_char] != ')') {
    return NULL;
  }
  // replace the ')' with a null terminating character.
  col_cnt[last_char] = '\0';
  // check that the database argument is the current active database
  if (!current_db || strcmp(current_db->name, db_name) != 0) {
    cs165_log(stdout, "query unsupported. Bad db name");
    return NULL;  // QUERY_UNSUPPORTED
  }
  // turn the string column count into an integer, and check that the input is valid.
  int column_cnt = atoi(col_cnt);
  if (column_cnt < 1) {
    return NULL;
  }
  // make create dbo for table
  DbOperator *dbo = malloc(sizeof(DbOperator));
  dbo->type = CREATE;
  dbo->operator_fields.create_operator.create_type = _TABLE;
  strcpy(dbo->operator_fields.create_operator.name, table_name);
  dbo->operator_fields.create_operator.db = current_db;
  dbo->operator_fields.create_operator.col_count = column_cnt;
  return dbo;
}

/**
 * @brief parse_create_db
 * This method takes in a string representing the arguments to create a database, parses
 * them, and returns a DbOperator if the arguments are valid. Otherwise, it returns NULL.
 * Example original query:
 *     - create(db,"db1")
 *
 * @param create_arguments
 * @return DbOperator*
 */
DbOperator *parse_create_db(char *create_arguments) {
  char *token;
  token = strsep(&create_arguments, ",");
  // not enough arguments if token is NULL
  if (token == NULL) {
    return NULL;
  } else {
    // create the database with given name
    char *db_name = token;
    // trim quotes and check for finishing parenthesis.
    db_name = trim_quotes(db_name);
    int last_char = strlen(db_name) - 1;
    if (last_char < 0 || db_name[last_char] != ')') {
      return NULL;
    }
    // replace final ')' with null-termination character.
    db_name[last_char] = '\0';

    token = strsep(&create_arguments, ",");
    if (token != NULL) {
      return NULL;
    }
    // make create operator.
    DbOperator *dbo = malloc(sizeof(DbOperator));
    dbo->type = CREATE;
    dbo->operator_fields.create_operator.create_type = _DB;
    strcpy(dbo->operator_fields.create_operator.name, db_name);
    return dbo;
  }
}

/**
 * parse_create parses a create statement and then passes the necessary arguments off
 *to the next function
 **/
DbOperator *parse_create(char *create_arguments) {
  message_status mes_status = OK_DONE;
  DbOperator *dbo = NULL;
  char *tokenizer_copy, *to_free;
  // Since strsep destroys input, we create a copy of our input.
  tokenizer_copy = to_free = malloc((strlen(create_arguments) + 1) * sizeof(char));
  char *token;
  strcpy(tokenizer_copy, create_arguments);
  // check for leading parenthesis after create.
  if (strncmp(tokenizer_copy, "(", 1) == 0) {
    tokenizer_copy++;
    // token stores first argument. Tokenizer copy now points to just past first ","
    token = next_token(&tokenizer_copy, &mes_status);
    if (mes_status == INCORRECT_FORMAT) {
      return NULL;
    } else {
      // pass off to next parse function.
      if (strcmp(token, "db") == 0) {
        dbo = parse_create_db(tokenizer_copy);
      } else if (strcmp(token, "tbl") == 0) {
        dbo = parse_create_tbl(tokenizer_copy);
      } else if (strcmp(token, "col") == 0) {
        dbo = parse_create_column(tokenizer_copy);
      } else {
        mes_status = UNKNOWN_COMMAND;
      }
    }
  } else {
    mes_status = UNKNOWN_COMMAND;
  }
  free(to_free);
  return dbo;
}

/**
 * @brief parse_insert
 * Takes in a string representing the arguments to insert into a table, parses them, and
 * returns a DbOperator if the arguments are valid. Otherwise, it returns NULL.
 *
 * Example original query:
 *    - relational_insert(db1.tbl2,-1,-11,-111,-1111)  --- if db1.tbl2 has 4 columns
 *
 * @param query_command
 * @param send_message
 * @return DbOperator*
 */
DbOperator *parse_insert(char *query_command, message *send_message) {
  unsigned int columns_inserted = 0;
  char *token = NULL;
  // check for leading '('
  if (strncmp(query_command, "(", 1) == 0) {
    query_command++;
    char **command_index = &query_command;
    // parse table input
    char *db_tbl_name = next_token(command_index, &send_message->status);
    if (send_message->status == INCORRECT_FORMAT) {
      return NULL;
    }
    // split db and table name
    char *db_name = strsep(&db_tbl_name, ".");
    char *table_name = db_tbl_name;
    // check that the database argument is the current active database
    if (!current_db || strcmp(current_db->name, db_name) != 0) {
      cs165_log(stdout, "query unsupported. Bad db name\n");
      send_message->status = OBJECT_NOT_FOUND;
      return NULL;
    }

    // lookup the table and make sure it exists.
    Table *insert_table = get_table_from_catalog(table_name);
    if (insert_table == NULL) {
      send_message->status = OBJECT_NOT_FOUND;
      return NULL;
    }
    // make insert operator.
    DbOperator *dbo = malloc(sizeof(DbOperator));
    dbo->type = INSERT;
    dbo->operator_fields.insert_operator.table = insert_table;
    dbo->operator_fields.insert_operator.values =
        malloc(sizeof(int) * insert_table->num_cols);
    // parse inputs until we reach the end. Turn each given string into an integer.
    while ((token = strsep(command_index, ",")) != NULL) {
      int insert_val = atoi(token);
      dbo->operator_fields.insert_operator.values[columns_inserted] = insert_val;
      columns_inserted++;
    }
    // check that we received the correct number of input values
    if (columns_inserted != insert_table->num_cols) {
      log_err("L%d: parse_insert failed. This table has %d columns\n", __LINE__,
              insert_table->num_cols);
      send_message->status = INCORRECT_FORMAT;
      free(dbo->operator_fields.insert_operator.values);
      free(dbo);
      return NULL;
    }
    return dbo;
  } else {
    send_message->status = UNKNOWN_COMMAND;
    return NULL;
  }
}

/**
 * @brief parse_select
 * This method takes in a string representing the arguments to select from a table, parses
 * them, and returns a DbOperator if the arguments are valid. Otherwise, it returns NULL.
 *
 * Example query (without a handle):
 *     - select(db1.tbl1.col1,null,20)   --- select all values strictly less than 20
 *     - select(db1.tbl1.col1,20,40)     --- select all values between 20 (incl.) and 40
 *
 * @param query_command
 * @param handle  the handle to the result of this select query
 * @return DbOperator*
 */
DbOperator *parse_select(char *query_command, char *handle) {
  message_status status = OK_DONE;
  char **command_index = &query_command;

  char *db_tbl_col_name = next_token(command_index, &status);
  if (status == INCORRECT_FORMAT) return NULL;

  DbOperator *dbo = malloc(sizeof(DbOperator));
  dbo->type = SELECT;
  dbo->operator_fields.select_operator.comparator = malloc(sizeof(Comparator));

  char *low_str = next_token(command_index, &status);
  if (status == INCORRECT_FORMAT) {
    return NULL;
  }
  if (strcmp(low_str, "null") == 0) {
    dbo->operator_fields.select_operator.comparator->type1 = NO_COMPARISON;
  } else {
    dbo->operator_fields.select_operator.comparator->type1 = GREATER_THAN_OR_EQUAL;
    dbo->operator_fields.select_operator.comparator->p_low = atol(low_str);
    cs165_log(stdout, "parse_select: low_val: %ld\n",
              dbo->operator_fields.select_operator.comparator->p_low);
  }

  char *high_str = next_token(command_index, &status);
  printf("high_str: %s\n", high_str);
  if (status == INCORRECT_FORMAT) {
    return NULL;
  }
  if (strcmp(high_str, "null)") == 0) {
    dbo->operator_fields.select_operator.comparator->type2 = NO_COMPARISON;
  } else {
    dbo->operator_fields.select_operator.comparator->type2 = LESS_THAN;
    dbo->operator_fields.select_operator.comparator->p_high = atol(high_str);
    cs165_log(stdout, "parse_select: high_val: %ld\n",
              dbo->operator_fields.select_operator.comparator->p_high);
  }

  // Try getting column from catalog manager
  cs165_log(stdout, "parse_select: getting column %s from catalog\n", db_tbl_col_name);
  Column *col = get_column_from_catalog(db_tbl_col_name);
  if (!col) {
    db_operator_free(dbo);
    return NULL;
  }
  cs165_log(stdout, "parse_select: got column %s\n", col->name);
  dbo->operator_fields.select_operator.comparator->col = col;
  dbo->operator_fields.select_operator.res_handle = handle;

  return dbo;
}

/**
 * @brief parse_fetch
 * This method takes in a string representing the arguments to fetch from a column, parses
 * them, and returns a DbOperator if the arguments are valid. Otherwise, it returns NULL.
 *
 * Example query (without a handle):
 *     - fetch(db1.tbl1.col2,s1)           --- where s1 is a handle to the result of a
 *                                              select query
 * @param query_command
 * @param fetch_handle the handle to the result of this fetch query
 * @return DbOperator*
 */
DbOperator *parse_fetch(char *query_command, char *fetch_handle) {
  message_status status = OK_DONE;
  char **command_index = &query_command;

  char *db_tbl_col_name = next_token(command_index, &status);
  if (status == INCORRECT_FORMAT) return NULL;

  char *handle = next_token(command_index, &status);
  if (status == INCORRECT_FORMAT) return NULL;

  //   Check for the last ) in handle and replace it with a null-terminating character
  int last_char = strlen(handle) - 1;
  if (handle[last_char] != ')') {
    log_err("L%d: parse_fetch failed. incorrect format\n", __LINE__);
    return NULL;
  }
  handle[last_char] = '\0';

  DbOperator *dbo = malloc(sizeof(DbOperator));
  dbo->type = FETCH;
  dbo->operator_fields.fetch_operator.fetch_handle = fetch_handle;
  dbo->operator_fields.fetch_operator.select_handle = handle;

  // Try getting column from catalog manager
  Column *col = get_column_from_catalog(db_tbl_col_name);
  if (!col) {
    db_operator_free(dbo);
    return NULL;
  }
  cs165_log(stdout, "parse_fetch: getting column %s\nNew handle: %s\nSelect handle: %s\n",
            db_tbl_col_name, fetch_handle, handle);

  dbo->operator_fields.fetch_operator.col = col;
  log_info("Successfully parsed fetch command\n");
  return dbo;
}

/**
 * @brief parse_aggregate
 * Parses the aggregate command and returns a DbOperator if the arguments are valid.
 * Otherwise, it returns NULL.
 *
 * Example query (without a handle):
 *    - avg(f1)            --- where f1 is a handle to the result of a fetch query
 *    - sum(f1)            --- where f1 is a handle to the result of a fetch query
 *    - avg(db1.tbl1.col1) --- where db1.tbl1.col1 is a handle to the result of a fetch
 *
 * @param query_command  the command to parse (e.g., (f1), (db1.tbl1.col1))
 * @param res_handle the handle to the result of this aggregate query
 * @param type the type of aggregate operator (e.g., AVG, SUM)
 * @return DbOperator*
 */
DbOperator *parse_aggr(char *query_command, char *res_handle, OperatorType type) {
  log_info("L%d: parse_aggr: received: %s\n", __LINE__, query_command);

  // get the fetch handle "avg(f1)" -> "f1"
  const char *start = strchr(query_command, '(');
  if (start == NULL) {
    return NULL;
  }
  start++;
  char *col_handle = strndup(start, strchr(start, ')') - start);

  cs165_log(stdout, "res_handle: %s, col: %s\n", res_handle, col_handle);

  Column *col = NULL;
  // NOTE: based on project language, we can assume column names include dots
  if (strchr(col_handle, '.') != NULL) {
    col = get_column_from_catalog(col_handle);  // get the column from the catalog
  } else {
    col = get_handle(col_handle);  // from client context (variable pool)
  }
  if (!col) {
    log_err("L%d: parse_aggr failed. Bad column name\n", __LINE__);
    return NULL;
  }

  // Make DbOperator for avg
  DbOperator *dbo = malloc(sizeof(DbOperator));
  if (dbo == NULL) {
    log_err("L%d: parse_aggr: failed. malloc for DbOperator failed\n", __LINE__);
    return NULL;
  }

  dbo->type = type;
  dbo->operator_fields.aggregate_operator.res_handle = res_handle;
  dbo->operator_fields.aggregate_operator.col = col;

  log_info("Successfully parsed avg command\n");
  return dbo;
}

DbOperator *parse_arithmetic(char *query_command, char *handle, OperatorType type) {
  (void)query_command;
  (void)handle;
  (void)type;
  log_err("L%d: TODO: Implement parse_arithmetic\n", __LINE__);
  return NULL;
}

DbOperator *parse_print(char *query_command) {
  cs165_log(stdout, "L%d: parse_print received: %s\n", __LINE__, query_command);

  // get the fetch handle "print(f1)" -> "f1"
  const char *start = strchr(query_command, '(');
  if (start == NULL) {
    log_err("L%d: parse_print failed. incorrect format\n", __LINE__);
    return NULL;
  }
  start++;
  char *handle = strndup(start, strchr(start, ')') - start);

  cs165_log(stdout, "fetch_handle: %s\n", handle);

  DbOperator *dbo = malloc(sizeof(DbOperator));
  if (dbo == NULL) {
    log_err("L%d: parse_print failed. malloc for DbOperator failed\n", __LINE__);
    return NULL;
  }

  dbo->type = PRINT;
  dbo->operator_fields.print_operator.handle_to_print = handle;

  cs165_log(stdout, "handle_to_print: %s\n", handle);
  log_info("Successfully parsed print command\n");
  return dbo;
}
