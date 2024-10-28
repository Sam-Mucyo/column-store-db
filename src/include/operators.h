#ifndef OPERATORS_H
#define OPERATORS_H

#include "client_context.h"
#include "common.h"

/*
 * tells the databaase what type of operator this is
 */
typedef enum OperatorType {
  CREATE,
  INSERT,
  LOAD,
  SELECT,
  FETCH,
  PRINT,
  AVG,
  MIN,
  MAX,
  SUM,
  ADD,
  SUB,
  SHUTDOWN,
} OperatorType;

// Defines a comparator flag between two values.
typedef enum ComparatorType {
  NO_COMPARISON = 0,
  LESS_THAN = 1,
  GREATER_THAN = 2,
  EQUAL = 4,
  LESS_THAN_OR_EQUAL = 5,
  GREATER_THAN_OR_EQUAL = 6
} ComparatorType;

typedef enum CreateType {
  _DB,
  _TABLE,
  _COLUMN,
} CreateType;

/*
 * necessary fields for creation
 * "create_type" indicates what kind of object you are creating.
 * For example, if create_type == _DB, the operator should create a db named <<name>>
 * if create_type = _TABLE, the operator should create a table named <<name>> with
 * <<col_count>> columns within db <<db>> if create_type = = _COLUMN, the operator
 * should create a column named
 * <<name>> within table <<table>>
 */
typedef struct CreateOperator {
  CreateType create_type;
  char name[MAX_SIZE_NAME];
  Db *db;
  Table *table;
  int col_count;
} CreateOperator;

/*
 * necessary fields for insertion
 */
typedef struct InsertOperator {
  Table *table;
  int *values;
} InsertOperator;
/*
 * necessary fields for insertion
 */
typedef struct LoadOperator {
  char *file_name;
} LoadOperator;

/**
 * comparator
 * A comparator defines a comparison operation over a column.
 **/
typedef struct Comparator {
  Column *col;      // the column to compare against.
  int *ref_posns;   // original positions of the values in the column.
  long int p_low;   // used in equality and ranges.
  long int p_high;  // used in range compares.
  ComparatorType type1;
  ComparatorType type2;
} Comparator;

typedef struct SelectOperator {
  char *res_handle;
  Comparator *comparator;
} SelectOperator;

typedef struct FetchOperator {
  char *fetch_handle;
  char *select_handle;
  Column *col;
} FetchOperator;

typedef struct AggregateOperator {
  Column *col;
  char *res_handle;
} AggregateOperator;

typedef struct ArithmeticOperator {
  Column *col1;
  Column *col2;
  char *res_handle;
} ArithmeticOperator;

typedef struct PrintOperator {
  char *handle_to_print;
} PrintOperator;

/*
 * union type holding the fields of any operator
 */
typedef union OperatorFields {
  CreateOperator create_operator;
  InsertOperator insert_operator;
  LoadOperator load_operator;
  SelectOperator select_operator;
  FetchOperator fetch_operator;
  PrintOperator print_operator;
  AggregateOperator aggregate_operator;
  ArithmeticOperator arithmetic_operator;
} OperatorFields;
/*
 * DbOperator holds the following fields:
 * type: the type of operator to perform (i.e. insert, select, ...)
 * operator fields: the fields of the operator in question
 * client_fd: the file descriptor of the client that this operator will return to
 * context: the context of the operator in question. This context holds the local
 * results of the client in question.
 */
typedef struct DbOperator {
  OperatorType type;
  OperatorFields operator_fields;
  int client_fd;
  ClientContext *context;
} DbOperator;

void db_operator_free(DbOperator *query);

#endif
