// This file includes shared constants and other values.
#ifndef COMMON_H__
#define COMMON_H__

// define the socket path if not defined.
// note on windows we want this to be written to a docker container-only path
#ifndef SOCK_PATH
#define SOCK_PATH "cs165_unix_socket"
#endif

// Limits the size of a name in our database to 64 characters
#define MAX_SIZE_NAME 64
#define HANDLE_MAX_SIZE 64
#define MAX_PATH_LEN 512
#define NUM_ELEMENTS_TO_MULTITHREAD 10000
#define STORAGE_PATH "disk"

// CSV Transfer Constants
#define CSV_BUFFER_SIZE 1024
#define MAX_COLUMNS 100  // TODO: Make this dynamic in upcoming milestones
#define CSV_CHUNK_SIZE 4096
typedef struct {
  char column_name[256];  // of the form "db.table.column"
  //   int data_type;
  int chunk_size;
  size_t total_size;  // Total size of the column data (in bytes)
  char data[CSV_CHUNK_SIZE];
} CSVChunk;

typedef struct ColumnMetadata {
  char name[MAX_SIZE_NAME];
  size_t num_elements;
  long min_value;
  long max_value;
  long sum;
} ColumnMetadata;
/**
 * DataType
 * Flag to mark what type of data is held in the struct.
 * You can support additional types by including this enum and using void*
 * in place of int* in db_operator simliar to the way IndexType supports
 * additional types.
 **/
typedef enum DataType { INT, LONG, DOUBLE } DataType;

/**
 * Error codes used to indicate the outcome of an API call
 **/
typedef enum StatusCode {
  /* The operation completed successfully */
  OK,
  /* There was an error with the call. */
  ERROR,
} StatusCode;

// status declares an error code and associated message
typedef struct Status {
  StatusCode code;
  char *error_message;
} Status;

// mesage_status defines the status of the previous request.
// FEEL FREE TO ADD YOUR OWN OR REMOVE ANY THAT ARE UNUSED IN YOUR PROJECT
typedef enum message_status {
  INCOMING_QUERY,
  OK_DONE,
  OK_WAIT_FOR_RESPONSE,
  SERVER_SHUTDOWN,
  CSV_TRANSFER,
  UNKNOWN_COMMAND,
  QUERY_UNSUPPORTED,
  OBJECT_ALREADY_EXISTS,
  OBJECT_NOT_FOUND,
  INCORRECT_FORMAT,
  EXECUTION_ERROR,
  INDEX_ALREADY_EXISTS
} message_status;

// message is a single packet of information sent between client/server.
// message_status: defines the status of the message.
// length: defines the length of the string message to be sent.
// payload: defines the payload of the message.
typedef struct message {
  message_status status;
  int length;
  char *payload;
} message;

#endif  // COMMON_H__
