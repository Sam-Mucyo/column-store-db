/* This line at the top is necessary for compilation on the lab machine and many other
Unix machines. Please look up _XOPEN_SOURCE for more details. As well, if your code does
not compile on the lab machine please look into this as a a source of error. */
#define _XOPEN_SOURCE

/**
 * client.c
 *  CS165 Fall 2018
 *
 * This file provides a basic unix socket implementation for a client
 * used in an interactive client-server database.
 * The client receives input from stdin and sends it to the server.
 * No pre-processing is done on the client-side.
 *
 * For more information on unix sockets, refer to:
 * http://beej.us/guide/bgipc/output/html/multipage/unixsock.html
 **/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/un.h>
#include <unistd.h>

#include "common.h"
#include "utils.h"

#define DEFAULT_STDIN_BUFFER_SIZE 1024

int connect_client(void);
int send_csv_file(int client_socket, const char *filename);

/**
 * Getting Started Hint:
 *      What kind of protocol or structure will you use to deliver your results from the
 *server to the client? What kind of protocol or structure will you use to interpret
 *results for final display to the user?
 *
 **/
int main(void) {
  int client_socket = connect_client();
  if (client_socket < 0) {
    exit(1);
  }

  message send_message;
  message recv_message;

  // Always output an interactive marker at the start of each command if the
  // input is from stdin. Do not output if piped in from file or from other fd
  char *prefix = "";
  if (isatty(fileno(stdin))) {
    prefix = "db_client > ";
  }

  char *output_str = NULL;
  int len = 0;

  // Continuously loop and wait for input. At each iteration:
  // 1. output interactive marker
  // 2. read from stdin until eof.
  char read_buffer[DEFAULT_STDIN_BUFFER_SIZE];
  send_message.payload = read_buffer;
  send_message.status = 0;

  while (printf("%s", prefix),
         output_str = fgets(read_buffer, DEFAULT_STDIN_BUFFER_SIZE, stdin),
         !feof(stdin)) {
    if (output_str == NULL) {
      log_err("fgets failed.\n");
      break;
    }

    // Only process input that is greater than 1 character.
    // Convert to message and send the message and the
    // payload directly to the server.
    send_message.length = strlen(read_buffer);
    if (send_message.length > 1) {
      // Check if the input is a load command
      if (strncmp(read_buffer, "load(", 5) == 0) {
        char filename[MAX_PATH_LEN];
        sscanf(read_buffer, "load(\"%[^\"]\")", filename);

        // Send a special message to indicate CSV transfer
        send_message.status = CSV_TRANSFER_START;
        // cs165_log(stdout, "sending csv transfer start message\n");
        if (send(client_socket, &send_message, sizeof(message), 0) == -1) {
          log_err("Failed to send CSV transfer start message");
          exit(1);
        }

        // cs165_log(stdout, "sending csv file\n");
        if (send_csv_file(client_socket, filename) == -1) {
          log_err("Failed to send CSV file");
          exit(1);
        } else {
          log_info("Client: CSV file sent successfully\n");
        }
      } else {
        // cs165_log(stdout, "sending message metadata\n");
        // Send the message_header, which tells server payload size
        if (send(client_socket, &(send_message), sizeof(message), 0) == -1) {
          log_err("Failed to send message header.");
          exit(1);
        }

        // cs165_log(stdout, "sending message payload:%s\n", send_message.payload);
        // Send the payload (query) to server
        if (send(client_socket, send_message.payload, send_message.length, 0) == -1) {
          log_err("Failed to send query payload.");
          exit(1);
        }
      }

      // Always wait for server response (even if it is just an OK message)
      if ((len = recv(client_socket, &(recv_message), sizeof(message), 0)) > 0) {
        if ((recv_message.status == OK_WAIT_FOR_RESPONSE ||
             recv_message.status == OK_DONE) &&
            (int)recv_message.length > 0) {
          // Calculate number of bytes in response package
          int num_bytes = (int)recv_message.length;
          char payload[num_bytes + 1];

          // Receive the payload and print it out
          if ((len = recv(client_socket, payload, num_bytes, 0)) > 0) {
            payload[num_bytes] = '\0';
            if (strncmp(payload, "OK", 2) != 0) {
              printf("\n%s\n", payload);
            }
          }
        }
      } else {
        if (len < 0) {
          log_err("Failed to receive message.");
        } else {
          log_info("-- Server closed connection\n");
        }
        exit(1);
      }
    }
  }
  close(client_socket);
  return 0;
}
/**
 * connect_client()
 *
 * This sets up the connection on the client side using unix sockets.
 * Returns a valid client socket fd on success, else -1 on failure.
 *
 **/
int connect_client(void) {
  int client_socket;
  size_t len;
  struct sockaddr_un remote;

  log_info("-- Attempting to connect...\n");

  if ((client_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
    log_err("L%d: Failed to create socket.\n", __LINE__);
    return -1;
  }

  remote.sun_family = AF_UNIX;
  strncpy(remote.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
  len = strlen(remote.sun_path) + sizeof(remote.sun_family) + 1;
  if (connect(client_socket, (struct sockaddr *)&remote, len) == -1) {
    log_err("client connect fclailed: ");
    return -1;
  }

  log_info("-- Client connected at socket: %d.\n", client_socket);
  return client_socket;
}

char **extract_csv_columns(const char *header, int *num_columns) {
  char **columns = NULL;
  char *token;
  char header_copy[MAX_SIZE_NAME * MAX_COLUMNS];
  int count = 0;

  // Create a copy of the header string since strtok modifies the string
  strncpy(header_copy, header, sizeof(header_copy));
  header_copy[sizeof(header_copy) - 1] = '\0';  // Ensure null-termination

  // Allocate memory for the array of column names
  columns = (char **)malloc(MAX_COLUMNS * sizeof(char *));
  if (columns == NULL) {
    fprintf(stderr, "Memory allocation failed\n");
    return NULL;
  }

  // Get the first token
  token = strtok(header_copy, ",");

  // Walk through other tokens
  while (token != NULL && count < MAX_COLUMNS) {
    // Allocate memory for this column name
    columns[count] = (char *)malloc(MAX_SIZE_NAME * sizeof(char));
    if (columns[count] == NULL) {
      fprintf(stderr, "Memory allocation failed\n");
      // Free previously allocated memory
      for (int i = 0; i < count; i++) {
        free(columns[i]);
      }
      free(columns);
      return NULL;
    }

    // Copy the token to the column name
    strncpy(columns[count], token, MAX_SIZE_NAME);
    columns[count][MAX_SIZE_NAME - 1] = '\0';  // Ensure null-termination

    count++;
    token = strtok(NULL, ",");
  }

  *num_columns = count;
  return columns;
}

/**
 * @brief send_csv_file
 * Sends a CSV file to the server in chunks (currently assumes int data type)
 *
 * @param client_socket
 * @param filename
 * @return int
 */
int send_csv_file(int client_socket, const char *filename) {
  // Open CSV file
  FILE *csv_file = fopen(filename, "r");
  if (!csv_file) {
    log_err("Error opening CSV file");
  }

  //   cs165_log(stdout, "opened csv file\n");
  char *line = NULL;
  size_t len = 0;
  ssize_t read;

  // Array to store column names
  char *columns[MAX_COLUMNS] = {0};
  int column_count = 0;

  // Count the number of lines (excluding header)
  size_t line_count = 0;
  while ((read = getline(&line, &len, csv_file)) != -1) {
    line_count++;
  }
  line_count--;  // Subtract header line

  //   cs165_log(stdout, "num of ints per colum: %zu\n", line_count);

  // Reset file pointer to beginning
  fseek(csv_file, 0, SEEK_SET);

  // Read header to determine the number of columns
  if ((read = getline(&line, &len, csv_file)) != -1) {
    char *token;
    trim_whitespace(line);
    // cs165_log(stdout, "header: %s\n", line);
    // populate columns array
    char **columns = extract_csv_columns(line, &column_count);
    // cs165_log(stdout, "%d columns found\n", column_count);

    // Process CSV file column by column
    for (int i = 0; i < column_count; i++) {
      CSVChunk chunk;
      memset(&chunk, 0, sizeof(CSVChunk));
      strncpy(chunk.column_name, columns[i], sizeof(chunk.column_name) - 1);
      chunk.column_name[sizeof(chunk.column_name) - 1] = '\0';  // Ensure null-termination
      chunk.total_size =
          line_count * sizeof(int);  // Each column will contain 'line_count' integers

      //   cs165_log(stdout, "sending column: %s\n", chunk.column_name);
      // Send initial chunk with column information
      if (send(client_socket, &chunk, sizeof(CSVChunk), 0) == -1) {
        log_err("Error sending initial chunk");
      }

      // Reset chunk size for the new column
      chunk.chunk_size = 0;

      // Reset file pointer and skip the header
      fseek(csv_file, 0, SEEK_SET);
      getline(&line, &len, csv_file);  // Skip the header line

      // Read and send data for the current column
      while ((read = getline(&line, &len, csv_file)) != -1) {
        token = strtok(line, ",");
        for (int j = 0; j < i; j++) {
          token = strtok(NULL, ",");  // Move to the current column value
        }

        if (token != NULL) {
          int value = atoi(token);  // Convert the token to an integer

          // Check if the current chunk is full
          if (chunk.chunk_size + sizeof(int) > CSV_CHUNK_SIZE) {
            // Send the current chunk
            if (send(client_socket, &chunk, sizeof(CSVChunk), 0) == -1) {
              log_err("Error sending chunk");
            }
            // Reset chunk size for the next set of data
            chunk.chunk_size = 0;
          }

          // Copy the value into the chunk's data array
          memcpy(chunk.data + chunk.chunk_size, &value, sizeof(int));
          chunk.chunk_size += sizeof(int);
        }
      }

      // Send any remaining data in the last chunk
      if (chunk.chunk_size > 0) {
        if (send(client_socket, &chunk, sizeof(CSVChunk), 0) == -1) {
          log_err("Error sending last chunk");
        }
      }
    }

    // Send end of transmission signal
    CSVChunk end_chunk;
    memset(&end_chunk, 0, sizeof(CSVChunk));
    strcpy(end_chunk.column_name, "END_TRANSMISSION");
    if (send(client_socket, &end_chunk, sizeof(CSVChunk), 0) == -1) {
      log_err("Error sending END_TRANSMISSION signal");
    }
  }

  // Clean up
  fclose(csv_file);
  if (line) {
    free(line);
  }

  // Free column memory
  for (int i = 0; i < column_count; i++) {
    if (columns[i]) {
      free(columns[i]);
    }
  }
  printf("Client finished sending data\n");

  return 0;
}
