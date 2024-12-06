#define _GNU_SOURCE
#include "utils.h"

#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#define ANSI_COLOR_RED "\x1b[31m"
#define ANSI_COLOR_GREEN "\x1b[32m"
#define ANSI_COLOR_RESET "\x1b[0m"

// #define LOG 1

#define LOG_ERR 1
// #define LOG_DEBUG 1
// #define LOG_INFO 1
// #define LOG_PERF 1
#define LOG_SESSION_PERF 1

// Get current time in microseconds
double get_time() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return tv.tv_sec * 1e6 + tv.tv_usec;
}

/* removes newline characters from the input string.
 * Shifts characters over and shortens the length of
 * the string by the number of newline characters.
 */
char *trim_newline(char *str) {
  int length = strlen(str);
  int current = 0;
  for (int i = 0; i < length; ++i) {
    if (!(str[i] == '\r' || str[i] == '\n')) {
      str[current++] = str[i];
    }
  }

  // Write new null terminator
  str[current] = '\0';
  return str;
}
/* removes space characters from the input string.
 * Shifts characters over and shortens the length of
 * the string by the number of space characters.
 */
char *trim_whitespace(char *str) {
  int length = strlen(str);
  int current = 0;
  for (int i = 0; i < length; ++i) {
    if (!isspace(str[i])) {
      str[current++] = str[i];
    }
  }

  // Write new null terminator
  str[current] = '\0';
  return str;
}

/* removes parenthesis characters from the input string.
 * Shifts characters over and shortens the length of
 * the string by the number of parenthesis characters. */
char *trim_parenthesis(char *str) {
  int length = strlen(str);
  int current = 0;
  for (int i = 0; i < length; ++i) {
    if (!(str[i] == '(' || str[i] == ')')) {
      str[current++] = str[i];
    }
  }

  // Write new null terminator
  str[current] = '\0';
  return str;
}

char *trim_quotes(char *str) {
  int length = strlen(str);
  int current = 0;
  for (int i = 0; i < length; ++i) {
    if (str[i] != '\"') {
      str[current++] = str[i];
    }
  }

  // Write new null terminator
  str[current] = '\0';
  return str;
}
/* The following three functions will show output on the terminal
 * based off whether the corresponding level is defined.
 * To see log output, define LOG.
 * To see error output, define LOG_ERR.
 * To see info output, define LOG_INFO
 */
void cs165_log(FILE *out, const char *format, ...) {
#ifdef LOG
  va_list v;
  va_start(v, format);
  vfprintf(out, format, v);
  va_end(v);
#else
  (void)out;
  (void)format;
#endif
}

void log_perf(const char *format, ...) {
#ifdef LOG_PERF
  va_list v;
  va_start(v, format);
  vfprintf(stdout, format, v);
  va_end(v);
#else
  (void)format;
#endif
}

void log_debug(const char *format, ...) {
#ifdef LOG_DEBUG
  va_list v;
  va_start(v, format);
  fprintf(stderr, ANSI_COLOR_RED);
  vfprintf(stderr, format, v);
  fprintf(stderr, ANSI_COLOR_RESET);
  va_end(v);
#else
  (void)format;
#endif
}
void log_err(const char *format, ...) {
#ifdef LOG_ERR
  va_list v;
  va_start(v, format);
  fprintf(stderr, ANSI_COLOR_RED);
  vfprintf(stderr, format, v);
  fprintf(stderr, ANSI_COLOR_RESET);
  va_end(v);
#else
  (void)format;
#endif
}

void log_info(const char *format, ...) {
#ifdef LOG_INFO
  va_list v;
  va_start(v, format);
  fprintf(stdout, ANSI_COLOR_GREEN);
  vfprintf(stdout, format, v);
  fprintf(stdout, ANSI_COLOR_RESET);
  fflush(stdout);
  va_end(v);
#else
  (void)format;
#endif
}

void log_client_perf(FILE *out, const char *format, ...) {
#ifdef LOG_SESSION_PERF
  va_list v;
  va_start(v, format);
  vfprintf(out, format, v);
  va_end(v);
#else
  (void)out;
  (void)format;
#endif
}

/**
 * @brief handles error by updating the message to be sent back to the client as
 * appropriate. Sets the message status to EXECUTION_ERROR and the payload to the
 * error message. This won't log on the server-side, since the error_message might be
 * different for debugging purposes.
 *
 * @param send_message
 * @param error_message
 */
void handle_error(message *send_message, char *error_message) {
  if (send_message == NULL) {
    return;
  }
  send_message->status = EXECUTION_ERROR;
  send_message->payload = error_message;
  send_message->length = strlen(send_message->payload);
}

/**
 * @brief Extends memory-mapped region if necessary and updates values
 *
 * This function will extend the mapped memory region if needed to accommodate
 * new values, but does not immediately sync changes to disk. The actual file
 * extension and disk synchronization should be handled separately.
 *
 * @param mapped_addr Pointer to the current memory-mapped region
 * @param current_size Pointer to current size of mapped region in bytes
 * @param offset Starting position (in number of integers) where new values will be
 * written
 * @param new_values Array of integer values to write
 * @param count Number of integers to write
 *
 * @return Pointer to (possibly new) mapped region on success, NULL on failure
 *         If successful, *current_size will be updated if extension occurred
 *
 * @note If extension occurs, the old mapped_addr may become invalid and the
 *       new returned pointer should be used instead
 */
int *extend_and_update_mmap(int *mapped_addr, size_t *current_size, size_t offset,
                            const int *new_values, size_t count) {
  if (mapped_addr == NULL || current_size == NULL || new_values == NULL || count == 0) {
    errno = EINVAL;
    return NULL;
  }

  // Calculate required size
  size_t required_size = (offset + count) * sizeof(int);

  // Check if we need to extend
  if (required_size > *current_size) {
    // Round up to nearest page size for efficiency
    size_t page_size = sysconf(_SC_PAGESIZE);
    cs165_log(stdout, "extend_and_update_mmap: page size %zu\n", page_size);
    size_t new_size = (required_size + page_size - 1) & ~(page_size - 1);

    // Remap with new size
#ifdef __linux__
    void *new_addr = mremap(mapped_addr, *current_size, new_size, MREMAP_MAYMOVE);
    if (new_addr == MAP_FAILED) {
      return NULL;
    }
#elif __APPLE__
    // macOS does not manually mmap a new region, so we need to create a new mapping
    // and copy the old values over.
    log_err(
        "extend_and_update_mmap: macOS does not support mremap; support not implemented "
        "yet\n");
    return NULL;
#endif
  }

  // Copy new values to the specified offset
  memcpy(mapped_addr + offset, new_values, count * sizeof(int));

  return mapped_addr;
}

// message send function, with error handling
ssize_t send_message_safe(int socket, const void *buffer, size_t length) {
  size_t total_sent = 0;
  const char *buf = (const char *)buffer;

  while (total_sent < length) {
    size_t remaining = length - total_sent;
    size_t chunk_size = remaining;

    ssize_t sent = send(socket, buf + total_sent, chunk_size, 0);
    if (sent < 0) {
      if (errno == EINTR) continue;  // Interrupted, try again
      return -1;                     // Error
    }
    total_sent += sent;
  }
  return total_sent;
}

// Improved message receive function
ssize_t recv_message_safe(int socket, void *buffer, size_t length) {
  size_t total_received = 0;
  char *buf = (char *)buffer;

  while (total_received < length) {
    size_t remaining = length - total_received;
    size_t chunk_size = remaining;

    ssize_t received = recv(socket, buf + total_received, chunk_size, 0);
    if (received < 0) {
      if (errno == EINTR) continue;  // Interrupted, try again
      return -1;                     // Error
    }
    if (received == 0) {
      log_err("recv_message_safe: connection closed\n");
      return total_received;  // Connection closed
    }
    total_received += received;
  }
  return total_received;
}
