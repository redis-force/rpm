#ifndef __RPM_REDIS__
#define __RPM_REDIS__

#include <stdint.h>
#include <stdlib.h>
#include "rpm_allocator.h"
#include "rpm_chained_buffer.h"

typedef struct redis_response_reader redis_response_reader;
typedef struct redis_response redis_response;

typedef enum redis_response_type {
  REDIS_RESPONSE_STRING = 1,
  REDIS_RESPONSE_ARRAY = 2,
  REDIS_RESPONSE_INTEGER = 3,
  REDIS_RESPONSE_NIL = 4,
  REDIS_RESPONSE_STATUS = 5,
  REDIS_RESPONSE_ERROR = 6,
} redis_response_type;

typedef enum redis_response_status {
  REDIS_RESPONSE_OK = 0,
  REDIS_RESPONSE_ERROR_WOULDBLOCK = 1,
  REDIS_RESPONSE_ERROR_PROTOCOL = 2,
  REDIS_RESPONSE_ERROR_OOM = 3,
} redis_response_status;

typedef int64_t redis_integer_response;
typedef struct redis_string_response {
  char *str;
  size_t length;
} redis_string_response;
typedef struct redis_array_response {
  redis_response **array;
  size_t length;
} redis_array_response;

struct redis_response {
  redis_response_type type;
  uint64_t receive_time;
  void *upstream;
  allocator *allocator;
  union {
    redis_integer_response integer;
    redis_string_response string;
    redis_array_response array;
  } payload;
};

chained_buffer *redis_format_command(allocator *allocator, uint32_t argc, const char **argv, const size_t *argvlen);

redis_response_reader *redis_response_reader_create(allocator *allocator, int32_t maxbuf);
const char *redis_response_reader_strerr(redis_response_reader *reader);
redis_response_status redis_response_reader_feed(redis_response_reader *reader, const char *buf, int32_t size);
redis_response_status redis_response_reader_next(redis_response_reader *r, redis_response **reply);
void redis_response_reader_free_response(redis_response *response);
void redis_response_reader_free(redis_response_reader *reader);

#endif /* __RPM_REDIS__ */
