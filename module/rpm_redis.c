#include "rpm_redis.h"
#include "rpm_chained_buffer.h"
#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <assert.h>

#define SIZE_KB (1024)
#define SIZE_4KB (4 * SIZE_KB)
#define SIZE_16KB (16 * SIZE_KB)

static inline uint32_t redis_integer_digits(uint64_t integer) {
  uint32_t len = 1;
  while (1) {
    if (integer < 10) return len;
    if (integer < 100) return len + 1;
    if (integer < 1000) return len + 2;
    if (integer < 10000) return len + 3;
    integer /= 10000U;
    len += 4;
  }
}

static inline uint32_t redis_bulk_length(uint32_t len) {
  return 1 + redis_integer_digits(len) + 2 + len + 2;
}

chained_buffer *redis_format_command(allocator *allocator, uint32_t argc, const char **argv, const size_t *argvlen) {
  uint32_t wr, total, idx, len;
  char *realbuffer;
  chained_buffer *buffer;

  total = 1 + redis_integer_digits(argc) + 2;
  for (idx = 0; idx < argc; idx++) {
    total += redis_bulk_length(argvlen[idx]);
  }

  if ((buffer = chained_buffer_create(allocator, total + 1)) == NULL) {
    return NULL;
  }
  realbuffer = (char *) buffer->buffer;

  wr = sprintf(realbuffer, "*%d\r\n", argc);
  for (idx = 0; idx < argc; idx++) {
    len = argvlen[idx];
    wr += sprintf(realbuffer + wr, "$%u\r\n", len);
    memcpy(realbuffer + wr, argv[idx], len);
    wr += len;
    realbuffer[wr++] = '\r';
    realbuffer[wr++] = '\n';
  }
  realbuffer[wr] = '\0';
  buffer->size = wr;

  return buffer;
}

typedef struct redis_response_reader_task {
  int32_t type;
  int32_t elements;
  int32_t idx;
  void *obj;
  struct redis_response_reader_task *parent;
} redis_response_reader_task;

struct redis_response_reader {
  redis_response_status err;
  char errstr[128];

  allocator *allocator;
  char *buf;
  size_t capacity;
  size_t pos;
  size_t len;
  size_t max_buf;

  redis_response_reader_task tasks[9];
  int32_t task_idx;
  int32_t max_task_idx;

  redis_response *response;
};

#define REDIS_RESPONSE_READER_CHECK_ERROR(READER) \
  do {                                            \
    redis_response_status st = (READER)->err;     \
    if (st != REDIS_RESPONSE_OK) {                \
      return st;                                  \
    }                                             \
  } while (0)

static inline void redis_response_reader_task_init(redis_response_reader_task *parent, redis_response_reader_task *task, int32_t idx) {
  task->type = -1;
  task->elements = -1;
  task->idx = idx;
  task->obj = NULL;
  task->parent = parent;
}

static redis_response *redis_response_reader_create_response(const redis_response_reader *reader, int type) {
  redis_response *resp;
  if ((resp = allocator_calloc(reader->allocator, sizeof(*resp))) == NULL) {
    return NULL;
  }

  resp->type = type;
  resp->allocator = reader->allocator;
  return resp;
}

void redis_response_reader_free_response(redis_response *resp) {
  size_t idx;
  if (resp == NULL) {
    return;
  }

  switch(resp->type) {
  case REDIS_RESPONSE_INTEGER:
    break;
  case REDIS_RESPONSE_ARRAY:
    if (resp->payload.array.array != NULL) {
      for (idx = 0; idx < resp->payload.array.length; idx++) {
        if (resp->payload.array.array[idx] != NULL) {
          redis_response_reader_free_response(resp->payload.array.array[idx]);
        }
      }
      allocator_free(resp->allocator, resp->payload.array.array);
    }
    break;
  case REDIS_RESPONSE_ERROR:
  case REDIS_RESPONSE_STATUS:
  case REDIS_RESPONSE_STRING:
    if (resp->payload.string.str != NULL) {
      allocator_free(resp->allocator, resp->payload.string.str);
    }
    break;
  default:
    break;
  }
  allocator_free(resp->allocator, resp);
}

static void *redis_response_reader_create_string(const redis_response_reader *reader, const redis_response_reader_task *task, char *str, size_t len) {
  redis_response *resp, *parent;
  char *buf;
  if ((resp = redis_response_reader_create_response(reader, task->type)) == NULL) {
    return NULL;
  }
  if ((buf = allocator_malloc(reader->allocator, len + 1)) == NULL) {
    redis_response_reader_free_response(resp);
    return NULL;
  }

  assert(task->type == REDIS_RESPONSE_ERROR  ||
         task->type == REDIS_RESPONSE_STATUS ||
         task->type == REDIS_RESPONSE_STRING);

  memcpy(buf, str, len);
  buf[len] = '\0';
  resp->payload.string.str = buf;
  resp->payload.string.length = len;

  if (task->parent) {
    parent = task->parent->obj;
    assert(parent->type == REDIS_RESPONSE_ARRAY);
    parent->payload.array.array[task->idx] = resp;
  }
  return resp;
}

static void *redis_response_reader_create_array(const redis_response_reader *reader, const redis_response_reader_task *task, int elements) {
  redis_response *resp, *parent;

  if ((resp = redis_response_reader_create_response(reader, REDIS_RESPONSE_ARRAY)) == NULL) {
    return NULL;
  }

  if (elements > 0) {
    if ((resp->payload.array.array = allocator_calloc(reader->allocator, elements * sizeof(redis_response *))) == NULL) {
      redis_response_reader_free_response(resp);
      return NULL;
    }
  }

  resp->payload.array.length = elements;
  if (task->parent) {
    parent = task->parent->obj;
    assert(parent->type == REDIS_RESPONSE_ARRAY);
    parent->payload.array.array[task->idx] = resp;
  }
  return resp;
}

static void *redis_response_reader_create_integer(const redis_response_reader *reader, const redis_response_reader_task *task, long long value) {
  redis_response *resp, *parent;
  if ((resp = redis_response_reader_create_response(reader, REDIS_RESPONSE_INTEGER)) == NULL) {
    return NULL;
  }

  resp->payload.integer = value;
  if (task->parent) {
    parent = task->parent->obj;
    assert(parent->type == REDIS_RESPONSE_ARRAY);
    parent->payload.array.array[task->idx] = resp;
  }
  return resp;
}

static void *redis_response_reader_create_nil(const redis_response_reader *reader, const redis_response_reader_task *task) {
  redis_response *resp, *parent;

  if ((resp = redis_response_reader_create_response(reader, REDIS_RESPONSE_NIL)) == NULL) {
    return NULL;
  }

  if (task->parent) {
    parent = task->parent->obj;
    assert(parent->type == REDIS_RESPONSE_ARRAY);
    parent->payload.array.array[task->idx] = resp;
  }
  return resp;
}

const char *redis_response_reader_strerr(redis_response_reader *reader) {
  return reader->errstr;
}

redis_response_reader *redis_response_reader_create(allocator *allocator, int32_t max_buf) {
  redis_response_reader *reader;

  if ((reader = allocator_calloc(allocator, sizeof(redis_response_reader))) == NULL) {
    return NULL;
  }

  reader->allocator = allocator;
  reader->err = 0;
  reader->errstr[0] = '\0';
  reader->buf = NULL;
  reader->capacity = 0;
  reader->max_buf = max_buf < SIZE_16KB ? SIZE_16KB : max_buf;
  reader->task_idx = -1;
  reader->max_task_idx = (sizeof(reader->tasks) / sizeof(reader->tasks[0])) - 1;
  if (reader->max_task_idx >= 15) {
    reader->max_task_idx = 15;
  }
  return reader;
}

static void redis_response_reader_set_error(redis_response_reader *reader, int32_t type, const char *str) {
  size_t len;

  if (reader->response != NULL) {
    redis_response_reader_free_response(reader->response);
    reader->response = NULL;
  }

  if (reader->buf != NULL) {
    allocator_free(reader->allocator, reader->buf);
    reader->buf = NULL;
    reader->capacity = 0;
    reader->pos = reader->len = 0;
  }

  reader->task_idx = -1;

  reader->err = type;
  len = strlen(str);
  len = len < (sizeof(reader->errstr) - 1) ? len : (sizeof(reader->errstr) - 1);
  memcpy(reader->errstr, str, len);
  reader->errstr[len] = '\0';
}

static size_t redis_response_reader_set_error_protocol_str(char *buf, size_t size, char byte) {
  size_t len = 0;

  switch(byte) {
  case '\\':
  case '"':
    len = snprintf(buf, size, "\"\\%c\"", byte);
    break;
  case '\n': len = snprintf(buf, size, "\"\\n\""); break;
  case '\r': len = snprintf(buf, size, "\"\\r\""); break;
  case '\t': len = snprintf(buf, size, "\"\\t\""); break;
  case '\a': len = snprintf(buf, size, "\"\\a\""); break;
  case '\b': len = snprintf(buf, size, "\"\\b\""); break;
  default:
    if (isprint(byte)) {
      len = snprintf(buf, size,"\"%c\"", byte);
    } else {
      len = snprintf(buf, size, "\"\\x%02x\"", (unsigned char) byte);
    }
    break;
  }

  return len;
}

static inline void redis_response_reader_set_error_protocol_byte(redis_response_reader *reader, char byte) {
  char cbuf[8], sbuf[128];

  redis_response_reader_set_error_protocol_str(cbuf, sizeof(cbuf), byte);
  snprintf(sbuf, sizeof(sbuf), "Protocol error, got %s as response type byte", cbuf);
  redis_response_reader_set_error(reader, REDIS_RESPONSE_ERROR_PROTOCOL, sbuf);
}

static inline void redis_response_reader_set_error_oom(redis_response_reader *reader) {
  redis_response_reader_set_error(reader, REDIS_RESPONSE_ERROR_OOM, "Out of memory");
}

redis_response_status redis_response_reader_feed(redis_response_reader *reader, const char *buf, int32_t size) {
  REDIS_RESPONSE_READER_CHECK_ERROR(reader);

  if (buf != NULL && size >= 1) {
    if (reader->len == 0 && reader->capacity > reader->max_buf) {
      reader->capacity = SIZE_4KB < size ? size * 2 : SIZE_4KB;
      reader->pos = 0;
      if ((reader->buf = allocator_realloc(reader->allocator, reader->buf, reader->capacity)) == NULL) {
        redis_response_reader_set_error_oom(reader);
        return reader->err;
      }
    }

    if (reader->capacity - reader->len < (uint32_t) size) {
      reader->capacity = 2 * (reader->capacity + size);
      if ((reader->buf = allocator_realloc(reader->allocator, reader->buf, reader->capacity)) == NULL) {
        redis_response_reader_set_error_oom(reader);
        return reader->err;
      }
    }
    memcpy(reader->buf + reader->len, buf, size);
    reader->len += size;
  }

  return REDIS_RESPONSE_OK;
}

static char *redis_response_reader_read_bytes(redis_response_reader *reader, uint32_t bytes) {
  char *p;
  if (reader->len - reader->pos >= bytes) {
    p = reader->buf + reader->pos;
    reader->pos += bytes;
    return p;
  }
  return NULL;
}

static char *redis_response_reader_seek_crlf(char *buffer, size_t len) {
  int32_t rd = 0;
  int32_t scanlen = len - 1;

  while (rd < scanlen) {
    while (rd < scanlen && buffer[rd] != '\r') rd++;
    if (rd == scanlen) {
      return NULL;
    } else {
      if (buffer[rd + 1] == '\n') {
        return buffer + rd;
      } else {
        rd++;
      }
    }
  }
  return NULL;
}

static int64_t redis_response_reader_read_integer(char *buffer) {
  int64_t val = 0;
  int32_t digit, sign = 1;
  char ch;

  switch (*buffer) {
    case '-':
      sign = -1;
      /* FALL THROUGH */
    case '+':
      buffer++;
      /* FALL THROUGH */
    default:
      break;
  }

  while ((ch = *(buffer++)) != '\r') {
    digit = ch - '0';
    if (digit >= 0 && digit < 10) {
      val *= 10;
      val += digit;
    } else {
      return -1;
    }
  }

  return sign * val;
}

static char *redis_response_reader_read_line(redis_response_reader *reader, int32_t *len) {
  char *p, *s;
  int32_t real_len;

  p = reader->buf + reader->pos;
  s = redis_response_reader_seek_crlf(p, (reader->len - reader->pos));
  if (s != NULL) {
    real_len = s - (reader->buf + reader->pos);
    reader->pos += real_len + 2;
    if (len) {
      *len = real_len;
    }
    return p;
  }
  return NULL;
}

static void redis_response_reader_next_task(redis_response_reader *reader) {
  redis_response_reader_task *cur, *prv;
  while (reader->task_idx >= 0) {
    if (reader->task_idx == 0) {
      reader->task_idx--;
      return;
    }

    cur = reader->tasks + reader->task_idx;
    prv = cur - 1;
    assert(prv->type == REDIS_RESPONSE_ARRAY);
    if (cur->idx == prv->elements - 1) {
      reader->task_idx--;
    } else {
      assert(cur->idx < prv->elements);
      cur->type = -1;
      cur->elements = -1;
      cur->idx++;
      return;
    }
  }
}

static int32_t redis_response_reader_read_line_item(redis_response_reader *reader) {
  redis_response_reader_task *cur = reader->tasks + reader->task_idx;
  void *obj;
  char *p;
  int32_t len;

  if ((p = redis_response_reader_read_line(reader, &len)) != NULL) {
    if (cur->type == REDIS_RESPONSE_INTEGER) {
      obj = redis_response_reader_create_integer(reader, cur, redis_response_reader_read_integer(p));
    } else {
      obj = redis_response_reader_create_string(reader, cur, p, len);
    }
    if (obj == NULL) {
      redis_response_reader_set_error_oom(reader);
      return reader->err;
    }
    if (reader->task_idx == 0) {
      reader->response = obj;
    }
    redis_response_reader_next_task(reader);
    return REDIS_RESPONSE_OK;
  }

  return REDIS_RESPONSE_ERROR_WOULDBLOCK;
}

static int32_t redis_response_reader_read_bulk_item(redis_response_reader *reader) {
  redis_response_reader_task *cur = reader->tasks + reader->task_idx;
  void *obj = NULL;
  char *p, *s;
  long len;
  unsigned long bytelen;
  int32_t success = 0;

  p = reader->buf + reader->pos;
  s = redis_response_reader_seek_crlf(p, reader->len - reader->pos);
  if (s != NULL) {
    p = reader->buf + reader->pos;
    bytelen = s - (reader->buf + reader->pos) + 2;
    len = redis_response_reader_read_integer(p);

    if (len < 0) {
      obj = redis_response_reader_create_nil(reader, cur);
      success = 1;
    } else {
      bytelen += len + 2;
      if (reader->pos + bytelen <= reader->len) {
        obj = redis_response_reader_create_string(reader, cur, s + 2, len);
        success = 1;
      }
    }
    if (success) {
      if (obj == NULL) {
        redis_response_reader_set_error_oom(reader);
        return reader->err;
      }
      reader->pos += bytelen;
      if (reader->task_idx == 0) {
        reader->response = obj;
      }
      redis_response_reader_next_task(reader);
      return REDIS_RESPONSE_OK;
    }
  }

  return REDIS_RESPONSE_ERROR_WOULDBLOCK;
}

static int32_t redis_response_reader_read_multi_bulk_item(redis_response_reader *reader) {
  redis_response_reader_task *cur = reader->tasks + reader->task_idx;
  void *obj;
  char *p;
  long elements;
  int32_t root = 0;
  char error[64];

  if (reader->task_idx == reader->max_task_idx) {
    snprintf(error, sizeof(error), "Invalid nested multi bulk replies with depth greater than %d", reader->max_task_idx);
    redis_response_reader_set_error(reader, REDIS_RESPONSE_ERROR_PROTOCOL, error);
    return reader->err;
  }

  if ((p = redis_response_reader_read_line(reader, NULL)) != NULL) {
    elements = redis_response_reader_read_integer(p);
    root = (reader->task_idx == 0);

    if (elements == -1) {
      if ((obj = redis_response_reader_create_nil(reader, cur)) == NULL) {
        redis_response_reader_set_error_oom(reader);
        return reader->err;
      }
      redis_response_reader_next_task(reader);
    } else {
      if ((obj = redis_response_reader_create_array(reader, cur, elements)) == NULL) {
        redis_response_reader_set_error_oom(reader);
        return reader->err;
      }
      if (elements > 0) {
        cur->elements = elements;
        cur->obj = obj;
        redis_response_reader_task_init(cur, reader->tasks + (++reader->task_idx), 0);
      } else {
        redis_response_reader_next_task(reader);
      }
    }

    if (root) {
      reader->response = obj;
    }
    return REDIS_RESPONSE_OK;
  }

  return REDIS_RESPONSE_ERROR_WOULDBLOCK;
}


static int32_t redis_response_process_item(redis_response_reader *reader) {
  redis_response_reader_task *cur = reader->tasks + reader->task_idx;
  char *p;

  if (cur->type < 0) {
    if ((p = redis_response_reader_read_bytes(reader, 1)) != NULL) {
      switch (*p) {
      case '-':
        cur->type = REDIS_RESPONSE_ERROR;
        break;
      case '+':
        cur->type = REDIS_RESPONSE_STATUS;
        break;
      case ':':
        cur->type = REDIS_RESPONSE_INTEGER;
        break;
      case '$':
        cur->type = REDIS_RESPONSE_STRING;
        break;
      case '*':
        cur->type = REDIS_RESPONSE_ARRAY;
        break;
      default:
        redis_response_reader_set_error_protocol_byte(reader, *p);
        return reader->err;
      }
    } else {
      return REDIS_RESPONSE_ERROR_WOULDBLOCK;
    }
  }

  /* process typed item */
  switch(cur->type) {
  case REDIS_RESPONSE_ERROR:
  case REDIS_RESPONSE_STATUS:
  case REDIS_RESPONSE_INTEGER:
    return redis_response_reader_read_line_item(reader);
  case REDIS_RESPONSE_STRING:
    return redis_response_reader_read_bulk_item(reader);
  case REDIS_RESPONSE_ARRAY:
    return redis_response_reader_read_multi_bulk_item(reader);
  default:
    assert(0);
    redis_response_reader_set_error(reader, REDIS_RESPONSE_ERROR_PROTOCOL, "Invalid response type");
    return REDIS_RESPONSE_ERROR_PROTOCOL;
  }
}

redis_response_status redis_response_reader_next(redis_response_reader *reader, redis_response **response) {
  *response = NULL;
  REDIS_RESPONSE_READER_CHECK_ERROR(reader);
  if (reader->len == 0) {
    return REDIS_RESPONSE_OK;
  }

  if (reader->task_idx == -1) {
    redis_response_reader_task_init(NULL, reader->tasks, -1);
    reader->task_idx = 0;
  }

  while (reader->task_idx >= 0) {
    if (redis_response_process_item(reader) != REDIS_RESPONSE_OK) {
      break;
    }
  }

  REDIS_RESPONSE_READER_CHECK_ERROR(reader);

  if (reader->pos >= 1024) {
    memmove(reader->buf, reader->buf + reader->pos, reader->len - reader->pos);
    reader->len -= reader->pos;
    reader->pos = 0;
  }

  if (reader->task_idx == -1) {
    *response = reader->response;
    reader->response = NULL;
  }
  return REDIS_RESPONSE_OK;
}

void redis_response_reader_free(redis_response_reader *reader) {
  if (reader->response != NULL) {
    redis_response_reader_free_response(reader->response);
  }
  if (reader->buf != NULL) {
    allocator_free(reader->allocator, reader->buf);
  }
  allocator_free(reader->allocator, reader);
}
