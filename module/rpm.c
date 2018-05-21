#define REDISMODULE_EXPERIMENTAL_API 1
#include "redismodule.h"
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <stdint.h>
#include <errno.h>
#include <strings.h>
#include <assert.h>
#include <string.h>
#include "rpm_allocator.h"
#include "rpm_hashmap.h"
#include "rpm_chained_buffer.h"
#include "rpm_redis.h"
#include "rpm_queue.h"
#include <sys/types.h>
#include <signal.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/time.h>

#ifndef __LP64__
error "Only LP64 architectures are supported"
#endif

#define MAX_LOG_LEN (1000) /* leave some room for worker process id in log */
#define MAX_REAL_LOG_LEN (1024)

#define RPM_DEFAULT_DOWNSTREAM (4)
#define RPM_MAX_DOWNSTREAM (16)

#define RPM_DEFAULT_UPSTREAM (1)
#define RPM_MAX_UPSTREAM (16)

#define ENV_RPM_DOWNSTREAM_FD_PREFIX "RPM_DOWNSTREAM_FD"
#define ENV_RPM_DOWNSTREAM_FD_NUM "RPM_DOWNSTREAM_FD_NUM"
#define ENV_RPM_UPSTREAM_FD_PREFIX "RPM_UPSTREAM_FD"
#define ENV_RPM_UPSTREAM_FD_NUM "RPM_UPSTREAM_FD_NUM"
#define ENV_RPM_WORKER_GENERATION "RPM_WORKER_GENERATION"
#define ENV_RPM_DOWNSTREAM_ADDRESS "RPM_DOWNSTREAM_ADDRESS"

#define WORKER_SHUTDOWN_EVENT_TYPE_SUICIDE 'S'
#define WORKER_SHUTDOWN_EVENT_TYPE_TERMINATE 'T'

#define PROFILE_ELAPSED_THRESHOLD (10000)

const char *RPM_BAD_GATEWAY = "ERR Bad Gateway";
const char *RPM_REQUEST_TIMEOUT = "ERR Request Timeout";
const int64_t RPM_DEFAULT_TIMEOUT = 1000;
void *WORKER_PIPE_SHUTDOWN = "SHUTDOWN";

typedef struct rpm_context rpm_context;
typedef struct worker_process worker_process;
typedef struct worker_pipe worker_pipe;
typedef struct worker_upstream worker_upstream;
typedef struct rpm_context_config_descriptor rpm_context_config_descriptor;

static void rpm_worker_destroy(RedisModuleCtx *ctx, rpm_context *rpm, worker_process *worker);
static void rpm_worker_free(worker_process *worker);
static void rpm_worker_release(worker_process *worker);
static worker_process *rpm_worker_create(RedisModuleCtx *ctx, rpm_context *rpm);
static void rpm_context_shutdown_redis(RedisModuleCtx *ctx, rpm_context *rpm);

static void *rpm_allocator_malloc(void *privdata, size_t size) {
  REDISMODULE_NOT_USED(privdata);
  return RedisModule_Alloc(size);
}

static void *rpm_allocator_calloc(void *privdata, size_t size) {
  REDISMODULE_NOT_USED(privdata);
  return RedisModule_Calloc(1, size);
}

static void *rpm_allocator_realloc(void *privdata, void *ptr, size_t size) {
  REDISMODULE_NOT_USED(privdata);
  return RedisModule_Realloc(ptr, size);
}

static void rpm_allocator_free(void *privdata, void *ptr) {
  REDISMODULE_NOT_USED(privdata);
  RedisModule_Free(ptr);
}

static void rpm_allocator_destroy(void *privdata) {
  REDISMODULE_NOT_USED(privdata);
}

struct rpm_context {
  char **argv;
  int64_t current_request_id;
  worker_process *worker;
  hash_map *command_timeout;
  allocator *allocator;
  int32_t num_downstreams;
  int32_t num_upstreams;
  int32_t restart_count;
  int32_t retry_timeout;
  int32_t retry_attempts;
  int32_t debug_mode;
  int32_t generation;
  time_t restart_count_reset_time;
  char *shutdown_command;
  char *downstream_address;
  long long restart_timer;
};

typedef void (*rpm_context_config_parser)(void *, RedisModuleString **);

struct rpm_context_config_descriptor {
  const char *name;
  int32_t argc;
  uint32_t offset;
  rpm_context_config_parser parser;
};

static void rpm_context_int32_config_parser(void *, RedisModuleString **);
static void rpm_context_string_config_parser(void *, RedisModuleString **);
static void rpm_context_bool_config_parser(void *, RedisModuleString **);

static const rpm_context_config_descriptor rpm_config_descriptors[] = {
  {"--num-downstreams", 1, offsetof(rpm_context, num_downstreams), rpm_context_int32_config_parser},
  {"--num-upstreams", 1, offsetof(rpm_context, num_upstreams), rpm_context_int32_config_parser},
  {"--retry-timeout", 1, offsetof(rpm_context, retry_timeout), rpm_context_int32_config_parser},
  {"--retry-attempts", 1, offsetof(rpm_context, retry_attempts), rpm_context_int32_config_parser},
  {"--shutdown-command", 1, offsetof(rpm_context, shutdown_command), rpm_context_string_config_parser},
  {"--debug", 0, offsetof(rpm_context, debug_mode), rpm_context_bool_config_parser},
  {"--downstream-address", 1, offsetof(rpm_context, downstream_address), rpm_context_string_config_parser},
  {NULL, 0, 0, NULL},
};

struct rpm_command_profile_data {
  uint64_t command_start_time;
  uint32_t worker_start_offset;
  uint32_t worker_process_offset;
  uint32_t worker_serialize_offset;
  uint32_t worker_send_offset;
  uint32_t serialized_size;
  uint32_t padding;
};

static inline unsigned long long rpm_current_us(void);

static uint32_t int_key_hash_algorithm(hash_map *map, const void *data, size_t size) {
  REDISMODULE_NOT_USED(map);
  REDISMODULE_NOT_USED(size);
  return (uint32_t)(((uint64_t) (uintptr_t) data) & 0xFFFFFF);
}

#define hash_map_int_ptr(val) ((void *) (uintptr_t)(val))
static hash_map_type int_key_map = {
  int_key_hash_algorithm, NULL, NULL, NULL, NULL, NULL, NULL,
};

static hash_map_type command_timeout_map = {
  NULL, hash_map_allocator_dup_data, NULL, hash_map_bitwise_compare_data, hash_map_allocator_destroy_data, NULL, NULL,
};

struct worker_upstream {
  worker_process *worker;
  int pipe[2];
  queue *command_queue;
  pthread_t writer;
  pthread_t reader;
  char buf[16*1024];
};

struct worker_pipe {
  worker_process *worker;
  chained_buffer *read_buffer;
  chained_buffer *write_buffer;
  chained_buffer **next_buffer;
  int32_t num_buffers;
  int pipe[2];
};

struct worker_process {
  rpm_context *rpm;
  worker_pipe out;
  worker_pipe err;
  int32_t num_downstreams;
  int *downstreams;
  int32_t num_upstreams;
  worker_upstream *upstreams;
  hash_map *request_id_to_client;
  hash_map *client_id_to_request_id;
  pthread_mutex_t request_id_to_client_mutex;
  pthread_mutex_t client_id_to_request_id_mutex;
  pthread_mutex_t refcount_mutex;
  int32_t active_upstreams;
  char log_buf[1024];
  int shutdown_pipe[2];
  int32_t refcount;
};

static void rpm_log_profile_data(RedisModuleCtx *ctx, uint64_t receive_time, worker_pipe *upstream,
    redis_response *client_id, redis_response *request_id, redis_response *profile, const char *state) {
  struct rpm_command_profile_data *real_profile = (struct rpm_command_profile_data *) profile->payload.string.str;
  unsigned long long receive_offset, reply_offset;
  if (profile->payload.string.length < sizeof(struct rpm_command_profile_data)) {
    RedisModule_Log(ctx, "warning", "invalid profile data for client %lld command %lld");
  } else {
    receive_offset = receive_time - real_profile->command_start_time;
    reply_offset = rpm_current_us() - receive_time;
    if (real_profile->worker_start_offset > PROFILE_ELAPSED_THRESHOLD ||
        receive_offset - real_profile->worker_send_offset > PROFILE_ELAPSED_THRESHOLD ||
        reply_offset > PROFILE_ELAPSED_THRESHOLD) {
      RedisModule_Log(ctx, "warning", "execute client %lld command %lld received at %lld %s "
          "with %u bytes payload from upstream %d@%p: 0/%u/%u/%u/%u/%llu/%llu",
          client_id->payload.integer, request_id->payload.integer, (long long) real_profile->command_start_time, state,
          real_profile->serialized_size, upstream->pipe[0], upstream,
          real_profile->worker_start_offset, real_profile->worker_process_offset,
          real_profile->worker_serialize_offset, real_profile->worker_send_offset,
          (unsigned long long) (receive_time - real_profile->command_start_time),
          (unsigned long long) (rpm_current_us() - real_profile->command_start_time));
    }
  }
}

static inline rpm_context *rpm_context_get(RedisModuleCtx *ctx) {
  return RedisModule_GetAttachment(ctx, NULL, 0);
}

static void rpm_worker_pipe_buffer_reset(worker_pipe *pipe) {
  pipe->read_buffer = pipe->write_buffer = NULL;
  pipe->next_buffer = &pipe->write_buffer;
  pipe->num_buffers = 0;
}

static void rpm_worker_pipe_buffer_data(worker_pipe *pipe, char *buf, int size)
{
  chained_buffer *buffer = chained_buffer_check(&pipe->read_buffer, &pipe->write_buffer, &pipe->next_buffer, &pipe->num_buffers, size, MAX_REAL_LOG_LEN);
  memcpy(buffer->buffer + buffer->size, buf, size);
  buffer->size += size;
}

static void rpm_worker_upstream_submit(RedisModuleCtx *ctx, worker_upstream *upstream, chained_buffer *buf) {
  REDISMODULE_NOT_USED(ctx);
  queue_push(upstream->command_queue, buf);
}

static int32_t rpm_worker_upstream_write(int to, chained_buffer *buf) {
  int32_t wr, available, st = REDISMODULE_OK;
  while ((available = buf->size - buf->offset) > 0) {
    wr = write(to, buf->buffer + buf->offset, available);
    if (wr < 0) {
      if (errno != EINTR) {
        st = REDISMODULE_ERR;
        goto cleanup_exit;
      }
    } else {
      buf->offset += wr;
    }
  }
cleanup_exit:
  chained_buffer_destroy(&buf, 1);
  return st;
}

static void rpm_worker_shutdown(worker_process *worker, char type) {
  if (write(worker->shutdown_pipe[1], &type, 1) != 1) {
    /* ignored */
  }
  pthread_mutex_lock(&worker->refcount_mutex);
  if (worker->shutdown_pipe[1] != 0) {
    close(worker->shutdown_pipe[1]);
    worker->shutdown_pipe[1] = 0;
  }
  pthread_mutex_unlock(&worker->refcount_mutex);
}

static void rpm_worker_upstream_shutdown(worker_upstream *upstream) {
  worker_process *worker = upstream->worker;
  int32_t shutdown = 0;
  pthread_mutex_lock(&worker->refcount_mutex);
  if (--worker->active_upstreams <= 0) {
    shutdown = 1;
  }
  pthread_mutex_unlock(&worker->refcount_mutex);
  if (shutdown) {
    rpm_worker_shutdown(worker, WORKER_SHUTDOWN_EVENT_TYPE_TERMINATE);
  }
}

static void *rpm_worker_reply(worker_process *worker, int64_t client_id, int64_t request_id, void *reply) {
  RedisModuleBlockedClient *bc = NULL;
  void *result;
  size_t ignored;
  int32_t find;
  pthread_mutex_lock(&worker->request_id_to_client_mutex);
  find = hash_map_find(worker->request_id_to_client, hash_map_int_ptr(request_id), sizeof(request_id), (const void **) &bc, &ignored);
  if (find != 0) {
    result = reply;
    pthread_mutex_unlock(&worker->request_id_to_client_mutex);
  } else {
    hash_map_remove(worker->request_id_to_client, hash_map_int_ptr(request_id), sizeof(request_id));
    pthread_mutex_unlock(&worker->request_id_to_client_mutex);
    pthread_mutex_lock(&worker->client_id_to_request_id_mutex);
    hash_map_remove(worker->client_id_to_request_id, hash_map_int_ptr(client_id), sizeof(client_id));
    pthread_mutex_unlock(&worker->client_id_to_request_id_mutex);
    result = NULL;
  }
  if (result == NULL) {
    RedisModule_UnblockClient(bc, reply);
  }
  return result;
}

static int32_t rpm_worker_upstream_reply(worker_upstream *upstream, redis_response *reply) {
  redis_response *client_id, *request_id;
  if (reply->type != REDIS_RESPONSE_ARRAY || reply->payload.array.length != 4 ||
      reply->payload.array.array[0]->type != REDIS_RESPONSE_INTEGER ||
      reply->payload.array.array[1]->type != REDIS_RESPONSE_INTEGER ||
      reply->payload.array.array[2]->type != REDIS_RESPONSE_STRING) {
    redis_response_reader_free_response(reply);
    return REDIS_RESPONSE_ERROR_PROTOCOL;
  }
  client_id = reply->payload.array.array[0];
  request_id = reply->payload.array.array[1];
  reply->receive_time = rpm_current_us();
  reply->upstream = upstream;
  if ((reply = rpm_worker_reply(upstream->worker, client_id->payload.integer, request_id->payload.integer, reply)) != NULL) {
    redis_response_reader_free_response(reply);
  }
  return REDISMODULE_OK;
}

static void *rpm_worker_upstream_writer_dispatch(void *arg) {
  worker_upstream *upstream = arg;
  chained_buffer *command;
  queue_pop_result result;
  while (1) {
    queue_pop(upstream->command_queue, &result);
    while ((command = queue_pop_result_next(&result)) != NULL) {
      if (command == WORKER_PIPE_SHUTDOWN) {
        goto cleanup_exit;
      } else {
        if (rpm_worker_upstream_write(upstream->pipe[0], command) != REDISMODULE_OK) {
          goto cleanup_exit;
        }
      }
    }
  }
cleanup_exit:
  while ((command = queue_pop_result_next(&result)) != NULL) {
    if (command != WORKER_PIPE_SHUTDOWN) {
      chained_buffer_destroy(&command, 1);
    }
  }
  rpm_worker_release(upstream->worker);
  return NULL;
}

static void *rpm_worker_upstream_reader_dispatch(void *arg) {
  int rd, fd;
  worker_upstream *upstream = arg;
  redis_response_status st = REDIS_RESPONSE_OK;
  redis_response_reader *reader = redis_response_reader_create(upstream->worker->rpm->allocator, 1024 * 1024);
  char *buf = upstream->buf;
  redis_response *reply = NULL;
  fd = upstream->pipe[0];
  while (1) {
    while ((rd = read(fd, buf, sizeof(upstream->buf))) > 0) {
      redis_response_reader_feed(reader, buf, rd);
      while (st == REDIS_RESPONSE_OK && (st = redis_response_reader_next(reader, &reply)) == REDIS_RESPONSE_OK && reply != NULL) {
        st = rpm_worker_upstream_reply(upstream, reply);
      }
      if (st != REDIS_RESPONSE_OK) {
        /* protocol error, kill worker and restart to fix the channel */
        rpm_worker_shutdown(upstream->worker, WORKER_SHUTDOWN_EVENT_TYPE_SUICIDE);
        goto cleanup_exit;
      }
    }
    if (rd == 0 || (rd == -1 && errno != EINTR)) {
      /* upstream eof, worker process must be died, restart it automatically */
      rpm_worker_upstream_shutdown(upstream);
      goto cleanup_exit;
    }
  }

cleanup_exit:
  redis_response_reader_free(reader);
  rpm_worker_release(upstream->worker);
  return NULL;
}

static int rpm_worker_command_on_reply_item(RedisModuleCtx *ctx, redis_response *item) {
  size_t idx;
  switch (item->type) {
    case REDIS_RESPONSE_STRING:
      RedisModule_ReplyWithStringBuffer(ctx, item->payload.string.str, item->payload.string.length);
      break;
    case REDIS_RESPONSE_ARRAY:
      RedisModule_ReplyWithArray(ctx, item->payload.array.length);
      for (idx = 0; idx < item->payload.array.length; ++idx) {
        rpm_worker_command_on_reply_item(ctx, item->payload.array.array[idx]);
      }
      break;
    case REDIS_RESPONSE_INTEGER:
      RedisModule_ReplyWithLongLong(ctx, item->payload.integer);
      break;
    case REDIS_RESPONSE_NIL:
      RedisModule_ReplyWithNull(ctx);
      break;
    case REDIS_RESPONSE_STATUS:
      RedisModule_ReplyWithSimpleString(ctx, item->payload.string.str);
      break;
    case REDIS_RESPONSE_ERROR:
      RedisModule_ReplyWithError(ctx, item->payload.string.str);
      break;
    default:
      assert(0);
  }
  return 0;
}

static worker_process *rpm_worker_create(RedisModuleCtx *ctx, rpm_context *rpm);

static int rpm_worker_command_on_reply(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  void *data = RedisModule_GetBlockedClientPrivateData(ctx);
  rpm_context *rpm = rpm_context_get(ctx);
  redis_response *reply = data;
  REDISMODULE_NOT_USED(argv);
  REDISMODULE_NOT_USED(argc);
  if (data == RPM_BAD_GATEWAY) {
    RedisModule_ReplyWithError(ctx, RPM_BAD_GATEWAY);
  } else {
    rpm_worker_command_on_reply_item(ctx, reply->payload.array.array[3]);
    if (rpm->debug_mode) {
      rpm_log_profile_data(ctx, reply->receive_time, reply->upstream, reply->payload.array.array[0],
          reply->payload.array.array[1], reply->payload.array.array[2], "succeeded");
    }
  }
  return 0;
}

static int rpm_worker_command_on_timeout(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  size_t tmp;
  rpm_context *rpm = rpm_context_get(ctx);
  int32_t find;
  long long request_id, client_id = RedisModule_GetClientId(ctx);
  REDISMODULE_NOT_USED(argv);
  REDISMODULE_NOT_USED(argc);
  pthread_mutex_lock(&rpm->worker->client_id_to_request_id_mutex);
  find = hash_map_find(rpm->worker->client_id_to_request_id, hash_map_int_ptr(client_id), sizeof(client_id), (const void **) &request_id, &tmp);
  if (find == 0) {
    hash_map_remove(rpm->worker->client_id_to_request_id, hash_map_int_ptr(client_id), sizeof(client_id));
    pthread_mutex_unlock(&rpm->worker->client_id_to_request_id_mutex);
    pthread_mutex_lock(&rpm->worker->request_id_to_client_mutex);
    hash_map_remove(rpm->worker->request_id_to_client, hash_map_int_ptr(request_id), sizeof(request_id));
    pthread_mutex_unlock(&rpm->worker->request_id_to_client_mutex);
  } else {
    pthread_mutex_unlock(&rpm->worker->client_id_to_request_id_mutex);
  }
  return RedisModule_ReplyWithError(ctx, RPM_REQUEST_TIMEOUT);
}

static void rpm_worker_command_free_reply(void *privdata) {
  if (privdata && privdata != RPM_BAD_GATEWAY) {
    redis_response_reader_free_response(privdata);
  }
}

static long long rpm_worker_process_request(RedisModuleCtx *ctx, rpm_context *rpm, int64_t timeout, int64_t client_id) {
  int64_t request_id = rpm->current_request_id++;
  worker_process *worker = rpm->worker;
  RedisModuleBlockedClient *bc = RedisModule_BlockClient(ctx, rpm_worker_command_on_reply,
      rpm_worker_command_on_timeout, rpm_worker_command_free_reply, timeout);
  pthread_mutex_lock(&rpm->worker->request_id_to_client_mutex);
  hash_map_set(worker->request_id_to_client, hash_map_int_ptr(request_id), sizeof(request_id), bc, sizeof(bc));
  pthread_mutex_unlock(&rpm->worker->request_id_to_client_mutex);
  pthread_mutex_lock(&rpm->worker->client_id_to_request_id_mutex);
  hash_map_set(rpm->worker->client_id_to_request_id, hash_map_int_ptr(client_id),
      sizeof(client_id), hash_map_int_ptr(request_id), sizeof(request_id));
  pthread_mutex_unlock(&rpm->worker->client_id_to_request_id_mutex);
  return request_id;
}

static inline unsigned long long rpm_current_us(void) {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return (tv.tv_sec * 1000000ll) + tv.tv_usec;
}

static int rpm_worker_upstream_write_command(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  int idx;
  size_t len;
  char *tmp, client_id_buffer[32], request_id_buffer[32], timestamp_buffer[32];
  chained_buffer *cmd;
  const char **args, *second, *third, *omit;
  size_t *args_len;
  rpm_context *rpm;
  long long timeout, client_id;
  worker_upstream *upstream;

  rpm = rpm_context_get(ctx);
  timeout = RPM_DEFAULT_TIMEOUT;
  client_id = RedisModule_GetClientId(ctx);
  if (rpm->worker == NULL) {
    RedisModule_ReplyWithError(ctx, RPM_BAD_GATEWAY);
    return REDISMODULE_OK;
  }
  args = RedisModule_Alloc((sizeof(char *) + sizeof(size_t)) * (argc + 3));
  args_len = (size_t *) (args + argc + 3);
  upstream = rpm->worker->upstreams + (client_id % rpm->worker->num_upstreams);
  if (rpm->command_timeout) {
    tmp = (char *) RedisModule_StringPtrLen(argv[0], &len);
    hash_map_find(rpm->command_timeout, tmp, len, (const void **) &timeout, &len);
  }
  args[0] = client_id_buffer;
  args_len[0] = snprintf(client_id_buffer, sizeof(client_id_buffer), "%lld", client_id);
  args[1] = request_id_buffer;
  args_len[1] = snprintf(request_id_buffer, sizeof(request_id_buffer), "%lld", rpm_worker_process_request(ctx, rpm, timeout, client_id));
  args[2] = timestamp_buffer;
  args_len[2] = snprintf(timestamp_buffer, sizeof(timestamp_buffer), "%llu", rpm_current_us());
  for (idx = 0; idx < argc; ++idx) {
    args[idx + 3] = RedisModule_StringPtrLen(argv[idx], args_len + idx + 3);
  }
  if (rpm->debug_mode) {
    if (argc > 1) {
      second = args[4];
      third = argc > 2 ? args[5] : "";
      omit = argc > 3 ? "[...]" : "";
    } else {
      second = "";
      third = "";
      omit = "";
    }
    RedisModule_Log(ctx, "warning", "client %s execute command %s on stream %d@%p: %s %s %s %s", client_id_buffer,
        request_id_buffer, upstream->pipe[0], upstream, args[3], second, third, omit);
  }
  cmd = redis_format_command(rpm->allocator, argc + 3, args, args_len);
  rpm_worker_upstream_submit(ctx, upstream, cmd);
  RedisModule_Free(args);
  return REDISMODULE_OK;
}

static size_t rpm_worker_log_buffer_append(char *buffer, size_t offset, const void *log, size_t length) {
  if (offset + length >= MAX_LOG_LEN) {
    length = MAX_LOG_LEN - offset;
  }
  if (length > 0) {
    memcpy(buffer + offset, log, length);
    return offset + length;
  } else {
    return offset;
  }
}

static size_t rpm_worker_log_buffer_flush(worker_pipe *pipe, char *log_buffer, size_t offset)
{
  chained_buffer *buffer = pipe->read_buffer;
  while (buffer != NULL) {
    offset = rpm_worker_log_buffer_append(log_buffer, offset, buffer->buffer, buffer->size);
    buffer = buffer->next;
  }
  chained_buffer_destroy(&pipe->read_buffer, pipe->num_buffers);
  rpm_worker_pipe_buffer_reset(pipe);
  return offset;
}

static void rpm_worker_log_flush(RedisModuleCtx *ctx, worker_pipe *pipe, const char *log, size_t length, const char *level)
{
  char log_buffer[MAX_REAL_LOG_LEN];
  int32_t offset = 0;
  if (pipe->num_buffers > 0) {
    offset = rpm_worker_log_buffer_flush(pipe, log_buffer, offset);
  }
  offset = rpm_worker_log_buffer_append(log_buffer, offset, log, length);
  if (offset == MAX_LOG_LEN) {
    memset(log_buffer + offset, '.', 6);
    offset += 6;
  }
  log_buffer[offset] = '\0';
  RedisModule_Log(ctx, level, "%s", log_buffer);
}

static void rpm_worker_log_read(RedisModuleCtx *ctx, int fd, void *client_data, int mask, const char *level) {
  int rd;
  worker_pipe *pipe = client_data;
  worker_process *worker = pipe->worker;
  char *buf = worker->log_buf, *start, *end, *linefeed = NULL;
  REDISMODULE_NOT_USED(mask);
  while ((rd = read(fd, buf, sizeof(worker->log_buf))) > 0) {
    linefeed = start = buf;
    end = buf + rd;
scan_for_lf:
    if (start < end && (linefeed = memchr(start, '\n', end - start)) != NULL) {
      rpm_worker_log_flush(ctx, pipe, start, linefeed - start, level);
      start = linefeed + 1;
      goto scan_for_lf;
    }
    /* check if there are more data without linefeed left in read buffer */
    if (linefeed == NULL && start < end) {
      rpm_worker_pipe_buffer_data(pipe, start, end - start);
    }
  }
}

static void rpm_worker_stdout_read(RedisModuleCtx *ctx, int fd, void *client_data, int mask) {
  rpm_worker_log_read(ctx, fd, client_data, mask, "notice");
}

static void rpm_worker_stderr_read(RedisModuleCtx *ctx, int fd, void *client_data, int mask) {
  rpm_worker_log_read(ctx, fd, client_data, mask, "warning");
}

static void rpm_worker_close_pipe(RedisModuleCtx *ctx, worker_pipe *pipe) {
  if (pipe->pipe[0]) {
    RedisModule_DeleteFileEvent(ctx, pipe->pipe[0], REDISMODULE_READ | (pipe->write_buffer != NULL ? REDISMODULE_WRITE : 0));
    if (pipe->read_buffer != NULL) {
      chained_buffer_destroy(&pipe->read_buffer, pipe->num_buffers);
    }
    if (pipe->pipe[0] != 0) {
      close(pipe->pipe[0]);
      pipe->pipe[0] = 0;
    }
    if (pipe->pipe[1] != 0) {
      close(pipe->pipe[1]);
      pipe->pipe[1] = 0;
    }
  }
}

static void rpm_worker_close_upstream(RedisModuleCtx *ctx, worker_upstream *upstream) {
  REDISMODULE_NOT_USED(ctx);
  if (upstream->pipe[0]) {
    rpm_worker_upstream_submit(ctx, upstream, WORKER_PIPE_SHUTDOWN);
    if (upstream->pipe[0] != 0) {
      close(upstream->pipe[0]);
      upstream->pipe[0] = 0;
    }
    if (upstream->pipe[1] != 0) {
      close(upstream->pipe[1]);
      upstream->pipe[1] = 0;
    }
  }
}

static inline void rpm_worker_redirect_fd(int from, int to) {
  if (from == to) {
    return;
  }
  dup2(from, to);
  close(from);
}

static void rpm_worker_process_daemonize() {
  pid_t pid;

  umask(027);
  setsid();
  signal(SIGCHLD, SIG_IGN);
  signal(SIGHUP, SIG_IGN);
  if ((pid = fork()) != 0) {
    if (pid < 0) {
      exit(1);
    } else {
      exit(0);
    }
  }
}

static void rpm_worker_process_start(worker_process *worker, char **argv, int32_t generation) {
  long idx, cidx, closefd, max = sysconf(_SC_OPEN_MAX);
  char buff[32], envbuff[32 + sizeof(ENV_RPM_DOWNSTREAM_FD_PREFIX) + sizeof(ENV_RPM_UPSTREAM_FD_PREFIX)];
  rpm_worker_process_daemonize();
  for (idx = 1; idx < max; ++idx) {
    if (idx == worker->out.pipe[1] || idx == worker->err.pipe[1]) {
      continue;
    }
    closefd = 1;
    if (closefd != 0) {
      for (cidx = 0; cidx < worker->num_upstreams; ++cidx) {
        if (idx == worker->upstreams[cidx].pipe[1]) {
          closefd = 0;
          break;
        }
      }
    }
    if (closefd != 0) {
      for (cidx = 0; cidx < worker->num_downstreams; ++cidx) {
        if (idx == worker->downstreams[cidx]) {
          closefd = 0;
          break;
        }
      }
    }
    if (closefd != 0) {
      close(idx);
    }
  }
  rpm_worker_redirect_fd(worker->out.pipe[1], STDOUT_FILENO);
  rpm_worker_redirect_fd(worker->err.pipe[1], STDERR_FILENO);
  snprintf(buff, sizeof(buff), "%d", worker->num_upstreams);
  setenv(ENV_RPM_UPSTREAM_FD_NUM, buff, 1);
  for (cidx = 0; cidx < worker->num_upstreams; ++cidx) {
    snprintf(envbuff, sizeof(envbuff), "%s[%ld]", ENV_RPM_UPSTREAM_FD_PREFIX, cidx);
    snprintf(buff, sizeof(buff), "%d", worker->upstreams[cidx].pipe[1]);
    setenv(envbuff, buff, 1);
  }
  snprintf(buff, sizeof(buff), "%d", worker->num_downstreams);
  setenv(ENV_RPM_DOWNSTREAM_FD_NUM, buff, 1);
  for (cidx = 0; cidx < worker->num_downstreams; ++cidx) {
    snprintf(envbuff, sizeof(envbuff), "%s[%ld]", ENV_RPM_DOWNSTREAM_FD_PREFIX, cidx);
    snprintf(buff, sizeof(buff), "%d", worker->downstreams[cidx]);
    setenv(envbuff, buff, 1);
  }
  snprintf(buff, sizeof(buff), "%d", generation);
  setenv(ENV_RPM_WORKER_GENERATION, buff, 1);
  if (worker->rpm->downstream_address && strlen(worker->rpm->downstream_address) > 0) {
    setenv(ENV_RPM_DOWNSTREAM_ADDRESS, worker->rpm->downstream_address, 1);
  }
  fprintf(stdout, "worker process %lld is spawned and prepare to serve requests\n", (long long) getpid());
  fflush(stdout);
  execvp(argv[0], argv);
  exit(1);
}

static int32_t rpm_worker_pipe_init(RedisModuleCtx *ctx, worker_process *worker, worker_pipe *p,
    RedisModuleFileProc read) {
  if (pipe(p->pipe) != 0) {
    return REDISMODULE_ERR;
  }
  RedisModule_EnableNonBlock(ctx, p->pipe[0]);
  rpm_worker_pipe_buffer_reset(p);
  p->worker = worker;

  if (read != NULL) {
    return RedisModule_CreateFileEvent(ctx, p->pipe[0], REDISMODULE_READ, read, p);
  }
  return REDISMODULE_OK;
}

static int32_t rpm_worker_upstream_init(RedisModuleCtx *ctx, rpm_context *rpm, worker_process *worker, worker_upstream *upstream) {
  REDISMODULE_NOT_USED(ctx);
  if (socketpair(AF_UNIX,SOCK_STREAM, 0, upstream->pipe) < 0) {
    return REDISMODULE_ERR;
  }
  upstream->worker = worker;
  upstream->command_queue = queue_create(rpm->allocator, 1024);
  return REDISMODULE_OK;
}

static int32_t rpm_worker_upstream_start(worker_process *worker, worker_upstream *upstream) {
  int32_t st = REDISMODULE_OK;
  pthread_mutex_lock(&worker->refcount_mutex);
  worker->refcount++;
  if (pthread_create(&upstream->writer, NULL, rpm_worker_upstream_writer_dispatch, upstream)) {
    st = REDISMODULE_ERR;
    worker->refcount--;
    goto cleanup_exit;
  }
  worker->refcount++;
  if (pthread_create(&upstream->reader, NULL, rpm_worker_upstream_reader_dispatch, upstream)) {
    st = REDISMODULE_ERR;
    worker->refcount--;
  }
cleanup_exit:
  pthread_mutex_unlock(&worker->refcount_mutex);
  return st;
}

static int rpm_worker_create_later(RedisModuleCtx *ctx, long long id, void *user_data) {
  rpm_context *rpm = user_data;
  REDISMODULE_NOT_USED(id);
  if ((rpm->worker = rpm_worker_create(ctx, rpm)) != NULL) {
    rpm->restart_timer = -1;
    return REDISMODULE_TIME_NOMORE;
  } else {
    return 0;
  }
}

static void rpm_worker_free(worker_process *worker) {
  queue_pop_result result;
  chained_buffer *command;
  int32_t idx;

  if (worker->downstreams != NULL) {
    RedisModule_Free(worker->downstreams);
    worker->downstreams = NULL;
  }

  if (worker->upstreams != NULL) {
    for (idx = 0; idx < worker->num_upstreams; ++idx) {
      queue_peek(worker->upstreams[idx].command_queue, &result);
      while ((command = queue_pop_result_next(&result)) != NULL) {
        if (command != WORKER_PIPE_SHUTDOWN) {
          chained_buffer_destroy(&command, 1);
        }
      }
      queue_destroy(worker->upstreams[idx].command_queue);
    }
    RedisModule_Free(worker->upstreams);
    worker->upstreams = NULL;
  }

  pthread_mutex_destroy(&worker->refcount_mutex);
  pthread_mutex_destroy(&worker->client_id_to_request_id_mutex);
  pthread_mutex_destroy(&worker->request_id_to_client_mutex);

  RedisModule_Free(worker);
}

static void rpm_worker_release(worker_process *worker) {
  int32_t free = 0;
  pthread_mutex_lock(&worker->refcount_mutex);
  free = --worker->refcount == 0;
  pthread_mutex_unlock(&worker->refcount_mutex);
  if (free) {
    rpm_worker_free(worker);
  }
}

static void rpm_worker_destroy(RedisModuleCtx *ctx, rpm_context *rpm, worker_process *worker) {
  /* send worker dead error to pending requests of current worker */
  int64_t request_id;
  size_t ignored;
  int32_t idx;
  RedisModuleBlockedClient *bc = NULL;

  rpm->worker = worker;
  pthread_mutex_lock(&rpm->worker->request_id_to_client_mutex);
  hash_map_iterator *iterator = hash_map_iterator_create(worker->request_id_to_client);
  while (hash_map_iterator_next(iterator) != 0) {
    hash_map_iterator_get(iterator, (const void **) &request_id, &ignored, (const void **) &bc, &ignored);
    RedisModule_UnblockClient(bc, (void *) RPM_BAD_GATEWAY);
  }
  hash_map_iterator_destroy(iterator);
  hash_map_destroy(worker->request_id_to_client);
  pthread_mutex_unlock(&rpm->worker->request_id_to_client_mutex);
  pthread_mutex_lock(&rpm->worker->client_id_to_request_id_mutex);
  hash_map_destroy(worker->client_id_to_request_id);
  pthread_mutex_unlock(&rpm->worker->client_id_to_request_id_mutex);
  if (worker->downstreams != NULL) {
    for (idx = 0; idx < worker->num_downstreams; ++idx) {
      if (worker->downstreams[idx] != 0) {
        close(worker->downstreams[idx]);
        worker->downstreams[idx] = 0;
      }
    }
  }
  if (worker->upstreams != NULL) {
    for (idx = 0; idx < worker->num_upstreams; ++idx) {
      rpm_worker_upstream_submit(ctx, worker->upstreams + idx, WORKER_PIPE_SHUTDOWN);
      pthread_join(worker->upstreams[idx].reader, NULL);
      pthread_join(worker->upstreams[idx].writer, NULL);
      rpm_worker_close_upstream(ctx, worker->upstreams + idx);
    }
  }
  rpm_worker_close_pipe(ctx, &worker->out);
  rpm_worker_close_pipe(ctx, &worker->err);
  RedisModule_Log(ctx, "warning", "worker process is terminated prematurely, a new worker is being spawned");
  /* failed to create worker during initialization, setup a timer to try it again 10 ms later */
  if (rpm->restart_timer == -1) {
    RedisModule_CreateTimeEvent(ctx, 10, rpm_worker_create_later, rpm, NULL, &rpm->restart_timer);
  }
  RedisModule_DeleteFileEvent(ctx, worker->shutdown_pipe[0], REDISMODULE_READ);
  close(worker->shutdown_pipe[0]);
  rpm->restart_count--;
  if (++rpm->restart_count >= rpm->retry_attempts) {
    RedisModule_Log(ctx, "warning", "worker has been stopped %d times within last %d seconds, shutdown redis automatically", rpm->restart_count, rpm->retry_timeout);
    rpm_context_shutdown_redis(ctx, rpm);
  }
  rpm->worker = NULL;
  rpm_worker_release(worker);
}

static void rpm_worker_suicide(RedisModuleCtx *ctx, worker_process *worker) {
  int32_t idx;
  for (idx = 0; idx < worker->num_upstreams; ++idx) {
    rpm_worker_close_upstream(ctx, worker->upstreams + idx);
  }
  for (idx = 0; idx < worker->num_downstreams; ++idx) {
    if (worker->downstreams[idx] != 0) {
      close(worker->downstreams[idx]);
      worker->downstreams[idx] = 0;
    }
  }
}

static void rpm_worker_shutdown_pipe_read(RedisModuleCtx *ctx, int fd, void *client_data, int mask) {
  char buff;
  int rd;
  REDISMODULE_NOT_USED(mask);
  while ((rd = read(fd, &buff, 1)) != 0) {
    if (rd == 1) {
      switch (buff) {
        case WORKER_SHUTDOWN_EVENT_TYPE_SUICIDE:
          RedisModule_Log(ctx, "warning", "worker process respond invalid response and will be restarted to fix the state.");
          rpm_worker_suicide(ctx, client_data);
          break;
        case WORKER_SHUTDOWN_EVENT_TYPE_TERMINATE:
          RedisModule_Log(ctx, "warning", "All upstream connections were closed, restart worker process now");
          break;
        default:
          RedisModule_Log(ctx, "warning", "Received unknown upstream event %c", buff);
          break;
      }
    }
  }
  rpm_worker_destroy(ctx, rpm_context_get(ctx), client_data);
}

static worker_process *rpm_worker_create(RedisModuleCtx *ctx, rpm_context *rpm) {
  int32_t idx, generation;
  int status;
  time_t now;
  pid_t pid;
  worker_process *worker = RedisModule_Calloc(1, sizeof(worker_process));
  worker->request_id_to_client = hash_map_create(&int_key_map, 1024);
  worker->client_id_to_request_id = hash_map_create(&int_key_map, 1024);
  worker->num_downstreams = rpm->num_downstreams;
  worker->num_upstreams = rpm->num_upstreams;
  pthread_mutex_init(&worker->refcount_mutex, NULL);
  pthread_mutex_init(&worker->request_id_to_client_mutex, NULL);
  pthread_mutex_init(&worker->client_id_to_request_id_mutex, NULL);
  worker->rpm = rpm;
  worker->active_upstreams = worker->num_upstreams;
  worker->refcount = 1;
  if (pipe(worker->shutdown_pipe) != 0) {
    RedisModule_Log(ctx, "warning", "Could not create worker shutdown pipe");
    goto cleanup_exit;
  }
  if ((worker->downstreams = RedisModule_Calloc(1, sizeof(worker->downstreams[0]) * worker->num_downstreams)) == NULL) {
    RedisModule_Log(ctx, "warning", "Could not allocate enough memory for downstreams, restart worker again");
    goto cleanup_exit;
  }
  for (idx = 0; idx < worker->num_downstreams; ++idx) {
    if (RedisModule_CreateClient(ctx, worker->downstreams + idx) != REDISMODULE_OK) {
      RedisModule_Log(ctx, "warning", "Could not create client for downstream %d, restart worker again", idx);
      goto cleanup_exit;
    }
  }
  if (rpm_worker_pipe_init(ctx, worker, &worker->out, rpm_worker_stdout_read) ||
      rpm_worker_pipe_init(ctx, worker, &worker->err, rpm_worker_stderr_read)) {
    RedisModule_Log(ctx, "warning", "Could not initiate stdout and stderr pipe, restart worker again");
    goto cleanup_exit;
  }
  if ((worker->upstreams = RedisModule_Calloc(1, sizeof(worker->upstreams[0]) * worker->num_upstreams)) == NULL) {
    RedisModule_Log(ctx, "warning", "Could not allocate enough memory for response reader, restart worker again");
    goto cleanup_exit;
  }
  for (idx = 0; idx < worker->num_upstreams; ++idx) {
    if (rpm_worker_upstream_init(ctx, rpm, worker, worker->upstreams + idx)) {
      RedisModule_Log(ctx, "warning", "Could not initiate upstream pipe %d, restart worker again", idx);
      goto cleanup_exit;
    }
  }
  time(&now);
  if (now >= rpm->restart_count_reset_time && (now - rpm->restart_count_reset_time) > rpm->retry_timeout) {
    rpm->restart_count_reset_time = now;
    rpm->restart_count = 0;
  }
  RedisModule_CreateFileEvent(ctx, worker->shutdown_pipe[0], REDISMODULE_READ, rpm_worker_shutdown_pipe_read, worker);

  generation = rpm->generation++;
  if ((pid = fork()) == 0) {
    rpm_worker_process_start(worker, rpm->argv, generation);
  } else if (pid == -1) {
    RedisModule_Log(ctx, "warning", "Could not create new worker process, restart worker again: %s", strerror(errno));
    goto cleanup_exit;
  } else {
    status = 0;
    waitpid(pid, &status, 0);
    if (WEXITSTATUS(status) != 0) {
      RedisModule_Log(ctx, "warning", "Could not daemonize worker process, restart worker again: %s", strerror(errno));
      goto cleanup_exit;
    }
    for (idx = 0; idx < worker->num_upstreams; ++idx) {
      rpm_worker_upstream_start(worker, worker->upstreams + idx);
      if (worker->upstreams[idx].pipe[1] != 0) {
        close(worker->upstreams[idx].pipe[1]);
        worker->upstreams[idx].pipe[1] = 0;
      }
    }
    close(worker->out.pipe[1]);
    worker->out.pipe[1] = 0;
    close(worker->err.pipe[1]);
    worker->err.pipe[1] = 0;
    for (idx = 0; idx < worker->num_downstreams; ++idx) {
      if (worker->downstreams[idx] != 0) {
        close(worker->downstreams[idx]);
        worker->downstreams[idx] = 0;
      }
    }
  }
  return worker;
cleanup_exit:
  rpm_worker_destroy(ctx, rpm, worker);
  return NULL;
}

static void rpm_context_destroy(RedisModuleCtx *ctx, rpm_context *rpm) {
  char **argv;
  if (rpm->worker != NULL) {
    rpm_worker_destroy(ctx, rpm, rpm->worker);
  }
  if (rpm->command_timeout != NULL) {
    hash_map_destroy(rpm->command_timeout);
  }
  if (rpm->restart_timer != -1) {
    RedisModule_DeleteTimeEvent(ctx, rpm->restart_timer);
  }
  allocator_destroy(rpm->allocator);
  for (argv = rpm->argv; *argv != NULL; ++argv) {
    RedisModule_Free(*argv);
  }
  RedisModule_Free(rpm->argv);
  RedisModule_Free(rpm->shutdown_command);
  RedisModule_Free(rpm->downstream_address);
  RedisModule_Free(rpm);
}

static void *rpm_context_field_at_offset(rpm_context *ctx, uint32_t offset) {
  return ((uint8_t *) ctx) + offset;
}

static void rpm_context_int32_config_parser(void *field, RedisModuleString **argv) {
  size_t len;
  *((int32_t *) field) = strtol(RedisModule_StringPtrLen(argv[0], &len), NULL, 10);
}

static void rpm_context_string_config_parser(void *field, RedisModuleString **argv) {
  size_t len;
  char *copy;
  const char *arg = RedisModule_StringPtrLen(argv[0], &len);
  copy = RedisModule_Alloc(len + 1);
  memcpy(copy, arg, len);
  copy[len] = '\0';
  *((char **) field) = copy;
}

static void rpm_context_bool_config_parser(void *field, RedisModuleString **argv) {
  REDISMODULE_NOT_USED(argv);
  *((int32_t *) field) = 1;
}

typedef struct string_builder {
  char *buffer;
  size_t cap;
  size_t size;
} string_builder;

static string_builder *rpm_string_builder_append(string_builder *builder, const char *needle, size_t len) {
  if (len == 0) {
    return builder;
  }
  if (builder == NULL) {
    builder = RedisModule_Calloc(1, sizeof(string_builder));
  }
  if (builder->cap < builder->size + len + 1) {
    builder->cap += len + 1;
    builder->buffer = RedisModule_Realloc(builder->buffer, builder->cap);
  }
  memcpy(builder->buffer + builder->size, needle, len);
  builder->size += len;
  return builder;
}

static char *rpm_string_builder_build(string_builder *builder) {
  char *str = builder->buffer;
  builder->buffer[builder->size] = '\0';
  RedisModule_Free(builder);
  return str;
}

static char *rpm_evaluate_arg(const char *arg, size_t len) {
  const char *env = NULL;
  const char *value = NULL;
  char *dup = RedisModule_Calloc(1, len + 1);
  char *new_start = dup, *start = dup;
  char *end = NULL;
  const char *limit = start + len;
  string_builder *builder = NULL;
  memcpy(dup, arg, len + 1);
  while (start < limit && (new_start = strstr(start, "${")) != NULL &&
      (new_start + 3 < limit) && (end = strstr(new_start + 3, "}")) != NULL) {
    env = new_start + 2;
    *end = '\0';
    builder = rpm_string_builder_append(builder, start, new_start - start);
    if ((value = getenv(env)) != NULL) {
      builder = rpm_string_builder_append(builder, value, strlen(value));
    } else {
      builder = rpm_string_builder_append(builder, "${", 2);
      builder = rpm_string_builder_append(builder, env, strlen(env));
      builder = rpm_string_builder_append(builder, "}", 1);
    }
    start = end + 1;
  }
  if (builder != NULL) {
    builder = rpm_string_builder_append(builder, start, strlen(start));
    RedisModule_Free(dup);
    return rpm_string_builder_build(builder);
  } else {
    return dup;
  }
}

static rpm_context *rpm_context_create(RedisModuleCtx *ctx, int argc, RedisModuleString **argv)
{
  int idx, didx, is_config, last_is_command = 0;
  int64_t timeout;
  size_t len;
  const char *arg;
  rpm_context *rpm = RedisModule_Calloc(1, sizeof(rpm_context));
  hash_map *command_timeout = NULL;
  const rpm_context_config_descriptor *config;
  rpm->allocator = allocator_create(rpm_allocator_malloc, rpm_allocator_calloc,
    rpm_allocator_realloc, rpm_allocator_free, rpm_allocator_destroy, rpm);
  for (idx = 0; idx + 1 < argc; idx++) {
    is_config = 0;
    arg = RedisModule_StringPtrLen(argv[idx], &len);
    for (config = rpm_config_descriptors; config->name != NULL; ++config) {
      if (strcasecmp(config->name, arg) == 0 && argc - 2 - idx >= config->argc) {
        config->parser(rpm_context_field_at_offset(rpm, config->offset), argv + idx + 1);
        idx += config->argc;
        is_config = 1;
        last_is_command = 0;
        break;
      }
    }
    if (is_config) {
      continue;
    }
    if (strcasecmp("--command", arg) == 0) {
      /* consume the next argument */
      arg = RedisModule_StringPtrLen(argv[++idx], &len);
      if (RedisModule_CreateCommand(ctx, arg, rpm_worker_upstream_write_command, "", 0, 0, 0) == REDISMODULE_ERR) {
        RedisModule_Log(ctx, "warning", "could not register command %s", arg);
      }
      last_is_command = 1;
    } else if (strcasecmp("--timeout", arg) == 0) {
      /* consume the next argument */
      arg = RedisModule_StringPtrLen(argv[++idx], &len);
      timeout = strtol(arg, NULL, 10);
      if (!last_is_command) {
        RedisModule_Log(ctx, "warning", "--command <cmd> is required before --timeout");
      } else {
        arg = RedisModule_StringPtrLen(argv[idx - 2], &len);
        if (command_timeout == NULL) {
          command_timeout_map.allocator = rpm->allocator;
          rpm->command_timeout = command_timeout = hash_map_create(&command_timeout_map, 128);
        }
        hash_map_set(command_timeout, arg, len, hash_map_int_ptr(timeout), sizeof(timeout));
      }
      last_is_command = 0;
    } else {
      last_is_command = 0;
      break;
    }
  }
  if (argc == idx) {
    goto cleanup_exit;
  }
  rpm->argv = RedisModule_Alloc(sizeof(const char *) * (argc - idx + 1));
  for (didx = 0; idx < argc; ++didx, ++idx) {
    arg = RedisModule_StringPtrLen(argv[idx], &len);
    rpm->argv[didx] = rpm_evaluate_arg(arg, len);
  }
  rpm->argv[didx] = NULL;

  if (rpm->num_downstreams <= 0) {
    rpm->num_downstreams = RPM_DEFAULT_DOWNSTREAM;
  } else if (rpm->num_downstreams > RPM_MAX_DOWNSTREAM) {
    rpm->num_downstreams = RPM_MAX_DOWNSTREAM;
  }
  if (rpm->num_upstreams <= 0) {
    rpm->num_upstreams = RPM_DEFAULT_UPSTREAM;
  } else if (rpm->num_upstreams > RPM_MAX_UPSTREAM) {
    rpm->num_upstreams = RPM_MAX_UPSTREAM;
  }
  if (rpm->retry_timeout <= 0) {
    rpm->retry_timeout = 0x7FFFFFFF;
  }
  if (rpm->retry_attempts <= 0) {
    rpm->retry_attempts = 0x7FFFFFFF;
  }
  rpm->restart_timer = -1;
  time(&rpm->restart_count_reset_time);
  if ((rpm->worker = rpm_worker_create(ctx, rpm)) == NULL) {
    goto cleanup_exit;
  }
  return rpm;
cleanup_exit:
  if (rpm != NULL) {
    rpm_context_destroy(ctx, rpm);
  }
  return NULL;
}

static void rpm_context_shutdown_redis(RedisModuleCtx *ctx, rpm_context *rpm) {
  const char *shutdown = rpm->shutdown_command ? rpm->shutdown_command : "shutdown";
  RedisModuleCallReply *reply = RedisModule_Call(ctx, shutdown, "");
  if (reply != NULL) {
    RedisModule_FreeCallReply(reply);
  }
}

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  rpm_context *rpm;
  if (RedisModule_Init(ctx, "rpm", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }
  if ((rpm = rpm_context_create(ctx, argc, argv)) == NULL) {
    return REDISMODULE_ERR;
  }
  RedisModule_Attach(ctx, NULL, 0, rpm, (RedisModuleFinalizer) rpm_context_destroy);
  return REDISMODULE_OK;
}
