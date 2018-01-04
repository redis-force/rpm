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

#define RPM_DEFAULT_UPSTREAM (4)
#define RPM_MAX_UPSTREAM (16)

#define ENV_RPM_DOWNSTREAM_FD_PREFIX "RPM_DOWNSTREAM_FD"
#define ENV_RPM_DOWNSTREAM_FD_NUM "RPM_DOWNSTREAM_FD_NUM"
#define ENV_RPM_UPSTREAM_FD_PREFIX "RPM_UPSTREAM_FD"
#define ENV_RPM_UPSTREAM_FD_NUM "RPM_UPSTREAM_FD_NUM"

const char *RPM_BAD_GATEWAY = "ERR Bad Gateway";
const char *RPM_REQUEST_TIMEOUT = "ERR Request Timeout";
const int64_t RPM_DEFAULT_TIMEOUT = 1000;

typedef struct rpm_context rpm_context;
typedef struct worker_process worker_process;
typedef struct worker_pipe worker_pipe;
typedef struct rpm_context_config_descriptor rpm_context_config_descriptor;

static void rpm_worker_destroy(RedisModuleCtx *ctx, rpm_context *rpm, worker_process *worker);
static worker_process *rpm_worker_create(RedisModuleCtx *ctx, rpm_context *rpm);
static void rpm_worker_suicide(RedisModuleCtx *ctx, worker_process *worker);
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
  time_t restart_count_reset_time;
  char *shutdown_command;
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

struct worker_pipe {
  worker_process *worker;
  chained_buffer *read_buffer;
  chained_buffer *write_buffer;
  chained_buffer **next_buffer;
  int32_t num_buffers;
  int pipe[2];
  void *user_data;
};

struct worker_process {
  worker_pipe out;
  worker_pipe err;
  redis_response_reader **readers;
  int32_t num_downstreams;
  int *downstreams;
  int32_t num_upstreams;
  int32_t active_upstreams;
  worker_pipe *upstreams;
  hash_map *request_id_to_client;
  hash_map *client_id_to_request_id;
  char buf[1024*1024];
};

static void rpm_log_profile_data(RedisModuleCtx *ctx, uint64_t receive_time, worker_pipe *upstream,
    redis_response *client_id, redis_response *request_id, redis_response *profile, const char *state) {
  struct rpm_command_profile_data *real_profile = (struct rpm_command_profile_data *) profile->payload.string.str;
  if (profile->payload.string.length < sizeof(struct rpm_command_profile_data)) {
    RedisModule_Log(ctx, "warning", "invalid profile data for client %lld command %lld");
  } else {
    RedisModule_Log(ctx, "warning", "execute client %lld command %lld received at %lld %s "
        "with %u bytes payload from upstream %d:%d@%p: 0/%u/%u/%u/%u/%llu/%llu",
        client_id->payload.integer, request_id->payload.integer, (long long) real_profile->command_start_time, state,
        real_profile->serialized_size, upstream->pipe[0], upstream->pipe[1], upstream, 
        real_profile->worker_start_offset, real_profile->worker_process_offset,
        real_profile->worker_serialize_offset, real_profile->worker_send_offset,
        (unsigned long long) (receive_time - real_profile->command_start_time),
        (unsigned long long) (rpm_current_us() - real_profile->command_start_time));
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

static void rpm_worker_pipe_buffer_chained_buffer(worker_pipe *pipe, chained_buffer *buf)
{
  chained_buffer_chain(&pipe->read_buffer, &pipe->write_buffer, &pipe->next_buffer, &pipe->num_buffers, buf);
}

static void rpm_worker_pipe_buffer_data(worker_pipe *pipe, char *buf, int size)
{
  chained_buffer *buffer = chained_buffer_check(&pipe->read_buffer, &pipe->write_buffer, &pipe->next_buffer, &pipe->num_buffers, size, MAX_REAL_LOG_LEN);
  memcpy(buffer->buffer + buffer->size, buf, size);
  buffer->size += size;
}

static void rpm_worker_upstream_buffer_write(RedisModuleCtx *ctx, int fd, void *client_data, int mask) {
  worker_pipe *pipe = client_data;
  chained_buffer *current = pipe->read_buffer;
  int wr = 0, count = 0, available;
  REDISMODULE_NOT_USED(ctx);
  REDISMODULE_NOT_USED(mask);
  while (current != NULL) {
    available = current->size - current->offset;
    if ((wr = write(fd, current->buffer + current->offset, available)) < available) {
      /* there are data leftover inside current buffer */
      if (wr > 0) {
        current->offset += wr;
      }
      break;
    }
    ++count;
    current = current->next;
  }

  chained_buffer_destroy(&pipe->read_buffer, count);
  pipe->num_buffers -= count;
  if (pipe->read_buffer == NULL) {
    rpm_worker_pipe_buffer_reset(pipe);
    RedisModule_DeleteFileEvent(ctx, fd, REDISMODULE_WRITE);
  }
}

static void rpm_worker_upstream_buffer_data(RedisModuleCtx *ctx, worker_pipe *pipe, int to, chained_buffer *buf) {
  if (pipe->write_buffer == NULL) {
    RedisModule_CreateFileEvent(ctx, to, REDISMODULE_WRITE, rpm_worker_upstream_buffer_write, pipe);
  }
  rpm_worker_pipe_buffer_chained_buffer(pipe, buf);
}

static void rpm_worker_upstream_write(RedisModuleCtx *ctx, worker_pipe *pipe, int to, chained_buffer *buf) {
  int32_t wr, available;
  if (pipe->write_buffer == NULL) {
    available = buf->size - buf->offset;
    if ((wr = write(to, buf->buffer + buf->offset, available)) == available) {
      chained_buffer_destroy(&buf, 1);
      return;
    }
    if (wr > 0) {
      buf->offset += wr;
    }
  }
  rpm_worker_upstream_buffer_data(ctx, pipe, to, buf);
  return;
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
  long long request_id, client_id = RedisModule_GetClientId(ctx);
  REDISMODULE_NOT_USED(argv);
  REDISMODULE_NOT_USED(argc);
  if (hash_map_find(rpm->worker->client_id_to_request_id, hash_map_int_ptr(client_id), sizeof(client_id), (const void **) &request_id, &tmp) == 0) {
    hash_map_remove(rpm->worker->client_id_to_request_id, hash_map_int_ptr(client_id), sizeof(client_id));
    hash_map_remove(rpm->worker->request_id_to_client, hash_map_int_ptr(request_id), sizeof(request_id));
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
  hash_map_set(worker->request_id_to_client, hash_map_int_ptr(request_id), sizeof(request_id), bc, sizeof(bc));
  hash_map_set(rpm->worker->client_id_to_request_id, hash_map_int_ptr(client_id),
      sizeof(client_id), hash_map_int_ptr(request_id), sizeof(request_id));
  return request_id;
}

static void *rpm_worker_process_reply(rpm_context *rpm, int64_t client_id, int64_t request_id, void *reply) {
  RedisModuleBlockedClient *bc = NULL;
  worker_process *worker = rpm->worker;
  size_t ignored;
  if (hash_map_find(worker->request_id_to_client, hash_map_int_ptr(request_id), sizeof(request_id), (const void **) &bc, &ignored) != 0) {
    return reply;
  } else {
    hash_map_remove(worker->client_id_to_request_id, hash_map_int_ptr(client_id), sizeof(client_id));
    hash_map_remove(worker->request_id_to_client, hash_map_int_ptr(request_id), sizeof(request_id));
    RedisModule_UnblockClient(bc, reply);
    return NULL;
  }
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
  worker_pipe *upstream;

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
    RedisModule_Log(ctx, "warning", "client %s execute command %s on stream %d:%d@%p: %s %s %s %s", client_id_buffer, 
        request_id_buffer, upstream->pipe[0], upstream->pipe[1], upstream, args[3], second, third, omit);
  }
  cmd = redis_format_command(rpm->allocator, argc + 3, args, args_len);
  rpm_worker_upstream_write(ctx, upstream, upstream->pipe[0], cmd);
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

static int rpm_worker_upstream_on_reply(RedisModuleCtx *ctx, redis_response *reply, worker_pipe *upstream) {
  redis_response *client_id, *request_id;
  rpm_context *rpm = rpm_context_get(ctx);
  if (reply->type != REDIS_RESPONSE_ARRAY || reply->payload.array.length != 4 ||
      reply->payload.array.array[0]->type != REDIS_RESPONSE_INTEGER ||
      reply->payload.array.array[1]->type != REDIS_RESPONSE_INTEGER ||
      reply->payload.array.array[2]->type != REDIS_RESPONSE_STRING) {
    return REDIS_RESPONSE_ERROR_PROTOCOL;
  }
  client_id = reply->payload.array.array[0];
  request_id = reply->payload.array.array[1];
  reply->receive_time = rpm_current_us();
  reply->upstream = upstream;
  if ((reply = rpm_worker_process_reply(rpm, client_id->payload.integer, request_id->payload.integer, reply)) != NULL) {
    if (rpm->debug_mode) {
      rpm_log_profile_data(ctx, reply->receive_time, upstream, client_id, request_id, reply->payload.array.array[2], "timeout");
    }
    redis_response_reader_free_response(reply);
  }
  return REDIS_RESPONSE_OK;
}

static void rpm_worker_upstream_read(RedisModuleCtx *ctx, int fd, void *client_data, int mask) {
  int rd;
  rpm_context *rpm;
  redis_response_status st = REDIS_RESPONSE_OK;
  worker_pipe *pipe = client_data;
  worker_process *worker = pipe->worker;
  redis_response_reader *reader = pipe->user_data;
  char *buf = worker->buf;
  redis_response *reply = NULL;
  REDISMODULE_NOT_USED(mask);
  while ((rd = read(fd, buf, sizeof(worker->buf))) > 0) {
    redis_response_reader_feed(reader, buf, rd);
    while (st == REDIS_RESPONSE_OK && (st = redis_response_reader_next(reader, &reply)) == REDIS_RESPONSE_OK && reply != NULL) {
      st = rpm_worker_upstream_on_reply(ctx, reply, pipe);
    }
    if (st != REDIS_RESPONSE_OK) {
      /* protocol error, kill worker and restart to fix the channel */
      RedisModule_Log(ctx, "warning", "worker process respond invalid response and will be restarted to fix the state. %s",
          redis_response_reader_strerr(reader));
      rpm_worker_suicide(ctx, worker);
    }
  }
  if (rd == 0) {
    /* upstream eof, worker process must be died, restart it automatically */
    RedisModule_DeleteFileEvent(ctx, fd, REDISMODULE_READ);
    RedisModule_Log(ctx, "warning", "Upstream connection with fd %d was closed", fd);
    rpm = rpm_context_get(ctx);
    if (--worker->active_upstreams <= 0) {
      RedisModule_Log(ctx, "warning", "All upstream connections were closed, restart worker process now");
      rpm_worker_destroy(ctx, rpm, worker);
    }
  }
}

static void rpm_worker_log_read(RedisModuleCtx *ctx, int fd, void *client_data, int mask, const char *level) {
  int rd;
  worker_pipe *pipe = client_data;
  worker_process *worker = pipe->worker;
  char *buf = worker->buf, *start, *end, *linefeed = NULL;
  REDISMODULE_NOT_USED(mask);
  while ((rd = read(fd, buf, sizeof(worker->buf))) > 0) {
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

static void rpm_worker_close_pipe(RedisModuleCtx *ctx, worker_pipe *pipe)
{
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

static void rpm_worker_suicide(RedisModuleCtx *ctx, worker_process *worker) {
  int32_t idx;
  for (idx = 0; idx < worker->num_upstreams; ++idx) {
    rpm_worker_close_pipe(ctx, worker->upstreams + idx);
  }
  for (idx = 0; idx < worker->num_downstreams; ++idx) {
    if (worker->downstreams[idx] != 0) {
      close(worker->downstreams[idx]);
      worker->downstreams[idx] = 0;
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

static void rpm_worker_process_start(worker_process *worker, char **argv) {
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
  fprintf(stdout, "worker process %lld is spawned and prepare to serve requests\n", (long long) getpid());
  fflush(stdout);
  execvp(argv[0], argv);
  exit(1);
}

static int rpm_worker_pipe_init(RedisModuleCtx *ctx, worker_process *worker, worker_pipe *p, RedisModuleFileProc read, int spair) {
  if (spair) {
    if (socketpair(AF_UNIX,SOCK_STREAM, 0, p->pipe) < 0) {
      return REDISMODULE_ERR;
    }
  } else {
    if (pipe(p->pipe) != 0) {
      return REDISMODULE_ERR;
    }
  }
  RedisModule_EnableNonBlock(ctx, p->pipe[0]);
  rpm_worker_pipe_buffer_reset(p);
  p->worker = worker;

  if (read != NULL) {
    return RedisModule_CreateFileEvent(ctx, p->pipe[0], REDISMODULE_READ, read, p);
  }
  return REDISMODULE_OK;
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

static void rpm_worker_destroy(RedisModuleCtx *ctx, rpm_context *rpm, worker_process *worker) {
  /* send worker dead error to pending requests of current worker */
  int64_t request_id;
  size_t ignored;
  int32_t idx;

  RedisModuleBlockedClient *bc = NULL;
  redis_response_reader *reader;

  hash_map_iterator *iterator = hash_map_iterator_create(worker->request_id_to_client);
  while (hash_map_iterator_next(iterator) != 0) {
    hash_map_iterator_get(iterator, (const void **) &request_id, &ignored, (const void **) &bc, &ignored);
    RedisModule_UnblockClient(bc, (void *) RPM_BAD_GATEWAY);
  }
  hash_map_iterator_destroy(iterator);
  hash_map_destroy(worker->request_id_to_client);
  hash_map_destroy(worker->client_id_to_request_id);
  if (worker->downstreams != NULL) {
    for (idx = 0; idx < worker->num_downstreams; ++idx) {
      if (worker->downstreams[idx] != 0) {
        close(worker->downstreams[idx]);
        worker->downstreams[idx] = 0;
      }
    }
    RedisModule_Free(worker->downstreams);
  }
  if (worker->upstreams != NULL) {
    for (idx = 0; idx < worker->num_upstreams; ++idx) {
      rpm_worker_close_pipe(ctx, worker->upstreams + idx);
    }
    RedisModule_Free(worker->upstreams);
  }
  if (worker->readers != NULL) {
    for (idx = 0; idx < worker->num_upstreams; ++idx) {
      if ((reader = worker->readers[idx]) != NULL) {
        redis_response_reader_free(reader);
      }
    }
    RedisModule_Free(worker->readers);
  }
  rpm_worker_close_pipe(ctx, &worker->out);
  rpm_worker_close_pipe(ctx, &worker->err);
  RedisModule_Log(ctx, "warning", "worker process is terminated prematurely, a new worker is being spawned");
  /* failed to create worker during initialization, setup a timer to try it again 10 ms later */
  RedisModule_CreateTimeEvent(ctx, 10, rpm_worker_create_later, rpm, NULL, &rpm->restart_timer);
  rpm->restart_count--;
  RedisModule_Free(worker);
  if (++rpm->restart_count >= rpm->retry_attempts) {
    RedisModule_Log(ctx, "warning", "worker has been stopped %d times within last %d seconds, shutdown redis automatically", rpm->restart_count, rpm->retry_timeout);
    rpm_context_shutdown_redis(ctx, rpm);
  }
  rpm->worker = NULL;
}

static worker_process *rpm_worker_create(RedisModuleCtx *ctx, rpm_context *rpm) {
  int32_t idx;
  int status;
  time_t now;
  pid_t pid;
  worker_process *worker = RedisModule_Calloc(1, sizeof(worker_process));
  worker->request_id_to_client = hash_map_create(&int_key_map, 1024);
  worker->client_id_to_request_id = hash_map_create(&int_key_map, 1024);
  worker->num_downstreams = rpm->num_downstreams;
  worker->num_upstreams = rpm->num_upstreams;
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
  if ((worker->readers = RedisModule_Calloc(1, sizeof(worker->readers[0]) * worker->num_upstreams)) == NULL) {
    RedisModule_Log(ctx, "warning", "Could not allocate enough memory for response reader, restart worker again");
    goto cleanup_exit;
  }
  if ((worker->upstreams = RedisModule_Calloc(1, sizeof(worker->upstreams[0]) * worker->num_upstreams)) == NULL) {
    RedisModule_Log(ctx, "warning", "Could not allocate enough memory for response reader, restart worker again");
    goto cleanup_exit;
  }
  for (idx = 0; idx < worker->num_upstreams; ++idx) {
    worker->readers[idx] = redis_response_reader_create(rpm->allocator, 1024 * 1024);
    if (rpm_worker_pipe_init(ctx, worker, worker->upstreams + idx, rpm_worker_upstream_read, 1)) {
      RedisModule_Log(ctx, "warning", "Could not initiate upstream pipe %d, restart worker again", idx);
      goto cleanup_exit;
    }
    worker->upstreams[idx].user_data = worker->readers[idx];
  }
  worker->active_upstreams = worker->num_upstreams;
  if (rpm_worker_pipe_init(ctx, worker, &worker->out, rpm_worker_stdout_read, 0) ||
      rpm_worker_pipe_init(ctx, worker, &worker->err, rpm_worker_stderr_read, 0)) {
    RedisModule_Log(ctx, "warning", "Could not initiate stdout and stderr pipe, restart worker again");
    goto cleanup_exit;
  }

  time(&now);
  if (now >= rpm->restart_count_reset_time && (now - rpm->restart_count_reset_time) > rpm->retry_timeout) {
    rpm->restart_count_reset_time = now;
    rpm->restart_count = 0;
  }

  if ((pid = fork()) == 0) {
    rpm_worker_process_start(worker, rpm->argv);
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
    rpm_worker_suicide(ctx, rpm->worker);
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
    rpm->argv[didx] = RedisModule_Calloc(1, len + 1);
    memcpy(rpm->argv[didx], arg, len + 1);
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
