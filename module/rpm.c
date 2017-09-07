#include "redismodule.h"
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
#include <sys/socket.h>

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

const char *RPM_BAD_GATEWAY = "-ERR Bad Gateway";
const char *RPM_REQUEST_TIMEOUT = "-ERR Request Timeout";
const int64_t RPM_DEFAULT_TIMEOUT = 1000;

typedef enum rpm_event_type {
  RPM_EVENT_WATCHDOG,
} rpm_event_type;

typedef struct rpm_context rpm_context;
typedef struct worker_process worker_process;
typedef struct worker_pipe worker_pipe;
typedef struct watchdog watchdog;
typedef struct rpm_event rpm_event;
typedef struct rpm_watchdog_event rpm_watchdog_event;

static void rpm_worker_destroy(RedisModuleCtx *ctx, worker_process *worker);
static worker_process *rpm_worker_create(RedisModuleCtx *ctx, rpm_context *rpm);
static void rpm_worker_suicide(RedisModuleCtx *ctx, worker_process *worker);

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

struct rpm_event {
  int32_t type;
  char payload[4];
};

struct rpm_watchdog_event {
  int32_t type;
  pid_t target;
  int stat;
};

struct rpm_context {
  char **argv;
  int64_t current_request_id;
  worker_process *worker;
  hash_map *command_timeout;
  int event_pipe[2];
  allocator *allocator;
  int32_t num_downstreams;
  int32_t num_upstreams;
};

struct watchdog {
  pid_t target;
  int notify;
};

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
  pid_t pid;
  worker_pipe out;
  worker_pipe err;
  redis_response_reader **readers;
  int32_t num_downstreams;
  int *downstreams;
  int32_t num_upstreams;
  worker_pipe *upstreams;
  hash_map *request_id_to_client;
  hash_map *client_id_to_request_id;
  pthread_t watchdog;
  char buf[1024];
};

static inline void rpm_event_submit(int fd, void *event) {
  uintptr_t ptr = (uintptr_t) event;
  write(fd, &ptr, sizeof(ptr));
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
      current->offset += wr;
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
    buf->offset += wr;
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
  REDISMODULE_NOT_USED(argv);
  REDISMODULE_NOT_USED(argc);
  if (data == RPM_BAD_GATEWAY) {
    RedisModule_ReplyWithError(ctx, RPM_BAD_GATEWAY);
  } else {
    redis_response *reply = data;
    rpm_worker_command_on_reply_item(ctx, reply->payload.array.array[1]);
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

static int rpm_worker_upstream_write_command(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  int idx;
  size_t len;
  char *tmp, client_id_buffer[32], request_id_buffer[32];
  chained_buffer *cmd;
  const char **args = RedisModule_Alloc((sizeof(char *) + sizeof(size_t)) * (argc + 2));
  size_t *num_args = (size_t *) (args + argc + 2);
  rpm_context *rpm = rpm_context_get(ctx);
  long long timeout = RPM_DEFAULT_TIMEOUT, client_id = RedisModule_GetClientId(ctx);
  worker_pipe *upstream = rpm->worker->upstreams + (client_id % rpm->worker->num_upstreams);
  if (rpm->command_timeout) {
    tmp = (char *) RedisModule_StringPtrLen(argv[0], &len);
    hash_map_find(rpm->command_timeout, tmp, len, (const void **) &timeout, &len);
  }
  args[0] = client_id_buffer;
  num_args[0] = snprintf(client_id_buffer, sizeof(client_id_buffer), "%lld", client_id);
  args[1] = request_id_buffer;
  num_args[1] = snprintf(request_id_buffer, sizeof(request_id_buffer), "%lld", rpm_worker_process_request(ctx, rpm, timeout, client_id));
  for (idx = 0; idx < argc; ++idx) {
    args[idx + 2] = RedisModule_StringPtrLen(argv[idx], num_args + idx + 2);
  }
  cmd = redis_format_command(rpm->allocator, argc + 2, args, num_args);
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
  RedisModule_Log(ctx, level, log_buffer);
}

static void rpm_worker_upstream_on_reply(RedisModuleCtx *ctx, redis_response *reply) {
  redis_response *first;
  rpm_context *rpm = rpm_context_get(ctx);
  assert(reply->type == REDIS_RESPONSE_ARRAY && reply->payload.array.length == 2);
  first = reply->payload.array.array[0];
  assert(first->type == REDIS_RESPONSE_INTEGER);
  if ((reply = rpm_worker_process_reply(rpm, RedisModule_GetClientId(ctx), first->payload.integer, reply)) != NULL) {
    redis_response_reader_free_response(reply);
  }
}

static void rpm_worker_upstream_read(RedisModuleCtx *ctx, int fd, void *client_data, int mask) {
  int rd;
  redis_response_status st;
  worker_pipe *pipe = client_data;
  worker_process *worker = pipe->worker;
  redis_response_reader *reader = pipe->user_data;
  char *buf = worker->buf;
  redis_response *reply = NULL;
  REDISMODULE_NOT_USED(mask);
  while ((rd = read(fd, buf, sizeof(worker->buf))) > 0) {
    redis_response_reader_feed(reader, buf, rd);
    while ((st = redis_response_reader_next(reader, &reply)) == REDIS_RESPONSE_OK && reply != NULL) {
      rpm_worker_upstream_on_reply(ctx, reply);
    }
    if (st != REDIS_RESPONSE_OK) {
      /* protocol error, kill worker and restart to fix the channel */
      RedisModule_Log(ctx, "warning", "worker process %lld respond invalid response and will be restarted to fix the state. %s",
          worker->pid, redis_response_reader_strerr(reader));
      rpm_worker_suicide(ctx, worker);
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
    RedisModule_DeleteFileEvent(ctx, pipe->pipe[0], REDISMODULE_READ);
    if (pipe->write_buffer != NULL) {
      RedisModule_DeleteFileEvent(ctx, pipe->pipe[1], REDISMODULE_WRITE);
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
  kill(worker->pid, SIGTERM);
}

static void rpm_event_free(RedisModuleCtx *ctx, rpm_event *event, void *client_data) {
  REDISMODULE_NOT_USED(ctx);
  REDISMODULE_NOT_USED(client_data);
  RedisModule_Free(event);
}

static void rpm_event_dispatch(RedisModuleCtx *ctx, rpm_event *event, void *client_data) {
  rpm_context *rpm = rpm_context_get(ctx);
  switch (event->type) {
    case RPM_EVENT_WATCHDOG:
      rpm_worker_destroy(ctx, rpm->worker);
      rpm->worker = rpm_worker_create(ctx, rpm);
      break;
    default:
      return;
  }
  rpm_event_free(ctx, event, client_data);
}

static void rpm_event_drain(RedisModuleCtx *ctx, int fd, void (*callback)(RedisModuleCtx *, rpm_event *, void *), void *client_data) {
  uintptr_t events[16];
  int rd, idx;
  while ((rd = read(fd, &events, sizeof(events))) > 0) {
    rd /= sizeof(uintptr_t);
    for (idx = 0; idx < rd; ++idx) {
      callback(ctx, (rpm_event *) events[idx], client_data);
    }
  }
}

static void rpm_event_read(RedisModuleCtx *ctx, int fd, void *client_data, int mask) {
  REDISMODULE_NOT_USED(mask);
  rpm_event_drain(ctx, fd, rpm_event_dispatch, client_data);
}

static inline void rpm_worker_redirect_fd(int from, int to) {
  if (from == to) {
    return;
  }
  dup2(from, to);
  close(from);
}

static void rpm_worker_process_start(worker_process *worker, char **argv) {
  long idx, cidx, closefd, max = sysconf(_SC_OPEN_MAX);
  char buff[32], envbuff[32 + sizeof(ENV_RPM_DOWNSTREAM_FD_PREFIX) + sizeof(ENV_RPM_UPSTREAM_FD_PREFIX)];
  for (idx = 1; idx < max; ++idx) {
    closefd = 1;
    if (idx == worker->out.pipe[1] || idx == worker->err.pipe[1]) {
      closefd = 0;
    }
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

static void* rpm_worker_watchdog(void *arg) {
  watchdog *watch = arg;
  rpm_watchdog_event *event = RedisModule_Alloc(sizeof(rpm_watchdog_event));
  event->type = RPM_EVENT_WATCHDOG;
  event->target = waitpid(watch->target, &event->stat, 0);
  rpm_event_submit(watch->notify, event);
  RedisModule_Free(watch);
  return NULL;
}

static void rpm_worker_destroy(RedisModuleCtx *ctx, worker_process *worker) {
  void *tmp;
  /* send worker dead error to pending requests of current worker */
  int64_t request_id;
  size_t ignored;
  int32_t idx;
  RedisModuleBlockedClient *bc = NULL;
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
      redis_response_reader_free(worker->readers[idx]);
    }
    RedisModule_Free(worker->readers);
  }
  rpm_worker_close_pipe(ctx, &worker->out);
  rpm_worker_close_pipe(ctx, &worker->err);
  pthread_join(worker->watchdog, &tmp);
  RedisModule_Log(ctx, "warning", "worker process %lld is terminated prematurely, a new worker is being spawned", worker->pid);
  RedisModule_Free(worker);
}

static worker_process *rpm_worker_create(RedisModuleCtx *ctx, rpm_context *rpm) {
  int32_t idx;
  worker_process *worker = RedisModule_Calloc(1, sizeof(worker_process));
  worker->request_id_to_client = hash_map_create(&int_key_map, 1024);
  worker->client_id_to_request_id = hash_map_create(&int_key_map, 1024);
  worker->num_downstreams = rpm->num_downstreams;
  worker->num_upstreams = rpm->num_upstreams;
  if ((worker->downstreams = RedisModule_Calloc(1, sizeof(worker->downstreams[0]) * worker->num_downstreams)) == NULL) {
    rpm_worker_destroy(ctx, worker);
    return NULL;
  }
  for (idx = 0; idx < worker->num_downstreams; ++idx) {
    if (RedisModule_CreateClient(ctx, worker->downstreams + idx) != REDISMODULE_OK) {
      rpm_worker_destroy(ctx, worker);
      return NULL;
    }
  }
  if ((worker->readers = RedisModule_Calloc(1, sizeof(worker->readers[0]) * worker->num_upstreams)) == NULL) {
    rpm_worker_destroy(ctx, worker);
    return NULL;
  }
  if ((worker->upstreams = RedisModule_Calloc(1, sizeof(worker->upstreams[0]) * worker->num_upstreams)) == NULL) {
    rpm_worker_destroy(ctx, worker);
    return NULL;
  }
  for (idx = 0; idx < worker->num_upstreams; ++idx) {
    worker->readers[idx] = redis_response_reader_create(rpm->allocator, 1024 * 1024);
    if (rpm_worker_pipe_init(ctx, worker, worker->upstreams + idx, rpm_worker_upstream_read, 1)) {
      rpm_worker_destroy(ctx, worker);
      return NULL;
    }
    worker->upstreams[idx].user_data = worker->readers[idx];
  }
  if (rpm_worker_pipe_init(ctx, worker, &worker->out, rpm_worker_stdout_read, 0) ||
      rpm_worker_pipe_init(ctx, worker, &worker->err, rpm_worker_stderr_read, 0)) {
    rpm_worker_destroy(ctx, worker);
    return NULL;
  }
  if ((worker->pid = fork()) == 0) {
    rpm_worker_process_start(worker, rpm->argv);
  } else {
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
    setpgid(worker->pid, 0);
    watchdog *watchdog = RedisModule_Alloc(sizeof(watchdog));
    watchdog->target = worker->pid;
    watchdog->notify = rpm->event_pipe[1];
    pthread_create(&worker->watchdog, NULL, rpm_worker_watchdog, watchdog);
    RedisModule_Log(ctx, "notice", "worker process %lld is spawned and prepare to serve requests", worker->pid);
  }
  return worker;
}

static void rpm_context_destroy(RedisModuleCtx *ctx, rpm_context *rpm) {
  char **argv;
  if (rpm->worker != NULL) {
    rpm_worker_suicide(ctx, rpm->worker);
    rpm_worker_destroy(ctx, rpm->worker);
  }
  if (rpm->command_timeout != NULL) {
    hash_map_destroy(rpm->command_timeout);
  }
  RedisModule_DeleteFileEvent(ctx, rpm->event_pipe[0], REDISMODULE_READ);
  rpm_event_drain(ctx, rpm->event_pipe[0], rpm_event_free, NULL);
  close(rpm->event_pipe[0]);
  close(rpm->event_pipe[1]);
  allocator_destroy(rpm->allocator);
  for (argv = rpm->argv; *argv != NULL; ++argv) {
    RedisModule_Free(*argv);
  }
  RedisModule_Free(rpm->argv);
  RedisModule_Free(rpm);
}

static rpm_context *rpm_context_create(RedisModuleCtx *ctx, int argc, RedisModuleString **argv)
{
  int idx, didx, last_is_command = 0;
  int64_t timeout;
  size_t len;
  const char *arg;
  rpm_context *rpm = RedisModule_Calloc(1, sizeof(rpm_context));
  hash_map *command_timeout = NULL;
  rpm->allocator = allocator_create(rpm_allocator_malloc, rpm_allocator_calloc,
    rpm_allocator_realloc, rpm_allocator_free, rpm_allocator_destroy, rpm);
  for (idx = 0; idx + 1 < argc; ++idx) {
    arg = RedisModule_StringPtrLen(argv[idx], &len);
    if (strcasecmp("--num-downstreams", arg) == 0) {
      /* consume the next argument */
      arg = RedisModule_StringPtrLen(argv[++idx], &len);
      rpm->num_downstreams = strtol(arg, NULL, 10);
      last_is_command = 0;
    } else if (strcasecmp("--num-upstreams", arg) == 0) {
      /* consume the next argument */
      arg = RedisModule_StringPtrLen(argv[++idx], &len);
      rpm->num_upstreams = strtol(arg, NULL, 10);
      last_is_command = 0;
    } else if (strcasecmp("--command", arg) == 0) {
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

  if (pipe(rpm->event_pipe) != 0) {
    goto cleanup_exit;
  }
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
  RedisModule_EnableNonBlock(ctx, rpm->event_pipe[0]);
  RedisModule_CreateFileEvent(ctx, rpm->event_pipe[0], REDISMODULE_READ, rpm_event_read, rpm);
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
