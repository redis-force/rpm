#ifndef __REDIS_MODULE_MANAGER_QUEUE__
#define __REDIS_MODULE_MANAGER_QUEUE__

#include "rpm_allocator.h"

typedef struct queue queue;
typedef struct queue_item queue_item;
typedef struct queue_pop_result queue_pop_result;

struct queue_pop_result {
  queue *queue;
  queue_item *head;
  queue_item *freelist;
  queue_item **freelist_tail;
};

queue *queue_create(allocator *allocator, size_t buffer_size);
void queue_destroy(queue *q);
void queue_push(queue *q, void *data);
void queue_pop(queue *q, queue_pop_result *result);
void queue_peek(queue *q, queue_pop_result *result);
void *queue_pop_result_next(queue_pop_result *result);

#endif /* __REDIS_MODULE_MANAGER_QUEUE__ */
