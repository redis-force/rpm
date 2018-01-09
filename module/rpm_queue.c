#include "rpm_queue.h"
#include <pthread.h>
#include <stdio.h>

#define QUEUE_ITEM_FLAGS_PREALLOCATED (1)

struct queue_item {
  queue_item *next;
  int32_t flags;
  void *item;
};

struct queue {
  allocator *allocator;
  pthread_mutex_t mutex;
  pthread_cond_t cond;
  queue_item *volatile head;
  queue_item *volatile *volatile tail;
  pthread_mutex_t freelist_mutex;
  queue_item *freelist;
  queue_item item_buffer[1];
};

static queue_item *queue_allocate_item(queue *q, void *item) {
  queue_item *qitem = NULL;
  if (pthread_mutex_trylock(&q->freelist_mutex) == 0) {
    if ((qitem = q->freelist) != NULL) {
      q->freelist = qitem->next;
    }
    pthread_mutex_unlock(&q->freelist_mutex);
  }
  if (qitem == NULL) {
    qitem = allocator_calloc(q->allocator, sizeof(queue_item));
  }
  qitem->item = item;
  qitem->next = NULL;
  return qitem;
}

queue* queue_create(allocator *allocator, size_t buffer_size) {
  size_t idx;
  queue_item *item, *next;
  queue *q;
  buffer_size = buffer_size > 0 ? buffer_size : 1;
  q = allocator_calloc(allocator, sizeof(queue) + (buffer_size * sizeof(queue_item)));
  q->allocator = allocator;
  q->tail = &q->head;
  pthread_mutex_init(&q->mutex, NULL);
  pthread_cond_init(&q->cond, NULL);
  pthread_mutex_init(&q->freelist_mutex, NULL);
  for (idx = 0; idx < buffer_size; ++idx) {
    item = q->item_buffer + idx;
    next = item + 1;
    item->next = next;
    item->flags = QUEUE_ITEM_FLAGS_PREALLOCATED;
  }
  item->next = NULL;
  q->freelist = q->item_buffer;
  return q;
}

void queue_destroy(queue *q) {
  allocator *allocator = q->allocator;
  pthread_mutex_destroy(&q->freelist_mutex);
  pthread_cond_destroy(&q->cond);
  pthread_mutex_destroy(&q->mutex);
  allocator_free(allocator, q);
}

void queue_push(queue *q, void *item) {
  queue_item *qitem = queue_allocate_item(q, item);
  pthread_mutex_lock(&q->mutex);
  *q->tail = qitem;
  q->tail = &qitem->next;
  pthread_cond_signal(&q->cond);
  pthread_mutex_unlock(&q->mutex);
}

static queue_item *do_queue_pop(queue *q, int32_t blocking) {
  queue_item *item;
  
  pthread_mutex_lock(&q->mutex);
  while (blocking && q->head == NULL) {
    pthread_cond_wait(&q->cond, &q->mutex);
  }
  item = q->head;
  q->head = NULL;
  q->tail = &q->head;
  pthread_mutex_unlock(&q->mutex);
  return item;
}

static void queue_peek_or_pop(queue *q, queue_pop_result *result, int32_t blocking) {
  result->queue = q;
  result->head = do_queue_pop(q, blocking);
  result->freelist = NULL;
  result->freelist_tail = &result->freelist;
}

void queue_pop(queue *q, queue_pop_result *result) {
  return queue_peek_or_pop(q, result, 1);
}

void queue_peek(queue *q, queue_pop_result *result) {
  return queue_peek_or_pop(q, result, 0);
}

static void queue_pop_result_clean(queue_pop_result *result) {
  if (result->freelist != NULL) {
    pthread_mutex_lock(&result->queue->freelist_mutex);
    *result->freelist_tail = result->queue->freelist;
    result->queue->freelist = result->freelist;
    pthread_mutex_unlock(&result->queue->freelist_mutex);
    result->freelist = NULL;
    result->freelist_tail = NULL;
  }
}

void *queue_pop_result_next(queue_pop_result *result) {
  queue_item *qitem = result->head;
  void *item = NULL;
  if (qitem != NULL) {
    result->head = qitem->next;
    item = qitem->item;
    if (qitem->flags & QUEUE_ITEM_FLAGS_PREALLOCATED) {
      *result->freelist_tail = qitem;
      qitem->next = NULL;
      result->freelist_tail = &qitem->next;
    } else {
      allocator_free(result->queue->allocator, qitem);
    }
  } else {
    queue_pop_result_clean(result);
  }
  return item;
}
