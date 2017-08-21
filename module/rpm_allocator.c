#include "rpm_allocator.h"
#include "rpm_chained_buffer.h"
#include <string.h>

#define MIN_CHUNK_SIZE (1024)

typedef void *(*allocator_malloc_fn)(void *, size_t);
typedef void *(*allocator_realloc_fn)(void *, void *, size_t);
typedef void (*allocator_free_fn)(void *, void *);
typedef void (*allocator_destroy_fn)(void *);

typedef struct allocator {
  allocator_malloc_fn malloc;
  allocator_realloc_fn realloc;
  allocator_malloc_fn calloc;
  allocator_free_fn free;
  allocator_destroy_fn destroy;
  void *impl;
} allocator;

void *allocator_malloc(allocator *allocator, size_t size) {
  return allocator->malloc(allocator->impl, size);
}

void *allocator_calloc(allocator *allocator, size_t size) {
  return allocator->calloc(allocator->impl, size);
}

void *allocator_realloc(allocator *allocator, void *ptr, size_t size) {
  return allocator->realloc(allocator->impl, ptr, size);
}

void allocator_free(allocator *allocator, void *ptr) {
  allocator->free(allocator->impl, ptr);
}

void allocator_destroy(allocator *allocator) {
  allocator->destroy(allocator->impl);
  free(allocator);
}

typedef struct chained_buffer_allocator {
  chained_buffer *head;
  chained_buffer *current;
  chained_buffer **next;
  int32_t buffer_num;
  int32_t chunk_size;
} chained_buffer_allocator;

void *chained_buffer_allocator_malloc(void *impl, size_t size) {
  chained_buffer_allocator *allocator = impl;
  return chained_buffer_allocate(chained_buffer_check(&allocator->head, &allocator->current,
      &allocator->next, &allocator->buffer_num, size, allocator->chunk_size), size);
}

void *chained_buffer_allocator_calloc(void *impl, size_t size) {
  chained_buffer_allocator *allocator = impl;
  void *result = chained_buffer_allocate(chained_buffer_check(&allocator->head, &allocator->current,
      &allocator->next, &allocator->buffer_num, size, allocator->chunk_size), size);
  memset(result, 0, size);
  return result;
}

void *chained_buffer_allocator_realloc(void *impl, void *ptr, size_t size) {
  chained_buffer_allocator *allocator = impl;
  void *result = chained_buffer_allocate(chained_buffer_check(&allocator->head, &allocator->current,
      &allocator->next, &allocator->buffer_num, size, allocator->chunk_size), size);
  memcpy(result, ptr, size);
  return result;
}

void chained_buffer_allocator_free(void *impl, void *ptr) {
  (void) impl;
  (void) ptr;
  /* NO OP for chained buffer */
  return;
}

void chained_buffer_allocator_destroy(void *impl) {
  chained_buffer_allocator allocator = *(chained_buffer_allocator *)(impl);
  chained_buffer_destroy(&allocator.head, allocator.buffer_num);
}

allocator *allocator_create(void *(*amalloc)(void *, size_t), void *(*acalloc)(void *, size_t), 
    void *(*arealloc)(void *, void *, size_t), void (*afree)(void *, void *), 
    void (*adestroy)(void *), void *privdata) {
  allocator *allocator = calloc(1, sizeof(struct allocator));
  allocator->malloc = amalloc;
  allocator->calloc = acalloc;
  allocator->realloc = arealloc;
  allocator->free = afree;
  allocator->destroy = adestroy;
  allocator->impl = privdata;
  return allocator;
}

allocator *chained_buffer_allocator_create(int32_t chunk_size) {
  chained_buffer_allocator *allocator;
  chained_buffer *head = NULL;
  if (chunk_size < MIN_CHUNK_SIZE) {
    chunk_size = MIN_CHUNK_SIZE;
  }
  head = chained_buffer_create(libc_allocator, chunk_size + sizeof(chained_buffer_allocator));
  allocator = chained_buffer_allocate(head, sizeof(chained_buffer_allocator));
  allocator->head = head;
  allocator->current = head;
  allocator->next = &head->next;
  allocator->buffer_num = 1;
  allocator->chunk_size = chunk_size;
  return allocator_create(chained_buffer_allocator_malloc, chained_buffer_allocator_calloc,
    chained_buffer_allocator_realloc, chained_buffer_allocator_free, chained_buffer_allocator_destroy, allocator);
}

void *libc_malloc(void *impl, size_t sz) {
  (void) impl;
  return malloc(sz);
}

void *libc_calloc(void *impl, size_t sz) {
  (void) impl;
  return calloc(1, sz);
}

void *libc_realloc(void *impl, void *ptr, size_t sz) {
  (void) impl;
  return realloc(ptr, sz);
}

void libc_free(void *impl, void *ptr) {
  (void) impl;
  free(ptr);
}

void libc_destroy(void *impl) {
  (void) impl;
  /* NO OP */
}

allocator libc_allocator_impl = {libc_malloc, libc_realloc, libc_calloc, libc_free, libc_destroy, &libc_allocator_impl};

allocator *libc_allocator = &libc_allocator_impl;
