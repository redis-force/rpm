#include "rpm_chained_buffer.h"
#include <string.h>

#define OFFSET_OF(TYPE, FIELD) (((size_t)(&((TYPE *) sizeof(TYPE))->FIELD)) - sizeof(TYPE))
#define CHAINED_BUFFER_ALIGN(size, align) (((size) + (align) - 1) & ~((align) - 1))
#define CHAINED_BUFFER_SIZE(size) CHAINED_BUFFER_ALIGN(size + OFFSET_OF(chained_buffer, buffer), 8)

void *chained_buffer_allocate(chained_buffer *buffer, size_t size) {
  void *result = buffer->buffer + buffer->size;
  buffer->size += size;
  return result;
}

void chained_buffer_destroy(chained_buffer **buffer_address, size_t limit) {
  chained_buffer *next;
  chained_buffer *buffer = *buffer_address;
  while (buffer && limit > 0) {
    next = buffer->next;
    allocator_free(buffer->allocator, buffer);
    buffer = next;
    --limit;
  }
  *buffer_address = buffer;
}

chained_buffer *chained_buffer_create(allocator *allocator, int32_t size)
{
  int32_t allocation_size = CHAINED_BUFFER_SIZE(size);
  int32_t capacity = allocation_size - OFFSET_OF(chained_buffer, buffer);
  chained_buffer *new_buffer = allocator_malloc(allocator, allocation_size);
  memset(new_buffer, 0, OFFSET_OF(chained_buffer, buffer));
  new_buffer->capacity = capacity;
  new_buffer->offset = 0;
  new_buffer->allocator = allocator != NULL ? allocator : libc_allocator;
  return new_buffer;
}

void chained_buffer_chain(chained_buffer **head, chained_buffer **current,
    chained_buffer ***next_address, int32_t *num_buffers, chained_buffer *next) {
  *current = next;
  if (!*head) {
    *head = *current;
  }
  (*num_buffers)++;
  **next_address = next;
  *next_address = &next->next;
}

chained_buffer *chained_buffer_check(chained_buffer **head, chained_buffer **current,
    chained_buffer ***next_address, int32_t *num_buffers, size_t required, int32_t chunk_size) {
  chained_buffer *current_buffer = *current;
  allocator *allocator = libc_allocator;
  if (!current_buffer || chained_buffer_available(current_buffer) < (int32_t)(required)) {
    if ((int32_t) required < chunk_size) {
      required = chunk_size;
    }
    if (current_buffer != NULL) {
      allocator = current_buffer->allocator;
    }
    chained_buffer_chain(head, current, next_address, num_buffers, current_buffer = chained_buffer_create(allocator, required));
  }
  return current_buffer;
}
