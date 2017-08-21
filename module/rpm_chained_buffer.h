#ifndef __RPM_CHAINED_BUFFER__
#define __RPM_CHAINED_BUFFER__
#include <stdint.h>
#include <stdlib.h>
#include "rpm_allocator.h"

typedef struct chained_buffer chained_buffer;

struct chained_buffer {
  chained_buffer *next;
  int32_t offset; /* read offset of the buffer */
  int32_t size;
  int32_t capacity;
  allocator *allocator;
  uint8_t buffer[sizeof(int32_t)];
};

#define chained_buffer_available(BUFFER) ((BUFFER)->capacity - (BUFFER)->size)
#define chained_buffer_size(BUFFER) ((BUFFER)->size)
#define chained_buffer_get(BUFFER, OFFSET) ((BUFFER)->buffer + (OFFSET))

void *chained_buffer_allocate(chained_buffer *buffer, size_t size);
void chained_buffer_destroy(chained_buffer **buffer_address, size_t limit);
chained_buffer *chained_buffer_create(allocator *allocator, int32_t size);
chained_buffer *chained_buffer_check(chained_buffer **head, chained_buffer **current,
    chained_buffer ***next_address, int32_t *num_buffers, size_t required, int32_t chunk_size);
void chained_buffer_chain(chained_buffer **head, chained_buffer **current,
    chained_buffer ***next_address, int32_t *num_buffers, chained_buffer *next);

#endif /* __RPM_CHAINED_BUFFER__ */
