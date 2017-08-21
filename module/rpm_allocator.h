#ifndef __RPM_ALLOCATOR__
#define __RPM_ALLOCATOR__

#include <stdint.h>
#include <stdlib.h>

typedef struct allocator allocator;
void *allocator_malloc(allocator *allocator, size_t size);
void *allocator_calloc(allocator *allocator, size_t size);
void *allocator_realloc(allocator *allocator, void *ptr, size_t size);
void allocator_free(allocator *allocator, void *ptr);
void allocator_destroy(allocator *allocator);

allocator *chained_buffer_allocator_create(int32_t chunk_size);
extern allocator *libc_allocator;

allocator *allocator_create(void *(*malloc)(void *, size_t), void *(*calloc)(void *, size_t),
    void *(*realloc)(void *, void *, size_t), void (*free)(void *, void *),
    void (*destroy)(void *), void *privdata);

#endif /* __RPM_ALLOCATOR__ */
