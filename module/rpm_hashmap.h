#ifndef __REDIS_MODULE_MANAGER_HASHMAP__
#define __REDIS_MODULE_MANAGER_HASHMAP__

#include "rpm_allocator.h"

typedef struct hash_map hash_map;
typedef struct hash_map_iterator hash_map_iterator;

typedef uint32_t (*hash_algorithm)(hash_map *map, const void *data, size_t size);
typedef void *(*hash_map_dup_data)(hash_map *map, const void *data, size_t size);
typedef int32_t (*hash_map_cmp_data)(hash_map *map, const void *data1, size_t size1, const void *data2, size_t size2);
typedef void (*hash_map_destroy_data)(hash_map *map, void *data, size_t size);

typedef struct hash_map_type
{
  hash_algorithm algorithm;
  hash_map_dup_data dup_key;
  hash_map_dup_data dup_value;
  hash_map_cmp_data compare_key;
  hash_map_destroy_data destroy_key;
  hash_map_destroy_data destroy_value;
  allocator *allocator;
} hash_map_type;

uint8_t hash_map_set_if_absent(hash_map *mp, const void *key, size_t ksz, const void *val, size_t vsz);
int32_t hash_map_set(hash_map *mp, const void *key, size_t ksz, const void *val, size_t vsz);
int32_t hash_map_remove(hash_map *mp, const void *key, size_t ksz);
int32_t hash_map_remove_no_free(hash_map *mp, const void *key, size_t ksz);
int32_t hash_map_find(hash_map *mp, const void *key, size_t ksz, const void **val, size_t *vsz);
int32_t hash_map_get(hash_map *mp, const void *key, size_t ksz, void *buf, size_t *bufsz);
int32_t hash_map_clear(hash_map *mp);
size_t hash_map_count(hash_map *mp);

hash_map_iterator *hash_map_iterator_create(hash_map *mp);
hash_map_iterator *hash_map_safe_iterator_create(hash_map *mp);
void hash_map_iterator_get(hash_map_iterator *iterator, const void **key, size_t *ksz, const void **val, size_t *vsz);
uint8_t hash_map_iterator_next(hash_map_iterator *iterator);
void hash_map_iterator_destroy(hash_map_iterator *iterator);

hash_map *hash_map_create(const hash_map_type *type, size_t initial_size);
void hash_map_destroy(hash_map *mp);

void *hash_map_allocator_dup_data(hash_map *map, const void *data, size_t size);
void hash_map_allocator_destroy_data(hash_map *map, void *data, size_t size);
int32_t hash_map_bitwise_compare_data(hash_map *map, const void *data1, size_t size1, const void *data2, size_t size2);

#endif /* __REDIS_MODULE_MANAGER_HASHMAP__ */
