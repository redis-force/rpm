#include "rpm_hashmap.h"

#include <string.h>
#include <limits.h>
#include <stdio.h>

#define DEFAULT_HASH_MAP_INITIAL_SIZE 15

#define HASH_MAP_IS_REHASHING(hm) ((hm)->rehash_index != -1)
#define HASH_MAP_COUNT(hm) ((hm)->tables[0].used + (hm)->tables[1].used)
#define HASH_MAP_ENTRY_FREE_KEY(hm, entry)                        \
  if ((hm)->type.destroy_key) {                                   \
    (hm)->type.destroy_key(hm, (entry)->key, (entry)->ksize);     \
  }
#define HASH_MAP_ENTRY_FREE_VALUE(hm, entry)                      \
  if ((hm)->type.destroy_value) {                                 \
    (hm)->type.destroy_value(hm, (entry)->value, (entry)->vsize); \
  }
#define HASH_MAP_HASH_CODE(hm, key, ksiz) (hm)->type.algorithm(hm, key, ksiz)

#define HASH_MAP_COMPARE_KEYS(hm, key1, ksiz1, key2, ksiz2) \
  (((hm)->type.compare_key) ? (hm)->type.compare_key(hm, key1, ksiz1, key2, ksiz2) == 0 : (key1) == (key2))

#define HASH_MAP_ENTRY_SET_KEY(hm, entry, entry_key, key_size) \
  do {                                                         \
    hash_map_dup_data dup = (hm)->type.dup_key;                \
    if (dup) {                                                 \
      entry->key = dup(hm, entry_key, key_size);               \
    } else {                                                   \
      entry->key = (void *)(entry_key);                        \
    }                                                          \
    entry->ksize = key_size;                                   \
  } while(0)

#define HASH_MAP_ENTRY_SET_VALUE(hm, entry, entry_value, value_size) \
  do {                                                               \
    hash_map_dup_data dup = (hm)->type.dup_value;                    \
    if (dup) {                                                       \
      entry->value = dup(hm, entry_value, value_size);               \
    } else {                                                         \
      entry->value = (void *)(entry_value);                          \
    }                                                                \
    entry->vsize = value_size;                                       \
  } while(0)

#define HASH_MAP_MALLOC(mp, size) allocator_malloc((mp)->type.allocator, size)
#define HASH_MAP_CALLOC(mp, size) allocator_calloc((mp)->type.allocator, size)
#define HASH_MAP_FREE(mp, mem) allocator_free((mp)->type.allocator, mem)

typedef struct hash_map_entry {
  void *key;
  size_t ksize;
  void *value;
  size_t vsize;
  struct hash_map_entry *next;
} hash_map_entry;

typedef struct hash_table {
  hash_map_entry **buckets;
  size_t size;
  size_t size_mask;
  size_t used;
} hash_table;

struct hash_map {
  hash_map_type type;
  int32_t options;
  size_t initial_size;
  hash_table tables[2];
  int64_t rehash_index; /* this can make sure rehash number never overflow unless we can really use up whole 64bits address space */
  size_t iterators;
};

struct hash_map_iterator {
  hash_map *map;
  size_t table;
  int64_t index;
  hash_map_entry *current;
  hash_map_entry *next;
  uint8_t safe;
};

/* implementation details */
static void hash_table_reset(hash_table *table);
static int32_t hash_table_clear(hash_map *map, hash_table *table);
static size_t hash_map_key_index(hash_map *map, const void *key, size_t ksiz, uint8_t *found);
static void hash_map_rehash_step(hash_map *map, size_t steps);
static hash_map_entry *hash_map_find_entry(hash_map *map, const void *key, size_t ksiz);
static int32_t hash_map_check_and_expand(hash_map *map);
static int32_t hash_map_expand(hash_map *map, size_t size);
static int32_t hash_map_remove_internal(hash_map *map, const void *key, size_t ksiz, uint8_t call_free);
static size_t hash_map_expand_get_aligned_size(hash_map *map, size_t size);

uint32_t rs_hash(hash_map *mp, const void *in, size_t len)
{
  (void) mp;
  uint32_t b = 378551;
  uint32_t a = 63689;
  uint32_t hash = 0;
  uint32_t i = 0;
  uint8_t *input = (uint8_t *)in;

  for (i = 0; i < len; input++, i++) {
    hash = hash * a + (*input);
    a = a * b;
  }

  return hash;
}

static hash_map_type hash_map_type_default =
{
  rs_hash, NULL, NULL, NULL, NULL, NULL, NULL
};

hash_map *hash_map_create(const hash_map_type *type, size_t initial_size)
{
  hash_map *hm;
  allocator *allocator = NULL;

  if (type == NULL) {
    type = &hash_map_type_default;
  }
  allocator = type->allocator ? type->allocator : libc_allocator;

  hm = allocator_calloc(allocator, sizeof(hash_map));

  if (initial_size <= DEFAULT_HASH_MAP_INITIAL_SIZE) {
    initial_size = DEFAULT_HASH_MAP_INITIAL_SIZE;
  }

  hash_table_reset(&hm->tables[0]);
  hash_table_reset(&hm->tables[1]);

  memcpy(&hm->type, type, sizeof(hash_map_type));
  hm->type.allocator = allocator;
  if (type->algorithm == NULL) {
    hm->type.algorithm = rs_hash;
  }
  hm->initial_size = initial_size;
  hm->rehash_index = -1;
  hm->iterators = 0;

  return hm;
}

void hash_map_destroy(hash_map *map)
{
  hash_map_clear(map);
  HASH_MAP_FREE(map, map);
}

static void
hash_table_reset(hash_table *table)
{
  table->buckets = NULL;
  table->size = 0;
  table->size_mask = 0;
  table->used = 0;
}

uint8_t hash_map_set_if_absent(hash_map *map, const void *key, size_t ksiz, const void *val, size_t vsiz)
{
  size_t index;
  uint8_t found;
  hash_map_entry *entry;
  hash_table *table;

  if (HASH_MAP_IS_REHASHING(map)) {
    hash_map_rehash_step(map, 1);
  }

  index = hash_map_key_index(map, key, ksiz, &found);
  if (found == 1) {
    return 0;
  }

  table = HASH_MAP_IS_REHASHING(map) ? &map->tables[1] : &map->tables[0];

  entry = HASH_MAP_MALLOC(map, sizeof(hash_map_entry));
  entry->next = table->buckets[index];
  table->buckets[index] = entry;
  ++(table->used);

  HASH_MAP_ENTRY_SET_KEY(map, entry, key, ksiz);
  HASH_MAP_ENTRY_SET_VALUE(map, entry, val, vsiz);

  return 1;
}

int32_t hash_map_set(hash_map *map, const void *key, size_t ksiz, const void *val, size_t vsiz)
{
  hash_map_entry *entry, saved_entry;
  if (hash_map_set_if_absent(map, key, ksiz, val, vsiz) == 0) {
    return 0;
  }

  entry = hash_map_find_entry(map, key, ksiz);
  saved_entry = *entry;
  HASH_MAP_ENTRY_SET_VALUE(map, entry, val, vsiz);
  HASH_MAP_ENTRY_FREE_VALUE(map, &saved_entry);
  return 0;
}

static int32_t
hash_map_remove_internal(hash_map *map, const void *key, size_t ksiz, uint8_t call_free)
{
  size_t hash, index, table;
  hash_map_entry *entry, *prev;
  hash_table *hash_table;

  if (map->tables[0].size == 0) {
    return -1;
  }
  if (HASH_MAP_IS_REHASHING(map)) {
    hash_map_rehash_step(map, 1);
  }

  hash = HASH_MAP_HASH_CODE(map, key, ksiz);
  for (table = 0; table <= 1; ++table) {
    hash_table = &map->tables[table];
    index = hash & hash_table->size_mask;
    entry = hash_table->buckets[index];
    prev = NULL;
    while (entry) {
      if (HASH_MAP_COMPARE_KEYS(map, entry->key, entry->ksize, key, ksiz)) {
        if (prev) {
          prev->next = entry->next;
        } else {
          hash_table->buckets[index] = entry->next;
        }

        if (call_free) {
          HASH_MAP_ENTRY_FREE_KEY(map, entry);
          HASH_MAP_ENTRY_FREE_VALUE(map, entry);
        }
        HASH_MAP_FREE(map, entry);
        hash_table->used--;
        return 0;
      }
      prev = entry;
      entry = entry->next;
    }
    if (!HASH_MAP_IS_REHASHING(map)) {
      break;
    }
  }

  return -1;
}

int32_t hash_map_remove(hash_map *map, const void *key, size_t ksiz)
{
  return hash_map_remove_internal(map, key, ksiz, 1);
}

int32_t hash_map_remove_no_free(hash_map *map, const void *key, size_t ksiz)
{
  return hash_map_remove_internal(map, key, ksiz, 0);
}

int32_t hash_map_find(hash_map *map, const void *key, size_t ksiz, const void **val, size_t *vsiz)
{
  hash_map_entry *entry;

  entry = hash_map_find_entry(map, key, ksiz);
  if (entry) {
    *val = entry->value;
    *vsiz = entry->vsize;
    return 0;
  } else {
    return -1;
  }
}

int32_t hash_map_get(hash_map *map, const void *key, size_t ksiz, void *buff, size_t *bufsiz)
{
  const void *val = NULL;
  size_t vsiz = 0;
  int32_t st = 0;

  st = hash_map_find(map, key, ksiz, &val, &vsiz);
  if (st != 0) {
    return st;
  }
  if (*bufsiz < vsiz) {
    return -1;
  }
  memcpy(buff, val, vsiz);
  *bufsiz = vsiz;
  return 0;
}

int32_t hash_map_clear(hash_map *map)
{
  hash_table_clear(map, &map->tables[0]);
  hash_table_clear(map, &map->tables[1]);
  map->rehash_index = -1;
  map->iterators = 0;
  return 0;
}

size_t hash_map_count(hash_map *map)
{
  return HASH_MAP_COUNT(map);
}

int32_t hash_table_clear(hash_map *map, hash_table *table)
{
  size_t idx;
  hash_map_entry *entry = NULL;
  hash_map_entry *next = NULL;

  for (idx = 0; idx < table->size && table->used > 0; ++idx) {
    if ((entry = table->buckets[idx]) == NULL) {
      continue;
    }
    while (entry) {
      next = entry->next;
      HASH_MAP_ENTRY_FREE_KEY(map, entry)
      HASH_MAP_ENTRY_FREE_VALUE(map, entry)
      HASH_MAP_FREE(map, entry);
      --(table->used);
      entry = next;
    }
  }
  HASH_MAP_FREE(map, table->buckets);
  hash_table_reset(table);

  return 0;
}

static size_t hash_map_key_index(hash_map *map, const void *key, size_t ksiz, uint8_t *found)
{
  int32_t st = 0;
  size_t hash, idx, table;
  hash_map_entry *entry;
  hash_table *hash_table;
  *found = 0;

  if ((st = hash_map_check_and_expand(map)) != 0) {
    return st;
  }

  hash = HASH_MAP_HASH_CODE(map, key, ksiz);
  for (table = 0; table <= 1; ++table) {
    hash_table = &map->tables[table];
    idx = hash & hash_table->size_mask;
    /* Search if this slot does not already contain the given key */
    entry = hash_table->buckets[idx];
    while (entry) {
      if (HASH_MAP_COMPARE_KEYS(map, key, ksiz, entry->key, entry->ksize)) {
        *found = 1;
        break;
      }
      entry = entry->next;
    }
    if (!HASH_MAP_IS_REHASHING(map)) {
      break;
    }
  }
  return idx;
}

static void hash_map_rehash_step(hash_map *map, size_t steps)
{
  hash_map_entry *entry, *next_entry;
  hash_table *curr, *next;
  int64_t rehash_index;
  size_t size_mask, hash, curr_used, next_used;

  /* we can't rehash at this time as there is an open iterator */
  if (map->iterators != 0) {
    return;
  }
  if (!HASH_MAP_IS_REHASHING(map)) {
    return;
  }

  curr = &map->tables[0];
  next = &map->tables[1];
  rehash_index = map->rehash_index;
  size_mask = next->size_mask;
  curr_used = curr->used;
  next_used = next->used;

  while (steps--) {
    if (curr_used == 0) {
      /* we are done */
      HASH_MAP_FREE(map, curr->buckets);
      curr->used = curr_used;
      next->used = next_used;
      *curr = *next;
      hash_table_reset(next);
      map->rehash_index = -1;
      return;
    }

    /* rehash index can't overflow */
    while (curr->buckets[rehash_index] == NULL) {
      ++rehash_index;
    }

    entry = curr->buckets[rehash_index];
    while (entry) {
      next_entry = entry->next;
      hash = HASH_MAP_HASH_CODE(map, entry->key, entry->ksize);
      hash = hash & size_mask;
      entry->next = next->buckets[hash];
      next->buckets[hash] = entry;
      curr_used--;
      next_used++;
      entry = next_entry;
    }
    curr->buckets[rehash_index] = NULL;
    rehash_index++;
  }
  curr->used = curr_used;
  next->used = next_used;
  map->rehash_index = rehash_index;
}

static hash_map_entry *hash_map_find_entry(hash_map *map, const void *key, size_t ksiz)
{
  hash_map_entry *entry;
  size_t hash, idx, table;
  hash_table *hash_table;
  if (map->tables[0].size == 0) {
    /* don't have any data at all */
    return NULL;
  }
  hash = HASH_MAP_HASH_CODE(map, key, ksiz);
  if (HASH_MAP_IS_REHASHING(map)) {
    hash_map_rehash_step(map, 1);
  }

  for (table = 0; table <= 1; ++table) {
    hash_table = &map->tables[table];
    idx = hash & hash_table->size_mask;
    /* Search if this slot does not already contain the given key */
    entry = hash_table->buckets[idx];
    while (entry) {
      if (HASH_MAP_COMPARE_KEYS(map, key, ksiz, entry->key, entry->ksize)) {
        return entry;
      }
      entry = entry->next;
    }
    if (!HASH_MAP_IS_REHASHING(map)) {
      return NULL;
    }
  }
  return NULL;
}

static size_t hash_map_expand_get_aligned_size(hash_map *map, size_t size)
{
  size_t result = map->initial_size;

  if (size >= ULONG_MAX) {
    return ULONG_MAX;
  }
  while (1) {
    if (result >= size) {
      return result;
    }
    result <<= 1;
  }
}

static int32_t
hash_map_expand(hash_map *map, size_t size)
{
  size_t realsize = hash_map_expand_get_aligned_size(map, size);
  size_t memsize = realsize * sizeof(hash_map_entry);
  hash_table new_table;
  if (HASH_MAP_IS_REHASHING(map) || map->tables[0].used > size) {
    return -1;
  }

  new_table.size = realsize;
  new_table.size_mask = realsize - 1;
  new_table.buckets = HASH_MAP_CALLOC(map, memsize);
  new_table.used = 0;

  if (map->tables[0].buckets == NULL) {
    /* this is the first call after initialization */
    map->tables[0] = new_table;
    return 0;
  }

  map->tables[1] = new_table;
  map->rehash_index = 0;
  return 0;
}

static int32_t
hash_map_check_and_expand(hash_map *map)
{
  hash_table *table;
  if (HASH_MAP_IS_REHASHING(map)) {
    return 0;
  }

  table = &map->tables[0];
  if (table->size == 0) {
    return hash_map_expand(map, map->initial_size);
  }
  if (table->used >= table->size) {
    return hash_map_expand(map, table->size > table->used ? table->size : table->used * 2);
  }
  return 0;
}

hash_map_iterator *hash_map_iterator_create(hash_map *map)
{
  hash_map_iterator *iterator;
  iterator = HASH_MAP_CALLOC(map, sizeof(hash_map_iterator));
  iterator->map = map;
  iterator->index = -1;
  return iterator;
}

hash_map_iterator *hash_map_safe_iterator_create(hash_map *map)
{
  hash_map_iterator *iterator = hash_map_iterator_create(map);
  iterator->safe = 1;
  return iterator;
}

void hash_map_iterator_get(hash_map_iterator *iterator, const void **key, size_t *ksiz, const void **val, size_t *vsiz)
{
  hash_map_entry *entry = iterator->current;
  if (entry) {
    *key = entry->key;
    *ksiz = entry->ksize;
    *val = entry->value;
    *vsiz = entry->vsize;
  }
}

uint8_t hash_map_iterator_next(hash_map_iterator *iterator)
{
  hash_table *table;
  hash_map *map = iterator->map;
  while (1) {
    if (iterator->current == NULL) {
      table = &map->tables[iterator->table];
      if (iterator->safe && iterator->index == -1 && iterator->table == 0) {
        iterator->map->iterators++;
      }
      iterator->index++;
      if (iterator->index >= (int64_t)(table->size)) {
        if (HASH_MAP_IS_REHASHING(map) && iterator->table == 0) {
          iterator->table++;
          iterator->index = 0;
          table = &map->tables[1];
        } else {
          break;
        }
      }
      iterator->current = table->buckets[iterator->index];
    } else {
      iterator->current = iterator->next;
    }
    if (iterator->current) {
      /* save next entry */
      iterator->next = iterator->current->next;
      return 1;
    }
  }
  return 0;
}

void hash_map_iterator_destroy(hash_map_iterator *iterator)
{
  if (iterator->safe && !(iterator->index == -1 && iterator->table == 0)) {
    iterator->map->iterators--;
  }
  HASH_MAP_FREE(iterator->map, iterator);
}

void *hash_map_allocator_dup_data(hash_map *map, const void *data, size_t size)
{
  void *dup = allocator_malloc(map->type.allocator, size);
  memcpy(dup, data, size);
  return dup;
}

void hash_map_allocator_destroy_data(hash_map *map, void *data, size_t size)
{
  (void) size;
  allocator_free(map->type.allocator, data);
}

int32_t hash_map_bitwise_compare_data(hash_map *map, const void *data1, size_t size1, const void *data2, size_t size2)
{
  (void) map;
  if (size1 != size2) {
    if (size1 > size2) {
      return 1;
    } else {
      return -1;
    }
  }
  return memcmp(data1, data2, size1);
}
