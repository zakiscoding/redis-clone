#ifndef STORE_H
#define STORE_H

#include "platform.h"

#define MAX_KEYS 4096

typedef enum { VALUE_NONE = 0, VALUE_STRING, VALUE_LIST, VALUE_STREAM } ValueType;

typedef struct {
    char *data;
    size_t length;
} Item;

typedef struct {
    long long milliseconds;
    long long sequence;
    Item *fields;
    size_t field_count;
} StreamRecord;

typedef struct {
    char *key;
    size_t key_len;
    ValueType type;
    char *string;
    size_t string_len;
    Item *items;
    size_t count;
    StreamRecord *records;
    size_t record_count;
    size_t record_capacity;
    long long last_stream_ms;
    long long last_stream_sequence;
    int64_t expires_at_ms;
    int used;
} Entry;

Entry *find_entry(const char *key, size_t key_len);
Entry *get_entry(const char *key, size_t key_len, int create, ValueType type);
void free_entry(Entry *entry);
unsigned long long key_version(const char *key, size_t key_len);
void bump_version(const char *key, size_t key_len);
size_t store_key_count(void);
void store_flush(void);
void store_shutdown(void);

#endif
