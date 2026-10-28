#include "store.h"

static Entry store[MAX_KEYS];

typedef struct {
    char *key;
    size_t key_len;
    unsigned long long version;
} VersionSlot;

static VersionSlot *versions;
static size_t version_count;
static size_t version_cap;

static int key_eq(const char *left, size_t left_len, const char *right, size_t right_len) {
    return left_len == right_len && (left_len == 0 || memcmp(left, right, left_len) == 0);
}

static char *dup_bytes(const char *data, size_t len) {
    char *copy = malloc(len + 1);
    if (!copy) return NULL;
    if (len > 0) memcpy(copy, data, len);
    copy[len] = '\0';
    return copy;
}

void free_entry(Entry *entry) {
    if (!entry->used) return;
    free(entry->key);
    free(entry->string);
    for (size_t i = 0; i < entry->count; i++) free(entry->items[i].data);
    free(entry->items);
    for (size_t i = 0; i < entry->record_count; i++) {
        for (size_t j = 0; j < entry->records[i].field_count; j++) free(entry->records[i].fields[j].data);
        free(entry->records[i].fields);
    }
    free(entry->records);
    memset(entry, 0, sizeof(*entry));
}

unsigned long long key_version(const char *key, size_t key_len) {
    for (size_t i = 0; i < version_count; i++) {
        if (key_eq(versions[i].key, versions[i].key_len, key, key_len)) return versions[i].version;
    }
    return 0;
}

void bump_version(const char *key, size_t key_len) {
    for (size_t i = 0; i < version_count; i++) {
        if (key_eq(versions[i].key, versions[i].key_len, key, key_len)) {
            versions[i].version++;
            return;
        }
    }
    if (version_count == version_cap) {
        size_t cap = version_cap ? version_cap * 2 : 16;
        VersionSlot *grown = realloc(versions, cap * sizeof(*grown));
        if (!grown) return;
        versions = grown;
        version_cap = cap;
    }
    char *copy = dup_bytes(key, key_len);
    if (!copy) return;
    versions[version_count].key = copy;
    versions[version_count].key_len = key_len;
    versions[version_count].version = 1;
    version_count++;
}

Entry *find_entry(const char *key, size_t key_len) {
    int64_t now = unix_ms();
    for (size_t i = 0; i < MAX_KEYS; i++) {
        if (!store[i].used || !key_eq(store[i].key, store[i].key_len, key, key_len)) continue;
        if (store[i].expires_at_ms > 0 && store[i].expires_at_ms <= now) {
            bump_version(store[i].key, store[i].key_len);
            free_entry(&store[i]);
            return NULL;
        }
        return &store[i];
    }
    return NULL;
}

Entry *get_entry(const char *key, size_t key_len, int create, ValueType type) {
    Entry *entry = find_entry(key, key_len);
    if (entry || !create) return entry;
    for (size_t i = 0; i < MAX_KEYS; i++) {
        if (store[i].used) continue;
        char *copy = dup_bytes(key, key_len);
        if (!copy) return NULL;
        memset(&store[i], 0, sizeof(store[i]));
        store[i].used = 1;
        store[i].key = copy;
        store[i].key_len = key_len;
        store[i].type = type;
        return &store[i];
    }
    return NULL;
}

void store_flush(void) {
    for (size_t i = 0; i < MAX_KEYS; i++) {
        if (store[i].used) bump_version(store[i].key, store[i].key_len);
        free_entry(&store[i]);
    }
}

size_t store_key_count(void) {
    int64_t now = unix_ms();
    size_t count = 0;
    for (size_t i = 0; i < MAX_KEYS; i++) {
        if (!store[i].used) continue;
        if (store[i].expires_at_ms > 0 && store[i].expires_at_ms <= now) continue;
        count++;
    }
    return count;
}

void store_shutdown(void) {
    for (size_t i = 0; i < MAX_KEYS; i++) free_entry(&store[i]);
    for (size_t i = 0; i < version_count; i++) free(versions[i].key);
    free(versions);
    versions = NULL;
    version_count = 0;
    version_cap = 0;
}
