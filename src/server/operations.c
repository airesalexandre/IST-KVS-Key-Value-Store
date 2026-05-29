#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>

#include "constants.h"
#include "kvs.h"

static struct HashTable *kvs_table = NULL;


/// Calculates a timespec from a delay in milliseconds.
/// @param delay_ms Delay in milliseconds.
/// @return Timespec with the given delay.
static struct timespec delay_to_timespec(unsigned int delay_ms) {
    return (struct timespec){delay_ms / 1000, (delay_ms % 1000) * 1000000};
}

int kvs_init() {
    if (kvs_table != NULL) {
        fprintf(stderr, "KVS state has already been initialized\n");
        return 1;
    }

    kvs_table = create_hash_table();
    return kvs_table == NULL;
}

int kvs_terminate() {
    if (kvs_table == NULL) {
        fprintf(stderr, "KVS state must be initialized\n");
        return 1;
    }

    free_table(kvs_table);
    return 0;
}

// HELPER FUNCTIONS
void write_output(int fd_out, const char *message) {
    size_t len = strlen(message);
    size_t written = 0;  // Use size_t for written

    while (written < len) {
        size_t remaining = len - written;
        ssize_t n = write(fd_out, message + written, remaining);
        if (n == -1) {
            perror("Error writing to file descriptor");
            return;
        }

        // Explicitly cast `n` to size_t to avoid sign conversion issue
        written += (size_t)n;
    }
}

int compare_keys(const void *a, const void *b) {
    const char **key_a = (const char **)a;
    const char **key_b = (const char **)b;
    return strcmp(*key_a, *key_b);
}

// KVS OPERATIONS
int kvs_write(size_t num_pairs, char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE], int fd_out) {
    if (kvs_table == NULL) {
        write_output(fd_out, "KVS state must be initialized\n");
        return 1;
    }
    
    for (size_t i = 0; i < num_pairs; i++) {
        if (write_pair(kvs_table, keys[i], values[i]) != 0) {
            char buffer[MAX_STRING_SIZE * 2 + 50];
            snprintf(buffer, sizeof(buffer), "Failed to write keypair (%s,%s)\n", keys[i], values[i]);
            write_output(fd_out, buffer);
            return 1;  // Return early if any pair fails
        }
    }

    return 0;  // Success
}

int kvs_read(size_t num_pairs, char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE], int fd_out) {
    if (kvs_table == NULL) {
        write_output(fd_out, "KVS state must be initialized\n");
        return 1;
    }

    // Prepare the output buffer
    char buffer[MAX_WRITE_SIZE * (MAX_STRING_SIZE * 2 + 10)];
    size_t offset = 0;

    // Start the output with an opening bracket
    int written = snprintf(buffer + offset, sizeof(buffer) - offset, "[");
    if (written < 0 || (size_t)written >= sizeof(buffer) - offset) {
        write_output(fd_out, "Error formatting output. - READ1\n");
        return 1;
    }
    offset += (size_t)written;

    // Retrieve the values for the keys
    for (size_t i = 0; i < num_pairs; i++) {
        char *result = read_pair(kvs_table, keys[i]);
        if (result == NULL) {
            values[i][0] = '\0';  // Indicate error in values array
        } else {
            strncpy(values[i], result, MAX_STRING_SIZE - 1);
            values[i][MAX_STRING_SIZE - 1] = '\0';
            free(result);
        }
    }

    // Sort the keys and values
    char *sorted_keys[MAX_WRITE_SIZE];
    char *sorted_values[MAX_WRITE_SIZE];
    for (size_t i = 0; i < num_pairs; i++) {
        sorted_keys[i] = keys[i];
        sorted_values[i] = values[i];
    }
    qsort(sorted_keys, num_pairs, sizeof(char *), compare_keys);

    // Write the sorted output
    for (size_t i = 0; i < num_pairs; i++) {
        size_t j;
        for (j = 0; j < num_pairs; j++) {
            if (strcmp(sorted_keys[i], keys[j]) == 0) {
                break;
            }
        }
        if (values[j][0] == '\0') {
            written = snprintf(buffer + offset, sizeof(buffer) - offset, "(%s,KVSERROR)", sorted_keys[i]);
        } else {
            written = snprintf(buffer + offset, sizeof(buffer) - offset, "(%s,%s)", sorted_keys[i], sorted_values[j]);
        }
        if (written < 0 || (size_t)written >= sizeof(buffer) - offset) {
            write_output(fd_out, "Error formatting output. - READ2\n");
            return 1;
        }
        offset += (size_t)written;
    }

    // End the output with a closing bracket and newline
    written = snprintf(buffer + offset, sizeof(buffer) - offset, "]\n");
    if (written < 0 || (size_t)written >= sizeof(buffer) - offset) {
        write_output(fd_out, "Error formatting output. - READ3\n");
        return 1;
    }

    // Write the buffer to the specified output file descriptor
    write_output(fd_out, buffer);

    return 0;
}

int kvs_delete(size_t num_pairs, char keys[][MAX_STRING_SIZE], int fd_out) {
    if (kvs_table == NULL) {
        write_output(fd_out, "KVS state must be initialized\n");
        return 1;
    }
    int aux = 0;

    for (size_t i = 0; i < num_pairs; i++) {
        if (delete_pair(kvs_table, keys[i]) != 0) {
            if (!aux) {
                write_output(fd_out, "[");
                aux = 1;
            }
            char buffer[MAX_STRING_SIZE + 20];
            snprintf(buffer, sizeof(buffer), "(%s,KVSMISSING)", keys[i]);
            write_output(fd_out, buffer);
        }
    }
    if (aux) {
        write_output(fd_out, "]\n");
    }

    return 0;
}

void kvs_show(int fd_out) {
    if (kvs_table == NULL) {
        write_output(fd_out, "KVS state must be initialized\n");
        return;
    }

    // Collect keys and values
    char *keys[MAX_WRITE_SIZE];
    char *values[MAX_WRITE_SIZE];
    size_t count = 0;

    for (int i = 0; i < TABLE_SIZE; i++) {
        KeyNode *keyNode = kvs_table->table[i];
        while (keyNode != NULL && count < MAX_WRITE_SIZE) {
            keys[count] = keyNode->key;
            values[count] = keyNode->value;
            count++;
            keyNode = keyNode->next;
        }
    }

    // Sort the keys
    char *sorted_keys[MAX_WRITE_SIZE];
    char *sorted_values[MAX_WRITE_SIZE];
    for (size_t i = 0; i < count; i++) {
        sorted_keys[i] = keys[i];
        sorted_values[i] = values[i];
    }
    qsort(sorted_keys, count, sizeof(char *), compare_keys);

    // Write sorted output
    char buffer[MAX_WRITE_SIZE * (MAX_STRING_SIZE * 2 + 10)];
    size_t offset = 0;
    for (size_t i = 0; i < count; i++) {
        for (size_t j = 0; j < count; j++) {
            if (strcmp(sorted_keys[i], keys[j]) == 0) {
                snprintf(buffer + offset, sizeof(buffer) - offset, "(%s, %s)\n", sorted_keys[i], sorted_values[j]);
                offset += strlen(buffer + offset);
                break;
            }
        }
    }
    write_output(fd_out, buffer);
}

void kvs_backup(const char *filename) {
    if (kvs_table == NULL) {
        fprintf(stderr, "KVS state must be initialized\n");
        return;
    }

    // Open the backup file
    int fd = open(filename, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        perror("Failed to create backup file");
        return;
    }

    // Collect keys and values
    char *keys[MAX_WRITE_SIZE];
    char *values[MAX_WRITE_SIZE];
    size_t count = 0;

    for (int i = 0; i < TABLE_SIZE; i++) {
        KeyNode *keyNode = kvs_table->table[i];
        while (keyNode != NULL && count < MAX_WRITE_SIZE) {
            keys[count] = keyNode->key;
            values[count] = keyNode->value;
            count++;
            keyNode = keyNode->next;
        }
    }

    // Sort the keys
    char *sorted_keys[MAX_WRITE_SIZE];
    char *sorted_values[MAX_WRITE_SIZE];
    memcpy(sorted_keys, keys, count * sizeof(char *));
    memcpy(sorted_values, values, count * sizeof(char *));

    qsort(sorted_keys, count, sizeof(char *), compare_keys);

    // Write sorted output to the backup file
    for (size_t i = 0; i < count; i++) {
        for (size_t j = 0; j < count; j++) {
            if (strcmp(sorted_keys[i], keys[j]) == 0) {
                dprintf(fd, "(%s, %s)\n", sorted_keys[i], values[j]);
                break;
            }
        }
    }

    // Close the file and free allocated memory
    close(fd);

    // Free memory for keys and values after use
    for (size_t i = 0; i < count; i++) {
        free(keys[i]);
        free(values[i]);
    }
}

void kvs_wait(unsigned int delay_ms) {
    struct timespec delay = delay_to_timespec(delay_ms);
    nanosleep(&delay, NULL);
}