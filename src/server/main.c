#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <linux/limits.h>
#include <semaphore.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/wait.h>
#include <threads.h>
#include <pthread.h>
#include <errno.h>

#include "../common/constants.h"
#include "constants.h"
#include "operations.h"
#include "parser.h"

sem_t backup_semaphore;
pthread_mutex_t mutex;
unsigned int active_threads = 0;
pthread_t main_thread;
char buffer[BUFFER_SIZE];

/*--------------------Structs------------------*/

// Struct to pass arguments to the thread function
typedef struct {
    char *file_path;
    char *output_file;
} thread_args;

// Struct to pass arguments to the client thread
typedef struct {
    char notif_fifo[BUFFER_SIZE];
    char req_fifo[BUFFER_SIZE];
    char resp_fifo[BUFFER_SIZE];
} client_pipes_args;

typedef struct {
    client_pipes_args messages[MAX_SESSION_COUNT];
    int front;
    int rear;
    int count;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} shared_buffer;

typedef struct {
    shared_buffer *buffer;
    const char *register_fifo;
} main_thread_args;

/*-----------------1ºPARTE-FUNCOES----------------------------*/

// Function to process the commands in a job file
void commands(int fd_in, int fd_out, const char *job_filename, const char *output_filename) {
    int backup_counter = 0;

    while (1) {
        char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
        char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
        unsigned int delay;
        size_t num_pairs;

        switch (get_next(fd_in)) {
            case CMD_WRITE:
                
                num_pairs = parse_write(fd_in, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
                if (num_pairs == 0) {
                    continue;
                }
                pthread_mutex_lock(&mutex);
                if (kvs_write(num_pairs, keys, values, fd_out)) {
                    // Error messages are already handled within `kvs_write`

                }
                pthread_mutex_unlock(&mutex);
                break;

            case CMD_READ:
                num_pairs = parse_read_delete(fd_in, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);
                if (num_pairs == 0) {
                    continue;
                }
                pthread_mutex_lock(&mutex);
                if (kvs_read(num_pairs, keys, values, fd_out)) {
                }
                pthread_mutex_unlock(&mutex);
                break;

            case CMD_DELETE:
                num_pairs = parse_read_delete(fd_in, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);
                if (num_pairs == 0) {
                    continue;
                }
                pthread_mutex_lock(&mutex);
                if (kvs_delete(num_pairs, keys, fd_out)) {
                }
                pthread_mutex_unlock(&mutex);
                break;

            case CMD_SHOW:
                pthread_mutex_lock(&mutex);
                kvs_show(fd_out);
                pthread_mutex_unlock(&mutex);
                break;

            case CMD_WAIT:
                if (parse_wait(fd_in, &delay, NULL) == -1) {
                    continue;
                }
                pthread_mutex_lock(&mutex);
                if (delay > 0) {
                    kvs_wait(delay);
                }
                pthread_mutex_unlock(&mutex);
                break;

            case CMD_BACKUP:
                backup_counter++;
                sem_wait(&backup_semaphore);
                // Create a child process to perform the backup
                pthread_mutex_lock(&mutex);
                pid_t pid = fork();
                pthread_mutex_unlock(&mutex);
                if (pid == 0) {
                    // Child process
                    char backup_filename[PATH_MAX];
                    
                    // Extract the directory from the output file path
                    char *output_dir = strdup(output_filename);
                    char *last_slash = strrchr(output_dir, '/');
                    if (last_slash) {
                        *last_slash = '\0';  // Terminate the string at the last slash
                    } else {
                        output_dir[0] = '.';  // Use current directory if no slash found
                        output_dir[1] = '\0';
                    }
                    snprintf(backup_filename, sizeof(backup_filename), "%s/%s-%d.bck", output_dir, job_filename, backup_counter);
                    free(output_dir);
                    pthread_mutex_lock(&mutex);
                    kvs_backup(backup_filename);
                    pthread_mutex_unlock(&mutex);
                    close(fd_in);
                    close(fd_out);  
                    _exit(0);
                } else if (pid < 0) {
                    // Fork failed
                    write_output(fd_out, "Failed to perform backup.\n");
                    sem_post(&backup_semaphore);
                } else {
                    // Father process
                    int status;
                    // Wait for any child process to finish
                    waitpid(pid, &status, 0);
                    if (WIFEXITED(status)) {
                        sem_post(&backup_semaphore);
                    }
                }
                 
                break;

            case CMD_INVALID:
                write_output(fd_out, "Invalid command format.\n");
                break;

            case CMD_HELP:
                write_output(fd_out,
                             "Available commands:\n"
                             "  WRITE [(key,value)(key2,value2),...]\n"
                             "  READ [key,key2,...]\n"
                             "  DELETE [key,key2,...]\n"
                             "  SHOW\n"
                             "  WAIT <delay_ms>\n"
                             "  BACKUP\n"
                             "  HELP\n");
                break;

            case CMD_EMPTY:
                break;

            case EOC:
                return;
        }
    }
}

void process_job_file(const char *input_file, const char *output_file) {
    // Open the input and output files
    int input_fd = open(input_file, O_RDONLY);
    if (input_fd < 0) {
        perror("Failed to open input file");
        return;
    }

    int output_fd = open(output_file, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (output_fd < 0) {
        perror("Failed to create output file");
        close(input_fd);
        return;
    }

    // Extract the job filename from the input file path
    char *job_filename = strrchr(input_file, '/');
    if (job_filename) {
        job_filename++;  // Skip the '/'
    } else {
        job_filename = (char *)input_file;  // No '/' found, use the whole input_file
    }

    // Remove the .job extension if present
    char *dot = strrchr(job_filename, '.');
    if (dot && strcmp(dot, ".job") == 0) {
        *dot = '\0';
    }

    // Process commands in the job file using the commands function
    commands(input_fd, output_fd, job_filename, output_file);

    // Close the input and output files
    close(input_fd);
    close(output_fd);
}

void *thread_function(void *arg) {
    thread_args *args = (thread_args *)arg;

    // Call the process_job_file function
    process_job_file(args->file_path, args->output_file);
    
    // Free the memory allocated for the arguments
    free(args->file_path);
    free(args->output_file);
    free(args);
    pthread_mutex_lock(&mutex);
    active_threads--;
    pthread_mutex_unlock(&mutex);
    return NULL;
}


/*---------------2ºPARTE-FUNCOES------------------*/

void init_buffer(shared_buffer *buffer) {
    buffer->front = 0;
    buffer->rear = 0;
    buffer->count = 0;
    pthread_mutex_init(&buffer->mutex, NULL);
    pthread_cond_init(&buffer->not_empty, NULL);
    pthread_cond_init(&buffer->not_full, NULL);
}

void *main_thread(void *arg) {
    main_thread_args *args = (main_thread_args *)arg;
    shared_buffer *buffer = args->buffer;
    const char *register_fifo = args->register_fifo;

    mkfifo(register_fifo, 0640);
    int register_fd = open(register_fifo, O_RDONLY);
    if (register_fd == -1) {
        perror("Failed to open register FIFO");
        return NULL;
    }
}

int main(int argc, char *argv[]) {
    // Read command line arguments
    if (argc != 5) {
        fprintf(stderr, "Usage: %s <directory> <max_concurrent_backups> <max_threads> <nome_do_FIFO_de_registo>\n", argv[0]);
        return 1;
    }

    char *path = argv[1];
    int temp_backups = atoi(argv[2]);
    int temp_threads = atoi(argv[3]);
    const char *fifo_name = argv[4];
    shared_buffer buffer;
    init_buffer(&buffer); 

    if (temp_backups < 0) {
        fprintf(stderr, "Error: max_concurrent_backups must be a non-negative integer.\n");
        return 1;
    }
    
    if (temp_threads <= 0) {
        fprintf(stderr, "Error: max_threads must be a positive integer.\n");
        return 1;
    }


    unsigned int max_concurrent_backups = (unsigned int)temp_backups;
    unsigned int max_concurrent_threads = (unsigned int)temp_threads;

    pthread_t threads[max_concurrent_threads];
    unsigned int thread_counter = 0;
    
    if (kvs_init()) {
        fprintf(stderr, "Failed to initialize KVS\n");
        return 1;
    }
    
    if (pthread_mutex_init(&mutex, NULL) != 0) {
        perror("Failed to initialize mutex");
        return 1;
    }

    // Inicialize the semaphore
    sem_init(&backup_semaphore, 0, max_concurrent_backups);

    DIR *dir;
    struct dirent *entry;

    // Open the directory
    if ((dir = opendir(path)) == NULL) {
        perror("Failed to open directory");
        return 1;
    }

    if (unlink(fifo_name) != 0) {
        perror("Failed to remove FIFO");
        return 1;
    }

    if (mkfifo(fifo_name, 0640) == -1) {
        perror("Failed to create FIFO");
        return 1;
    }

    int fifo = open(fifo_name, O_RDONLY);
    if (fifo == -1) {
        perror("Failed to open FIFO");
        return 1;
    }

    if (pthread_create(&main_thread, NULL, main_thread, &buffer) != 0) {
        perror("Failed to create main thread");
        return 1;
    }

    // Read the FIFO
    while (1) {
        ssize_t bytes_read = read(fifo, buffer, sizeof(buffer) - 1);
        if (bytes_read > 0) {
            buffer[bytes_read] = '\0';

            client_args *args = malloc(sizeof(client_args));
            if (!args) {
                perror("Failed to allocate memory for client args");
                continue;
            }

            if (sscanf(buffer, "%s %s %s", args->req_fifo, args->resp_fifo, args->notif_fifo) != 3) {
                fprintf(stderr, "Invalid message format\n");
                free(args);
                continue;
            }

            // Criar thread para o cliente
            if (pthread_create(&thread, NULL, client_thread, args) != 0) {
                perror("Failed to create client thread");
                free(args);
                continue;
            }

            // Desacoplar a thread principal da thread do cliente
            pthread_detach(thread);
        } else if (bytes_read == 0) {
            printf("FIFO closed\n");
            break;
        } else {
            perror("Failed to read from FIFO");
            break;
        }
    }



    while ((entry = readdir(dir)) != NULL) {
        char file_path[BUFFER_SIZE];
        snprintf(file_path, BUFFER_SIZE, "%s/%s", path, entry->d_name);

        struct stat sb;
        if (stat(file_path, &sb) == 0 && !S_ISDIR(sb.st_mode) && strstr(entry->d_name, ".job")) {
            char output_file[BUFFER_SIZE];
            snprintf(output_file, BUFFER_SIZE, "%s/%.*s.out", path, (int)(strlen(entry->d_name) - 4), entry->d_name);

            // Allocate memory for the thread arguments
            thread_args *args = malloc(sizeof(thread_args));

            // Check if memory allocation was successful
            if (!args) {
                perror("Failed to allocate memory for thread arguments");
                continue;
            }

            // Copy the file paths to the thread arguments
            args->file_path = strdup(file_path);
            args->output_file = strdup(output_file);

            if (!args->file_path || !args->output_file) {
                perror("Failed to allocate memory for file paths");
                free(args->file_path);
                free(args->output_file);
                free(args);
                continue;
            }

            // Create a new thread to process the job file
            if (pthread_create(&threads[thread_counter], NULL, thread_function, args) != 0) {
                perror("Failed to create thread");
                free(args->file_path);
                free(args->output_file);
                free(args);
                continue;
            }

            // Increment the thread counter
            pthread_mutex_lock(&mutex);
            thread_counter++;
            pthread_mutex_unlock(&mutex);

            // If the maximum number of threads has been reached, wait for them to finish
            if (thread_counter >= max_concurrent_threads) {
                for (unsigned int i = 0; i < thread_counter; i++) {
                    pthread_join(threads[i], NULL);
                }
                thread_counter = 0;  // Restart the counter
            }
        }
    }
    
    // Wait for the remaining threads to finish
    for (unsigned int i = 0; i < thread_counter; i++) {
        pthread_join(threads[i], NULL);
    }
    
    //Close the directory, terminate the KVS and destroy the mutex and semaphore
    closedir(dir);
    kvs_terminate();
    pthread_mutex_destroy(&mutex);
    sem_destroy(&backup_semaphore);

    return 0;
}