#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <dirent.h>
#include <sys/stat.h>    
#include <sys/types.h>   
#include <unistd.h>
#include <signal.h>
#include <errno.h>
#include <time.h>        
#include <stdatomic.h>  // Added for atomic operations

#define MAX_PATH_LENGTH 4096
#define MAX_THREADS 8
#define QUEUE_SIZE 1000

typedef struct {
    char path[MAX_PATH_LENGTH];
    off_t size;
    mode_t mode;
    time_t mtime;
} FileInfo;

typedef struct {
    char paths[QUEUE_SIZE][MAX_PATH_LENGTH];
    int front;
    int rear;
    int count;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
    int waiting_threads;
    atomic_int active_processes;  // Added: Counter for active processes
} WorkQueue;

WorkQueue work_queue;
pthread_t thread_pool[MAX_THREADS];
volatile sig_atomic_t running = 1;
FILE* output_file;
pthread_mutex_t output_mutex = PTHREAD_MUTEX_INITIALIZER;

void queue_init(WorkQueue* queue) {
    queue->front = 0;
    queue->rear = -1;
    queue->count = 0;
    queue->waiting_threads = 0;
    atomic_init(&queue->active_processes, 0);  // Initialize active processes counter
    pthread_mutex_init(&queue->mutex, NULL);
    pthread_cond_init(&queue->not_empty, NULL);
    pthread_cond_init(&queue->not_full, NULL);
}

void check_termination_condition(WorkQueue* queue) {
    int active = atomic_load(&queue->active_processes);
    if (active == 0 && queue->count == 0) {
        // If no active processes and queue is empty, terminate
        printf("No active processes and empty queue. Initiating self-termination.\n");
        kill(getpid(), SIGTERM);
    }
}

void queue_push(WorkQueue* queue, const char* path) {
    pthread_mutex_lock(&queue->mutex);
    
    while (queue->count >= QUEUE_SIZE && running) {
        pthread_cond_wait(&queue->not_full, &queue->mutex);
    }
    
    if (!running) {
        pthread_mutex_unlock(&queue->mutex);
        return;
    }
    
    queue->rear = (queue->rear + 1) % QUEUE_SIZE;
    strncpy(queue->paths[queue->rear], path, MAX_PATH_LENGTH - 1);
    queue->paths[queue->rear][MAX_PATH_LENGTH - 1] = '\0';
    queue->count++;
    
    pthread_cond_signal(&queue->not_empty);
    pthread_mutex_unlock(&queue->mutex);
}

int queue_pop(WorkQueue* queue, char* path) {
    pthread_mutex_lock(&queue->mutex);
    
    queue->waiting_threads++;
    
    while (queue->count == 0 && running) {
        check_termination_condition(queue);  // Check termination condition while waiting
        pthread_cond_wait(&queue->not_empty, &queue->mutex);
    }
    
    queue->waiting_threads--;
    
    if (queue->count == 0) {
        pthread_mutex_unlock(&queue->mutex);
        return 0;
    }
    
    strncpy(path, queue->paths[queue->front], MAX_PATH_LENGTH);
    queue->front = (queue->front + 1) % QUEUE_SIZE;
    queue->count--;
    
    pthread_cond_signal(&queue->not_full);
    pthread_mutex_unlock(&queue->mutex);
    return 1;
}

void handle_signal(int signum) {
    running = 0;
    pthread_cond_broadcast(&work_queue.not_empty);
    pthread_cond_broadcast(&work_queue.not_full);
}

void process_file(const char* path) {
    struct stat st;
    if (lstat(path, &st) == -1) {
        return;
    }
    
    FileInfo info;
    strncpy(info.path, path, MAX_PATH_LENGTH - 1);
    info.path[MAX_PATH_LENGTH - 1] = '\0';
    info.size = st.st_size;
    info.mode = st.st_mode;
    info.mtime = st.st_mtime;
    
    pthread_mutex_lock(&output_mutex);
    fprintf(output_file, "Path: %s\n", info.path);
    fprintf(output_file, "Size: %ld bytes\n", (long)info.size);
    fprintf(output_file, "Type: %s\n", S_ISDIR(info.mode) ? "Directory" : 
                                      S_ISREG(info.mode) ? "Regular File" :
                                      S_ISLNK(info.mode) ? "Symbolic Link" : "Other");
    fprintf(output_file, "Permissions: %o\n", info.mode & 0777);
    fprintf(output_file, "Last Modified: %s", ctime(&info.mtime));
    fprintf(output_file, "-------------------\n");
    fflush(output_file);
    pthread_mutex_unlock(&output_mutex);
}

void* worker_thread(void* arg) {
    char path[MAX_PATH_LENGTH];
    pthread_t thread_id = pthread_self();
    printf("Thread ID: %lu started\n", (unsigned long)thread_id);
    while (running) {
        if (!queue_pop(&work_queue, path)) {
            break;
        }
        
        atomic_fetch_add(&work_queue.active_processes, 1);  // Increment active processes
        
        DIR* dir = opendir(path);
        if (dir) {
            struct dirent* entry;
            while ((entry = readdir(dir)) != NULL && running) {
                if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
                    continue;
                }
                
                char full_path[MAX_PATH_LENGTH];
                snprintf(full_path, MAX_PATH_LENGTH, "%s/%s", path, entry->d_name);
                
                struct stat st;
                if (lstat(full_path, &st) == -1) {
                    continue;
                }
                
                process_file(full_path);
                
                if (S_ISDIR(st.st_mode)) {
                    queue_push(&work_queue, full_path);
                }
            }
            closedir(dir);
        }
        
        atomic_fetch_sub(&work_queue.active_processes, 1);  // Decrement active processes
        check_termination_condition(&work_queue);  // Check termination condition after processing
    }
    
    return NULL;
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        fprintf(stderr, "Usage: %s <directory> <output_file>\n", argv[0]);
        return 1;
    }
    
    signal(SIGINT, handle_signal);
    signal(SIGTERM, handle_signal);
    
    queue_init(&work_queue);
    
    output_file = fopen(argv[2], "w");
    if (!output_file) {
        perror("Failed to open output file");
        return 1;
    }
    
    queue_push(&work_queue, argv[1]);
    
    for (int i = 0; i < MAX_THREADS; i++) {
        if (pthread_create(&thread_pool[i], NULL, worker_thread, NULL) != 0) {
            fprintf(stderr, "Failed to create thread %d\n", i);
            running = 0;
            break;
        }
    }
    
    for (int i = 0; i < MAX_THREADS; i++) {
        pthread_join(thread_pool[i], NULL);
    }
    
    fclose(output_file);
    pthread_mutex_destroy(&output_mutex);
    pthread_mutex_destroy(&work_queue.mutex);
    pthread_cond_destroy(&work_queue.not_empty);
    pthread_cond_destroy(&work_queue.not_full);
    
    return 0;
}
