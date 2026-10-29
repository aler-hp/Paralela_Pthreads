#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <time.h>

static const int MAX_THREADS = 4;
int randomized_number(){
    return (rand()+1)%1000;
}

int *set_array(int n){
    if (n <= 0){
        return NULL;
    }

    int *array = (int *)malloc((size_t)n * sizeof(*array));
    if (array == NULL){
        return NULL;
    }

    for (int i = 0; i < n; i++){
        array[i] = randomized_number();
    }

    return array;
}

typedef struct {
    int *array;
    int start;
    int end;
    long sum_total;
    int max;
    int min_value;
} ThreadData;

void *check_chunk(void *argument){
    ThreadData *data = (ThreadData *)argument;
    data->sum_total = 0;
    data->max = data->array[data->start];
    data->min_value = data->array[data->start];

    for (int i = data->start; i < data->end; i++){
        int value = data->array[i];
        data->sum_total += value;
        if (value > data->max){
            data->max = value;
        }
        if (value < data->min_value){
            data->min_value = value;
        }
    }

    return NULL;
}

void serial_data_check(const int *array, int n, int show_extra_data){
    if (array == NULL || n <= 0){
        return;
    }

    long sum_total = 0;
    int max = array[0];
    int min_value = array[0];

    for (int i = 0; i < n; i++){
        sum_total += array[i];
        if (array[i] > max){
            max = array[i];
        }
        if (array[i] < min_value){
            min_value = array[i];
        }
    }

    double avg = (double)sum_total / n;
    if(show_extra_data == 1){
        printf("sum_total: %ld\n", sum_total);
        printf("avg: %.2f\n", avg);
        printf("max: %d\n", max);
        printf("min_value: %d\n", min_value);
    }

}

void pthread_data_check(const int *array, int n, int show_extra_data){
    if (array == NULL || n <= 0){
        return;
    }

    int thread_count = n < MAX_THREADS ? n : MAX_THREADS;
    int chunk_size = n / thread_count;
    int extra_items = n % thread_count;
    pthread_t threads[MAX_THREADS];
    ThreadData thread_data[MAX_THREADS];
    int start = 0;
    int created_threads = 0;

    for (int i = 0; i < thread_count; i++){
        int chunk_length = chunk_size + (i < extra_items);
        thread_data[i].array = array;
        thread_data[i].start = start;
        thread_data[i].end = start + chunk_length;

        if (pthread_create(&threads[i], NULL, check_chunk, &thread_data[i]) != 0){
            for (int j = 0; j < created_threads; j++){
                pthread_join(threads[j], NULL);
            }
            serial_data_check(array, n,0);
            return;
        }

        created_threads++;
        start += chunk_length;
    }

    for (int i = 0; i < thread_count; i++){
        pthread_join(threads[i], NULL);
    }

    long long sum_total = 0;
    int max = thread_data[0].max;
    int min_value = thread_data[0].min_value;
    for (int i = 0; i < thread_count; i++){
        sum_total += thread_data[i].sum_total;
        if (thread_data[i].max > max){
            max = thread_data[i].max;
        }
        if (thread_data[i].min_value < min_value){
            min_value = thread_data[i].min_value;
        }
    }

    double avg = (double)sum_total / n;
    if(show_extra_data == 1){
        printf("sum_total: %lld\n", sum_total);
        printf("avg: %.2f\n", avg);
        printf("max: %d\n", max);
        printf("min_value: %d\n", min_value);
    }
}

typedef void (*DataCheckFunction)(const int *, int, int);

double measure_execution_time(const char *label, DataCheckFunction function, const int *array, int n, int show_extra_data, int show_label){
    struct timespec start;
    struct timespec end;
    if (timespec_get(&start, TIME_UTC) != TIME_UTC){
        fprintf(stderr, "Could not read start time for %s check.\n", label);
        return -1.0;
    }

    function(array, n, show_extra_data);
    if (timespec_get(&end, TIME_UTC) != TIME_UTC){
        fprintf(stderr, "Could not read end time for %s check.\n", label);
        return -1.0;
    }

    double elapsed_ms = (end.tv_sec - start.tv_sec) * 1000.0
        + (end.tv_nsec - start.tv_nsec) / 1000000.0;
    if (show_label){
        printf("%s execution time: %.3f ms\n", label, elapsed_ms);
    }

    return elapsed_ms;
}

void battery_test(){
    int nums[]= {1000, 10000, 100000, 1000000, 10000000};
    printf("| %10s | %14s | %14s |\n", "n", "En serie", "Pthreads");
    printf("|------------|----------------|----------------|\n");

    for (int i = 0; i < 5; i++){
        int n = nums[i];
        int *array = set_array(n);
        if (array == NULL){
            fprintf(stderr, "Could not allocate array for n = %d.\n", n);
            exit(EXIT_FAILURE);
        }

        double serial_time = measure_execution_time("En serie", serial_data_check, array, n, 0, 0);
        double pthread_time = measure_execution_time("Pthreads", pthread_data_check, array, n, 0, 0);
        free(array);

        if (serial_time < 0.0 || pthread_time < 0.0){
            exit(EXIT_FAILURE);
        }

        printf("| %10d | %11.3f ms | %11.3f ms |\n", n, serial_time, pthread_time);
    }
}
int main(){
    srand((unsigned int)time(NULL));

    battery_test();
    return 0;
}
