#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <time.h>

static const int MAX_THREADS = 4;

/* constante de compilacion */
enum { HISTOGRAM_BINS = 10 };

static const int VALUES_PER_BIN = 10;
static const int MIN_VALUE = 0;
static const int MAX_VALUE = 99;
static int randomized_number(void) {
    return MIN_VALUE + rand() % (MAX_VALUE - MIN_VALUE + 1);
}

static int *set_array(int n) {
    if (n <= 0) {
        return NULL;
    }

    int *array = (int *)malloc((size_t)n * sizeof(*array));
    if (array == NULL) {
        return NULL;
    }

    for (int i = 0; i < n; i++) {
        array[i] = randomized_number();
    }

    return array;
}

static void split_range(int n, int thread_count, int thread_index, int *start, int *end) {
    int chunk_size = n / thread_count;
    int extra_items = n % thread_count;
    int offset = thread_index * chunk_size
        + (thread_index < extra_items ? thread_index : extra_items);

    *start = offset;
    *end = offset + chunk_size + (thread_index < extra_items ? 1 : 0);
}


static void histogram_serial(const int *array, int n, int thread_count, int histogram[HISTOGRAM_BINS]) {
    (void)thread_count;

    for (int i = 0; i < HISTOGRAM_BINS; i++) {
        histogram[i] = 0;
    }

    if (array == NULL || n <= 0) {
        return;
    }

    for (int i = 0; i < n; i++) {
        histogram[array[i] / VALUES_PER_BIN]++;
    }
}


typedef struct {
    const int *array;
    int start;
    int end;
    int *histogram;
    pthread_mutex_t *bin_mutexes;
} MutexHistogramData;

static void *histogram_mutex_worker(void *argument) {
    MutexHistogramData *data = (MutexHistogramData *)argument;

    for (int i = data->start; i < data->end; i++) {
        int bin = data->array[i] / VALUES_PER_BIN;
        pthread_mutex_lock(&data->bin_mutexes[bin]);
        data->histogram[bin]++;
        pthread_mutex_unlock(&data->bin_mutexes[bin]);
    }

    return NULL;
}

/* un mutex por categoria */
static void histogram_parallel_mutex(const int *array, int n, int thread_count, int histogram[HISTOGRAM_BINS]) {
    for (int i = 0; i < HISTOGRAM_BINS; i++) {
        histogram[i] = 0;
    }

    if (array == NULL || n <= 0) {
        return;
    }

    if (thread_count > n) {
        thread_count = n;
    }
    if (thread_count > MAX_THREADS) {
        thread_count = MAX_THREADS;
    }
    if (thread_count < 1) {
        return;
    }

    pthread_mutex_t bin_mutexes[HISTOGRAM_BINS];
    for (int i = 0; i < HISTOGRAM_BINS; i++) {
        if (pthread_mutex_init(&bin_mutexes[i], NULL) != 0) {
            for (int j = 0; j < i; j++) {
                pthread_mutex_destroy(&bin_mutexes[j]);
            }
            fprintf(stderr, "No se pudieron inicializar los mutex; se usa la version secuencial.\n");
            histogram_serial(array, n, 1, histogram);
            return;
        }
    }

    pthread_t threads[MAX_THREADS];
    MutexHistogramData data[MAX_THREADS];
    int created_threads = 0;

    for (int i = 0; i < thread_count; i++) {
        data[i].array = array;
        data[i].histogram = histogram;
        data[i].bin_mutexes = bin_mutexes;
        split_range(n, thread_count, i, &data[i].start, &data[i].end);

        if (pthread_create(&threads[i], NULL, histogram_mutex_worker, &data[i]) != 0) {
            for (int j = 0; j < created_threads; j++) {
                pthread_join(threads[j], NULL);
            }
            for (int j = 0; j < HISTOGRAM_BINS; j++) {
                pthread_mutex_destroy(&bin_mutexes[j]);
            }
            fprintf(stderr, "No se pudo crear el thread %d; se usa la version secuencial.\n", i);
            histogram_serial(array, n, 1, histogram);
            return;
        }

        created_threads++;
    }

    for (int i = 0; i < created_threads; i++) {
        pthread_join(threads[i], NULL);
    }
    for (int i = 0; i < HISTOGRAM_BINS; i++) {
        pthread_mutex_destroy(&bin_mutexes[i]);
    }
}


typedef struct {
    const int *array;
    int start;
    int end;
    /* evita false sharing */
    _Alignas(64) int histogram[HISTOGRAM_BINS];
} LocalHistogramData;

static void *histogram_private_worker(void *argument) {
    LocalHistogramData *data = (LocalHistogramData *)argument;

    for (int i = 0; i < HISTOGRAM_BINS; i++) {
        data->histogram[i] = 0;
    }

    for (int i = data->start; i < data->end; i++) {
        data->histogram[data->array[i] / VALUES_PER_BIN]++;
    }

    return NULL;
}

/* histograma privado por thread */
static void histogram_parallel_private(const int *array, int n, int thread_count, int histogram[HISTOGRAM_BINS]) {
    for (int i = 0; i < HISTOGRAM_BINS; i++) {
        histogram[i] = 0;
    }

    if (array == NULL || n <= 0) {
        return;
    }

    if (thread_count > n) {
        thread_count = n;
    }
    if (thread_count > MAX_THREADS) {
        thread_count = MAX_THREADS;
    }
    if (thread_count < 1) {
        return;
    }

    pthread_t threads[MAX_THREADS];
    LocalHistogramData data[MAX_THREADS];
    int created_threads = 0;

    for (int i = 0; i < thread_count; i++) {
        data[i].array = array;
        split_range(n, thread_count, i, &data[i].start, &data[i].end);

        if (pthread_create(&threads[i], NULL, histogram_private_worker, &data[i]) != 0) {
            for (int j = 0; j < created_threads; j++) {
                pthread_join(threads[j], NULL);
            }
            fprintf(stderr, "No se pudo crear el thread %d; se usa la version secuencial.\n", i);
            histogram_serial(array, n, 1, histogram);
            return;
        }

        created_threads++;
    }

    for (int i = 0; i < created_threads; i++) {
        pthread_join(threads[i], NULL);
    }

    for (int i = 0; i < created_threads; i++) {
        for (int bin = 0; bin < HISTOGRAM_BINS; bin++) {
            histogram[bin] += data[i].histogram[bin];
        }
    }
}


typedef void (*HistogramFunction)(const int *, int, int, int *);

/* reloj monotono (compatible con MinGW) */
static int get_current_time(struct timespec *timestamp) {
#if defined(_WIN32) && !defined(_UCRT)
    return clock_gettime(CLOCK_MONOTONIC, timestamp) == 0;
#else
    return timespec_get(timestamp, TIME_UTC) == TIME_UTC;
#endif
}

static double measure_execution_time(HistogramFunction function, const int *array, int n,
                                     int thread_count, int histogram[HISTOGRAM_BINS]) {
    struct timespec start;
    struct timespec end;

    if (!get_current_time(&start)) {
        fprintf(stderr, "No se pudo leer el tiempo inicial.\n");
        return -1.0;
    }

    function(array, n, thread_count, histogram);

    if (!get_current_time(&end)) {
        fprintf(stderr, "No se pudo leer el tiempo final.\n");
        return -1.0;
    }

    return (end.tv_sec - start.tv_sec) * 1000.0
        + (end.tv_nsec - start.tv_nsec) / 1000000.0;
}

static long histogram_total(const int *histogram) {
    long total = 0;
    for (int i = 0; i < HISTOGRAM_BINS; i++) {
        total += histogram[i];
    }
    return total;
}

static int histogram_equal(const int *first, const int *second) {
    for (int i = 0; i < HISTOGRAM_BINS; i++) {
        if (first[i] != second[i]) {
            return 0;
        }
    }
    return 1;
}

static int verify_histogram(const char *label, const int *histogram, int n) {
    long total = histogram_total(histogram);
    int ok = (total == n);

    printf("  %-22s suma de categorias = %ld / N = %d -> %s\n",
           label, total, n, ok ? "OK" : "ERROR");

    return ok;
}

static void print_histogram(const int *histogram, int n) {
    int max_count = 0;
    for (int i = 0; i < HISTOGRAM_BINS; i++) {
        if (histogram[i] > max_count) {
            max_count = histogram[i];
        }
    }

    printf("\n| %7s | %8s | %7s | Grafico\n", "Rango", "Conteo", "Porc.");
    printf("|---------|----------|---------|------------------------------------------\n");

    for (int i = 0; i < HISTOGRAM_BINS; i++) {
        int low = i * VALUES_PER_BIN;
        int high = low + VALUES_PER_BIN - 1;
        double percentage = n > 0 ? 100.0 * histogram[i] / n : 0.0;
        int bar_length = max_count > 0 ? (int)((long)histogram[i] * 40 / max_count) : 0;

        printf("| %2d-%2d  | %8d | %6.2f%% | ", low, high, histogram[i], percentage);
        for (int j = 0; j < bar_length; j++) {
            putchar('#');
        }
        putchar('\n');
    }
}


static void run_demo(int n) {
    int *array = set_array(n);
    if (array == NULL) {
        fprintf(stderr, "No se pudo reservar memoria para n = %d.\n", n);
        exit(EXIT_FAILURE);
    }

    int serial_histogram[HISTOGRAM_BINS];
    int mutex_histogram[HISTOGRAM_BINS];
    int private_histogram[HISTOGRAM_BINS];
    int thread_count = n < MAX_THREADS ? n : MAX_THREADS;

    double serial_time = measure_execution_time(histogram_serial, array, n, 1, serial_histogram);
    double mutex_time = measure_execution_time(histogram_parallel_mutex, array, n, thread_count, mutex_histogram);
    double private_time = measure_execution_time(histogram_parallel_private, array, n, thread_count, private_histogram);

    printf("=== Histograma de %d valores entre %d y %d (%d threads) ===\n",
           n, MIN_VALUE, MAX_VALUE, thread_count);
    print_histogram(serial_histogram, n);

    printf("\nVerificacion (suma de las 10 categorias == N):\n");
    int serial_ok = verify_histogram("Secuencial", serial_histogram, n);
    int mutex_ok = verify_histogram("Paralelo (mutex)", mutex_histogram, n);
    int private_ok = verify_histogram("Paralelo (privado)", private_histogram, n);

    int mutex_equal = histogram_equal(serial_histogram, mutex_histogram);
    int private_equal = histogram_equal(serial_histogram, private_histogram);

    printf("\nComparacion con la version secuencial:\n");
    printf("  Mutex   == Secuencial: %s\n", mutex_equal ? "Si" : "No");
    printf("  Privado == Secuencial: %s\n", private_equal ? "Si" : "No");

    printf("\nTiempos de ejecucion:\n");
    printf("  Secuencial:          %.3f ms\n", serial_time);
    printf("  Paralelo (mutex):    %.3f ms\n", mutex_time);
    printf("  Paralelo (privado):  %.3f ms\n", private_time);

    if (serial_time > 0.0) {
        printf("\nAceleracion respecto a la version secuencial:\n");
        printf("  Mutex:   %.2fx\n", serial_time / mutex_time);
        printf("  Privado: %.2fx\n", serial_time / private_time);
    }

    if (!serial_ok || !mutex_ok || !private_ok || !mutex_equal || !private_equal) {
        fprintf(stderr, "\nADVERTENCIA: alguna version produjo un histograma incorrecto.\n");
    }

    free(array);
}


static void battery_test(void) {
    int sizes[] = {1000, 10000, 100000, 1000000, 10000000};
    int size_count = (int)(sizeof(sizes) / sizeof(sizes[0]));

    printf("| %10s | %10s | %10s | %10s | %6s | %7s | %-13s |\n",
           "n", "Secuencial", "Mutex", "Privado", "Suma", "Iguales", "Mejor");
    printf("|------------|------------|------------|------------|--------|---------|---------------|\n");

    for (int k = 0; k < size_count; k++) {
        int n = sizes[k];
        int *array = set_array(n);
        if (array == NULL) {
            fprintf(stderr, "No se pudo reservar memoria para n = %d.\n", n);
            exit(EXIT_FAILURE);
        }

        int serial_histogram[HISTOGRAM_BINS];
        int mutex_histogram[HISTOGRAM_BINS];
        int private_histogram[HISTOGRAM_BINS];
        int thread_count = n < MAX_THREADS ? n : MAX_THREADS;

        double serial_time = measure_execution_time(histogram_serial, array, n, 1, serial_histogram);
        double mutex_time = measure_execution_time(histogram_parallel_mutex, array, n, thread_count, mutex_histogram);
        double private_time = measure_execution_time(histogram_parallel_private, array, n, thread_count, private_histogram);
        free(array);

        if (serial_time < 0.0 || mutex_time < 0.0 || private_time < 0.0) {
            exit(EXIT_FAILURE);
        }

        int sums_ok = histogram_total(serial_histogram) == n
            && histogram_total(mutex_histogram) == n
            && histogram_total(private_histogram) == n;
        int equal = histogram_equal(serial_histogram, mutex_histogram)
            && histogram_equal(serial_histogram, private_histogram);

        char best[32];
        if (mutex_time <= private_time) {
            snprintf(best, sizeof(best), "Mutex x%.2f", serial_time / mutex_time);
        } else {
            snprintf(best, sizeof(best), "Privado x%.2f", serial_time / private_time);
        }

        printf("| %10d | %8.3f ms | %8.3f ms | %8.3f ms | %6s | %7s | %-13s |\n",
               n, serial_time, mutex_time, private_time,
               sums_ok ? "OK" : "FALLO", equal ? "Si" : "No", best);
    }
}

int main(int argc, char *argv[]) {
    srand((unsigned int)time(NULL));

    if (argc > 1) {
        int n = atoi(argv[1]);
        if (n <= 0) {
            fprintf(stderr, "Uso: %s [N]  (N debe ser un entero positivo)\n", argv[0]);
            return EXIT_FAILURE;
        }
        run_demo(n);
        return EXIT_SUCCESS;
    }

    battery_test();
    return EXIT_SUCCESS;
}
