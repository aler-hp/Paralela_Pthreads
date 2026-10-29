
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <pthread.h>
#include <semaphore.h>
#include <time.h>

static const int DEFAULT_ITEMS_PER_PRODUCER = 20000;
static const int DEFAULT_PRODUCERS = 2;
static const int DEFAULT_CONSUMERS = 2;
static const int DEFAULT_CAPACITY = 8;


typedef struct {
    int *items;
    int capacity;
    int in;
    int out;
    int count;
    int max_count;
    long total_produced;
    long total_consumed;
    long total_items;
    pthread_mutex_t mutex;
    sem_t empty;            /* espacios libres */
    sem_t full;             /* elementos disponibles */
} BoundedBuffer;

typedef struct {
    BoundedBuffer *buffer;
    int id;
    int items_to_produce;
    int values_base;        /* primer valor unico */
    long produced;
} ProducerData;

typedef struct {
    BoundedBuffer *buffer;
    int id;
    long consumed;
    unsigned char *seen;    /* valores extraidos por este consumidor */
} ConsumerData;

typedef struct {
    long produced;
    long consumed;
    long missing;
    long duplicates;
    int max_occupancy;
    double elapsed_ms;
    int ok;
} RunSummary;

static int buffer_init(BoundedBuffer *buffer, int capacity, long total_items) {
    memset(buffer, 0, sizeof(*buffer));

    buffer->items = (int *)malloc((size_t)capacity * sizeof(*buffer->items));
    if (buffer->items == NULL) {
        return 0;
    }

    buffer->capacity = capacity;
    buffer->total_items = total_items;

    if (pthread_mutex_init(&buffer->mutex, NULL) != 0) {
        free(buffer->items);
        buffer->items = NULL;
        return 0;
    }
    if (sem_init(&buffer->empty, 0, (unsigned int)capacity) != 0) {
        pthread_mutex_destroy(&buffer->mutex);
        free(buffer->items);
        buffer->items = NULL;
        return 0;
    }
    if (sem_init(&buffer->full, 0, 0) != 0) {
        sem_destroy(&buffer->empty);
        pthread_mutex_destroy(&buffer->mutex);
        free(buffer->items);
        buffer->items = NULL;
        return 0;
    }

    return 1;
}

static void buffer_destroy(BoundedBuffer *buffer) {
    sem_destroy(&buffer->full);
    sem_destroy(&buffer->empty);
    pthread_mutex_destroy(&buffer->mutex);
    free(buffer->items);
    buffer->items = NULL;
}


static void *producer_worker(void *argument) {
    ProducerData *data = (ProducerData *)argument;
    BoundedBuffer *buffer = data->buffer;

    for (int i = 0; i < data->items_to_produce; i++) {
        int value = data->values_base + i;

        /* espera un espacio libre */
        sem_wait(&buffer->empty);

        pthread_mutex_lock(&buffer->mutex);
        buffer->items[buffer->in] = value;
        buffer->in = (buffer->in + 1) % buffer->capacity;
        buffer->count++;
        if (buffer->count > buffer->max_count) {
            buffer->max_count = buffer->count;
        }
        buffer->total_produced++;
        pthread_mutex_unlock(&buffer->mutex);

        sem_post(&buffer->full);

        data->produced++;
    }

    return NULL;
}

static void *consumer_worker(void *argument) {
    ConsumerData *data = (ConsumerData *)argument;
    BoundedBuffer *buffer = data->buffer;

    for (;;) {
        /* espera un elemento */
        sem_wait(&buffer->full);

        pthread_mutex_lock(&buffer->mutex);

        if (buffer->total_consumed >= buffer->total_items) {
            /* token de fin */
            pthread_mutex_unlock(&buffer->mutex);
            break;
        }

        int value = buffer->items[buffer->out];
        buffer->out = (buffer->out + 1) % buffer->capacity;
        buffer->count--;
        buffer->total_consumed++;
        pthread_mutex_unlock(&buffer->mutex);

        sem_post(&buffer->empty);

        /* privado: sin carrera */
        data->seen[value]++;
        data->consumed++;
    }

    return NULL;
}


static int get_current_time(struct timespec *timestamp) {
#if defined(_WIN32) && !defined(_UCRT)
    return clock_gettime(CLOCK_MONOTONIC, timestamp) == 0;
#else
    return timespec_get(timestamp, TIME_UTC) == TIME_UTC;
#endif
}


static int run_configuration(int items_per_producer, int producers, int consumers,
                             int capacity, int verbose, RunSummary *summary) {
    long total_items = (long)items_per_producer * producers;

    BoundedBuffer buffer;
    if (!buffer_init(&buffer, capacity, total_items)) {
        fprintf(stderr, "No se pudo inicializar el buffer.\n");
        return 0;
    }

    ProducerData *producer_data = (ProducerData *)calloc((size_t)producers, sizeof(*producer_data));
    ConsumerData *consumer_data = (ConsumerData *)calloc((size_t)consumers, sizeof(*consumer_data));
    pthread_t *producer_threads = (pthread_t *)calloc((size_t)producers, sizeof(*producer_threads));
    pthread_t *consumer_threads = (pthread_t *)calloc((size_t)consumers, sizeof(*consumer_threads));

    if (producer_data == NULL || consumer_data == NULL
        || producer_threads == NULL || consumer_threads == NULL) {
        fprintf(stderr, "No se pudo reservar memoria para los threads.\n");
        free(producer_data);
        free(consumer_data);
        free(producer_threads);
        free(consumer_threads);
        buffer_destroy(&buffer);
        return 0;
    }

    int allocation_ok = 1;
    for (int i = 0; i < consumers; i++) {
        consumer_data[i].buffer = &buffer;
        consumer_data[i].id = i;
        consumer_data[i].seen = (unsigned char *)calloc((size_t)total_items, 1);
        if (consumer_data[i].seen == NULL) {
            allocation_ok = 0;
            fprintf(stderr, "No se pudo reservar el arreglo de verificacion.\n");
            break;
        }
    }

    if (!allocation_ok) {
        for (int i = 0; i < consumers; i++) {
            free(consumer_data[i].seen);
        }
        free(producer_data);
        free(consumer_data);
        free(producer_threads);
        free(consumer_threads);
        buffer_destroy(&buffer);
        return 0;
    }

    for (int i = 0; i < producers; i++) {
        producer_data[i].buffer = &buffer;
        producer_data[i].id = i;
        producer_data[i].items_to_produce = items_per_producer;
        producer_data[i].values_base = i * items_per_producer;
    }

    struct timespec start;
    struct timespec end;
    if (!get_current_time(&start)) {
        fprintf(stderr, "No se pudo leer el tiempo inicial.\n");
    }

    for (int i = 0; i < producers; i++) {
        if (pthread_create(&producer_threads[i], NULL, producer_worker, &producer_data[i]) != 0) {
            fprintf(stderr, "No se pudo crear el productor %d.\n", i);
            exit(EXIT_FAILURE);
        }
    }
    for (int i = 0; i < consumers; i++) {
        if (pthread_create(&consumer_threads[i], NULL, consumer_worker, &consumer_data[i]) != 0) {
            fprintf(stderr, "No se pudo crear el consumidor %d.\n", i);
            exit(EXIT_FAILURE);
        }
    }

    for (int i = 0; i < producers; i++) {
        pthread_join(producer_threads[i], NULL);
    }

    /* un token de fin por consumidor */
    for (int i = 0; i < consumers; i++) {
        sem_post(&buffer.full);
    }

    for (int i = 0; i < consumers; i++) {
        pthread_join(consumer_threads[i], NULL);
    }

    double elapsed_ms = -1.0;
    if (get_current_time(&end)) {
        elapsed_ms = (end.tv_sec - start.tv_sec) * 1000.0
            + (end.tv_nsec - start.tv_nsec) / 1000000.0;
    }

    /* cada valor exactamente una vez */
    long missing = 0;
    long duplicates = 0;
    for (long value = 0; value < total_items; value++) {
        int copies = 0;
        for (int c = 0; c < consumers; c++) {
            copies += consumer_data[c].seen[value];
        }
        if (copies == 0) {
            missing++;
        } else if (copies > 1) {
            duplicates += copies - 1;
        }
    }

    int produced_ok = 1;
    for (int i = 0; i < producers; i++) {
        if (producer_data[i].produced != items_per_producer) {
            produced_ok = 0;
        }
    }

    long consumed_by_threads = 0;
    for (int i = 0; i < consumers; i++) {
        consumed_by_threads += consumer_data[i].consumed;
    }

    int ok = produced_ok
        && buffer.total_produced == total_items
        && buffer.total_consumed == total_items
        && consumed_by_threads == total_items
        && missing == 0
        && duplicates == 0
        && buffer.count == 0;

    summary->produced = buffer.total_produced;
    summary->consumed = buffer.total_consumed;
    summary->missing = missing;
    summary->duplicates = duplicates;
    summary->max_occupancy = buffer.max_count;
    summary->elapsed_ms = elapsed_ms;
    summary->ok = ok;

    if (verbose) {
        printf("=== Productores y consumidores con buffer acotado ===\n");
        printf("Items por productor: %d\n", items_per_producer);
        printf("Productores: %d\n", producers);
        printf("Consumidores: %d\n", consumers);
        printf("Capacidad del buffer: %d\n", capacity);
        printf("Items totales esperados: %ld\n", total_items);
        printf("Tiempo total: %.3f ms\n", elapsed_ms);
        printf("Ocupacion maxima del buffer: %d / %d\n\n",
               summary->max_occupancy, capacity);

        printf("Elementos producidos por thread:\n");
        for (int i = 0; i < producers; i++) {
            printf("  Productor %d: %ld\n", i, producer_data[i].produced);
        }

        printf("Elementos consumidos por thread:\n");
        for (int i = 0; i < consumers; i++) {
            printf("  Consumidor %d: %ld\n", i, consumer_data[i].consumed);
        }

        printf("\nVerificacion:\n");
        printf("  Producidos:  %ld / %ld -> %s\n", buffer.total_produced, total_items,
               buffer.total_produced == total_items ? "OK" : "ERROR");
        printf("  Consumidos:  %ld / %ld -> %s\n", buffer.total_consumed, total_items,
               buffer.total_consumed == total_items ? "OK" : "ERROR");
        printf("  Faltantes:   %ld -> %s\n", missing, missing == 0 ? "OK" : "ERROR");
        printf("  Duplicados:  %ld -> %s\n", duplicates, duplicates == 0 ? "OK" : "ERROR");
        printf("  Buffer final: %d elementos -> %s\n", buffer.count,
               buffer.count == 0 ? "OK" : "ERROR");
        printf("  Resultado: %s\n", ok ? "CORRECTO" : "INCORRECTO");
    }

    for (int i = 0; i < consumers; i++) {
        free(consumer_data[i].seen);
    }
    free(producer_data);
    free(consumer_data);
    free(producer_threads);
    free(consumer_threads);
    buffer_destroy(&buffer);

    return ok;
}


typedef struct {
    int items_per_producer;
    int producers;
    int consumers;
    int capacity;
} TestConfig;

static void battery_test(void) {
    TestConfig configs[] = {
        {DEFAULT_ITEMS_PER_PRODUCER, 2, 2, 1},
        {DEFAULT_ITEMS_PER_PRODUCER, 2, 2, 4},
        {DEFAULT_ITEMS_PER_PRODUCER, 2, 2, 64},
        {DEFAULT_ITEMS_PER_PRODUCER, 4, 4, 8},
        {DEFAULT_ITEMS_PER_PRODUCER, 8, 2, 8},
        {DEFAULT_ITEMS_PER_PRODUCER, 2, 8, 8},
        {DEFAULT_ITEMS_PER_PRODUCER, 4, 4, 100000}
    };
    int config_count = (int)(sizeof(configs) / sizeof(configs[0]));
    int all_ok = 1;

    printf("| %2s | %2s | %8s | %7s | %10s | %10s | %9s | %10s | %9s | %-8s |\n",
           "P", "C", "Buffer", "Items", "Producidos", "Consumidos",
           "Faltantes", "Duplicados", "Tiempo", "Estado");
    printf("|----|----|----------|---------|------------|------------|-----------|------------|-----------|----------|\n");

    for (int i = 0; i < config_count; i++) {
        RunSummary summary;
        int ok = run_configuration(configs[i].items_per_producer,
                                   configs[i].producers,
                                   configs[i].consumers,
                                   configs[i].capacity,
                                   0, &summary);

        long total_items = (long)configs[i].items_per_producer * configs[i].producers;

        printf("| %2d | %2d | %8d | %7ld | %10ld | %10ld | %9ld | %10ld | %6.3f ms | %-8s |\n",
               configs[i].producers, configs[i].consumers, configs[i].capacity,
               total_items, summary.produced, summary.consumed,
               summary.missing, summary.duplicates, summary.elapsed_ms,
               ok ? "OK" : "FALLO");

        if (!ok) {
            all_ok = 0;
        }
    }

    if (all_ok) {
        printf("\nTodas las configuraciones son correctas: no se perdieron ni se duplicaron elementos.\n");
    } else {
        printf("\nADVERTENCIA: alguna configuracion produjo resultados incorrectos.\n");
    }
}


int main(int argc, char *argv[]) {
    if (argc > 5) {
        fprintf(stderr, "Uso: %s [items_por_productor] [productores] [consumidores] [capacidad]\n",
                argv[0]);
        return EXIT_FAILURE;
    }

    if (argc == 1) {
        battery_test();
        return EXIT_SUCCESS;
    }

    int items_per_producer = argc > 1 ? atoi(argv[1]) : DEFAULT_ITEMS_PER_PRODUCER;
    int producers = argc > 2 ? atoi(argv[2]) : DEFAULT_PRODUCERS;
    int consumers = argc > 3 ? atoi(argv[3]) : DEFAULT_CONSUMERS;
    int capacity = argc > 4 ? atoi(argv[4]) : DEFAULT_CAPACITY;

    if (items_per_producer <= 0 || producers <= 0 || consumers <= 0 || capacity <= 0) {
        fprintf(stderr, "Todos los parametros deben ser enteros positivos.\n");
        return EXIT_FAILURE;
    }
    if ((long)items_per_producer * producers > INT_MAX) {
        fprintf(stderr, "La cantidad total de items es demasiado grande.\n");
        return EXIT_FAILURE;
    }

    RunSummary summary;
    int ok = run_configuration(items_per_producer, producers, consumers, capacity, 1, &summary);

    return ok ? EXIT_SUCCESS : EXIT_FAILURE;
}
