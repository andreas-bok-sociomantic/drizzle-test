#include <stdio.h>
#include <stdlib.h>
#include <libdrizzle-5.1/libdrizzle.h>
#include <stdlib.h>
#include <unistd.h>

#define CHECK_ERR(x) (x != DRIZZLE_RETURN_OK? 1 : 0)


#define QUERY_EVENT 2

struct __attribute__((__packed__)) EventHeader
{
    unsigned int timestamp;
    unsigned int type_code: 8;
    unsigned int server_id;
    unsigned int event_length;
    unsigned int next_pos;
    unsigned short flags;

    // no extra headers
};

void dump_event_header(struct EventHeader* hdr)
{
    printf("Header:\n"
           "--------------------------------\n"
           "timestamp: %u\n"
           "type_code: %u\n"
           "server_id: %u\n"
           "event_len: %u\n"
           "next_pos:  %u\n"
           "flags:     %hu\n"
           "--------------------------------\n",
    hdr->timestamp,
    hdr->type_code,
    hdr->server_id,
    hdr->event_length,
    hdr->next_pos,
    hdr->flags);
}


struct __attribute__((__packed__)) QueryEventFixed
{
    unsigned thread_id;
    unsigned time_taken;
    char dbname_len: 8;
    unsigned short error_code: 16;
    unsigned short status_variable_block_len: 16;
};

void dump_query_event_fixed_data(struct QueryEventFixed* fixed)
{
    printf("QueryEvent fixed part:\n"
           "********************************\n"
           "thread_id: %u\n"
           "time_taken: %u\n"
           "dbname_len: %u\n"
           "error_code: %hu\n"
           "var_blk_len:  %hu\n"
           "********************************\n",
    fixed->thread_id,
    fixed->time_taken,
    fixed->dbname_len,
    fixed->error_code,
    fixed->status_variable_block_len);
}

void binlog_error(drizzle_return_t ret, drizzle_st *connection, void *context)
{
    (void)context;

    if (ret != DRIZZLE_RETURN_EOF)
    {
        fprintf(stderr, "Error retrieving binlog: %s\n", drizzle_error(connection));
    }
}

void binlog_event(drizzle_binlog_event_st *event, void *context)
{
    (void) context;
    const unsigned char* data = drizzle_binlog_event_raw_data(event);

    // Get Event header and process it if it's query query event
    struct EventHeader* header = (struct EventHeader*)data;
    dump_event_header(header);

    if (header->type_code == 2)
    {
        struct QueryEventFixed* fixed_data = (struct QueryEventFixed*)(data + sizeof(struct EventHeader));
        dump_query_event_fixed_data(fixed_data);

        // And now let's print the good stuff!
        char* database_name = (char*)(data + sizeof(struct EventHeader) + sizeof(struct QueryEventFixed) + fixed_data->status_variable_block_len);
        printf("Database name: %s\n", database_name);


        size_t statement_len = header->event_length - sizeof(struct EventHeader)
                               - sizeof(struct QueryEventFixed)
                               - fixed_data->status_variable_block_len - fixed_data->dbname_len - 1;

        char* statement = (char*)calloc(statement_len + 1, sizeof(char));

        memcpy(statement, data + sizeof(struct EventHeader) + sizeof(struct QueryEventFixed)
                               + fixed_data->status_variable_block_len + fixed_data->dbname_len + 1,
                statement_len);

        printf("Statement: %s\n",
               statement);

        free(statement);
    }
}

int main()
{
    //drizzle_st* d = drizzle_create("localhost", 3306, "nemanjaboric", "", "sonar_metadata", NULL);
    drizzle_st* d = drizzle_create("localhost", 3306, "ocean_user", "", "ocean_test", NULL);

    if (!d)
    {
        fprintf(stderr, "Error openning connection.\n");
        return -1;
    }

    if (CHECK_ERR(drizzle_connect(d)))
    {
        fprintf(stderr, "Can't connect to server: %s\n", drizzle_error(d));
        return -1;
    }


    fprintf(stderr, "%s@%s:%d (%s)\n", drizzle_user(d), drizzle_host(d),
        drizzle_port(d), drizzle_db(d));

    drizzle_return_t ret;
    drizzle_result_st *result = drizzle_query(d, "SHOW BINARY LOGS", 0, &ret);
    drizzle_row_t row;
    drizzle_result_buffer(result);
    int num_fields = drizzle_result_column_count(result);

    int next_pos = 0;

    fprintf(stderr, "Found %ld binlog files\n", drizzle_result_row_count(result));
    while ((row = drizzle_row_next(result)))
    {
        for (int i = 0; i < num_fields; i++)
        {
            printf("%s ", row[i]);
            next_pos = atol(row[1]);
        }
        printf ("\n");
    }
    next_pos = 415;

    drizzle_binlog_st* binlog = drizzle_binlog_init(d, binlog_event, binlog_error, NULL, true);

    if (!binlog)
    {
        fprintf(stderr, "Drizzle binlog init failure: %s\n", drizzle_error(d));
        return -1;
    }

    ret = drizzle_binlog_start(binlog, 0, "", 0);

    if (ret != DRIZZLE_RETURN_EOF)
    {
        fprintf(stderr, "Drizzle binlog start failure: %s\n", drizzle_error(d));
        return -1;
    }

    drizzle_quit(d);
    return 0;
}
