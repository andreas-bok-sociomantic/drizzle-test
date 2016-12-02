#include <stdio.h>
#include <stdlib.h>
#include <libdrizzle-5.1/libdrizzle.h>
#include <stdlib.h>
#include <iostream>
#include <sstream>

#include <unistd.h>

using namespace std;

#define CHECK_ERR(x) (x != DRIZZLE_RETURN_OK? 1 : 0)


#define QUERY_EVENT 2

#define drizzle_get_byte1(__buffer)               \
            ((((uint8_t *)__buffer)[0]))

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

/*
Binlog error callback function

Triggered when an error occurs
*/
void binlog_error(drizzle_return_t ret, drizzle_st *connection, void *context)
{
    (void)context;

    if (ret != DRIZZLE_RETURN_EOF)
    {
        fprintf(stderr, "Error retrieving binlog: %s\n", drizzle_error(connection));
    }
}


/*

Binlog event callback function

Triggered when a new event is available to retrieve

*/

void print_header(drizzle_binlog_event_st *event, void *context)
{
    fprintf(stderr, "Header\n");
    fprintf(stderr, "Timestamp : %u\n", drizzle_binlog_event_timestamp(event));
    fprintf(stderr, "Server id : %u\n", drizzle_binlog_event_server_id(event));
    fprintf(stderr, "Type code : %u\n", drizzle_binlog_event_type(event));
    fprintf(stderr, "Next-pos  : %u\n", drizzle_binlog_event_next_pos(event));
}

void print_event(drizzle_binlog_event_st *event, void *context)
{

    const unsigned char* data = drizzle_binlog_event_data(event);
    /*(void) context;

    struct EventHeader* header = (struct EventHeader*)data;
    dump_event_header(header);*/
    print_header(event, context);

    // 52

    fprintf(stderr, "Event\n");
    const unsigned char* event_ptr = data;
    fprintf(stderr, "binlog version %u\n",  drizzle_get_byte2(event_ptr));
    event_ptr += 50;
    fprintf(stderr, "create_time : %d\n",  drizzle_get_byte4(event_ptr));
    fprintf(stderr, "header_length : %d\n", drizzle_get_byte1(++event_ptr));

    uint length = drizzle_binlog_event_length(event);

/*    printf("event_length : %u\n", length);
    printf("Data: 0x");
    for (; *event_ptr != NULL; event_ptr++  )
    {
        printf("%02X ", *event_ptr);
    }*/

    for (uint i=0; i<length - 20; i++)
        printf("%02X ", event_ptr[i]);

    printf("\n\n");
}

void callback_binlog_event(drizzle_binlog_event_st *event, void *context)
{
    bool dump_event = false;
    switch (drizzle_binlog_event_type(event))
    {

        case DRIZZLE_EVENT_TYPE_QUERY :
            fprintf(stderr, "query rows\n");
            dump_event = true;
            break;
        case DRIZZLE_EVENT_TYPE_V1_WRITE_ROWS :
        case DRIZZLE_EVENT_TYPE_V2_WRITE_ROWS :
            fprintf(stderr, "write rows\n");
            dump_event = true;
            break ;

        case DRIZZLE_EVENT_TYPE_V1_DELETE_ROWS :
        case DRIZZLE_EVENT_TYPE_V2_DELETE_ROWS :
            dump_event = true;
            fprintf(stderr, "delete rows\n");
            break;

        case DRIZZLE_EVENT_TYPE_V1_UPDATE_ROWS :
        case DRIZZLE_EVENT_TYPE_V2_UPDATE_ROWS :
            dump_event = true;
            fprintf(stderr, "update rows\n");
            break;
        case DRIZZLE_EVENT_TYPE_INTVAR :
            dump_event = true;
            fprintf(stderr, "update int_var\n");
            /* Written every time a statement uses an AUTO_INCREMENT column or
            the LAST_INSERT_ID() function; precedes other events for the statement.
            This is written only before a QUERY_EVENT and is not used with
            row-based logging. An INTVAR_EVENT is written with a "subtype" in
            the event data part:
                INSERT_ID_EVENT indicates the value to use for an AUTO_INCREMENT
                column in the next statement.

                LAST_INSERT_ID_EVENT indicates the value to use for the
                LAST_INSERT_ID() function in the next statement.
*/
        default :
            break;
    }
    if (dump_event)
    {
        print_event(event, context);
    }

}

int main(int argc, char* argv[] )
{

    char* db_name = "ocean_test";
    if (argc > 1)
    {
        db_name = argv[1];
    }
    //drizzle_st* d = drizzle_create("localhost", 3306, "nemanjaboric", "", "sonar_metadata", NULL);
    drizzle_st* d = drizzle_create("localhost", 3306, "andreasbok", "", db_name, NULL);

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

    fprintf(stderr, "MySQL %s\n", drizzle_server_version(d));


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

    drizzle_binlog_st* binlog = drizzle_binlog_init(d, callback_binlog_event, binlog_error, NULL, true);

    if (!binlog)
    {
        fprintf(stderr, "Drizzle binlog init failure: %s\n", drizzle_error(d));
        return -1;
    }

    ret = drizzle_binlog_start(binlog, 1, "", 0);

    if (ret != DRIZZLE_RETURN_EOF)
    {
        fprintf(stderr, "Drizzle binlog start failure: %s\n", drizzle_error(d));
        return -1;
    }

    drizzle_quit(d);
    return 0;
}
