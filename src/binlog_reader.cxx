
#include <libdrizzle-5.1/libdrizzle.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <inttypes.h>



using namespace std;

int main()
{
    drizzle_st *con;
    drizzle_binlog_st *binlog;
    drizzle_options_st *opt;

    drizzle_return_t ret;

    con = drizzle_create("localhost", 3306, "andreasbok", "Ac7Dc4vr", "ocean_test", opt);

    ret = drizzle_connect(con);
    /*

    if (ret != DRIZZLE_RETURN_OK)
    {
        printf("Could not connect to server: %s\n", drizzle_error(con));
        return EXIT_FAILURE;

    }*/

        return EXIT_FAILURE;
}
