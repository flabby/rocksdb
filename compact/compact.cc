#include <iostream>
#include <cstring>
#include <assert.h>
#include <string>

#include "rocksdb/db.h"
#include "rocksdb/utilities/db_ttl.h"

#include "xdebug.h"

using namespace std;

int main(int argc, char **argv)
{
    if (argc != 2) {
        printf ("not enough parameter");
        exit(0);
    }
    std::string path(argv[1]);

    rocksdb::DBWithTTL *db;
    rocksdb::Options options;
    options.create_if_missing = true;
    /*
     * rocksdb::Status status = rocksdb::DB::OpenForReadOnly(options, argv[1], &db);
     */

    rocksdb::Status status = rocksdb::DBWithTTL::Open(options, argv[1], &db);
    assert(status.ok());
    db->CompactRange(NULL, NULL);
    cout << path << endl;
    return 0;
}
