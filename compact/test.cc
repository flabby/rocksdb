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
    options.merge_operator = std::make_shared<rocksdb::TtlMergeOperator>();
    //options.max_successive_merges = max_successive_merges;
    //options.min_partial_merge_operands = min_partial_merge_operands;

    /*
     * rocksdb::Status status = rocksdb::DB::OpenForReadOnly(options, argv[1], &db);
     */

    rocksdb::Status status = rocksdb::DBWithTTL::Open(options, argv[1], &db, '\0');
    assert(status.ok());

    status = db->Put(rocksdb::WriteOptions(), "setkey1", "setval1");
    assert(status.ok());

    status = db->Merge(rocksdb::WriteOptions(), "a", "1");
    assert(status.ok());

    db->CompactRange(NULL, NULL);
    cout << path << endl;
    return 0;
}
