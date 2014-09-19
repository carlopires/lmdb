/* mtest7.c - memory-mapped database tester/toy */
/* Tests for DB map resize */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "lmdb.h"

#define E(expr) CHECK((rc = (expr)) == MDB_SUCCESS, #expr)
#define EX(err, expr) CHECK((rc = (expr)) == (err), #expr)
#define RES(err, expr) ((rc = expr) == (err) || (CHECK(!rc, #expr), 0))
#define CHECK(test, msg) ((test) ? (void)0 : ((void)fprintf(stderr, \
	"%s:%d: %s: %s\n", __FILE__, __LINE__, msg, mdb_strerror(rc)), abort()))

int main(int argc,char * argv[])
{
	int rc;
	MDB_env *env;
	MDB_dbi dbi;
	MDB_val key, data;
	MDB_txn *txn;
	MDB_envinfo minfo;

	char kval[32];
	char* sval = calloc(1, 8*1024*1024);
	assert(sval);

	// create database
	E(mdb_env_create(&env));
	E(mdb_env_set_mapsize(env, 10*1024*1024));
	E(mdb_env_set_maxdbs(env, 0));
	E(mdb_env_open(env, "./testdb", MDB_NORDAHEAD|MDB_NOLOCK|MDB_NOSYNC, 0664));

	// starts dbi
	E(mdb_txn_begin(env, NULL, 0, &txn));
	E(mdb_open(txn, NULL, MDB_CREATE, &dbi));
	E(mdb_txn_commit(txn));

	// check database mapsize
	E(mdb_env_info(env, &minfo));
	printf("map size is %ldMB\n", minfo.me_mapsize/1048576);

	// try to insert data
	E(mdb_txn_begin(env, NULL, 0, &txn));

	key.mv_size = sizeof(kval);
	key.mv_data = &kval;
	data.mv_size = 8*1024*1024;
	data.mv_data = sval;

	sprintf(kval, "0");
	E(mdb_put(txn, dbi, &key, &data, 0));

	sprintf(kval, "1");
	EX(MDB_MAP_FULL, mdb_put(txn, dbi, &key, &data, 0));

	// abort transaction and increase map size
	mdb_txn_abort(txn);
	mdb_env_set_mapsize(env, 20*1024*1024);

	// check database mapsize
	E(mdb_env_info(env, &minfo));
	printf("new map size is %ldMB\n", minfo.me_mapsize/1048576);

	// try to insert data again
	E(mdb_txn_begin(env, NULL, 0, &txn));

	sprintf(kval, "0");
	E(mdb_put(txn, dbi, &key, &data, 0));

	sprintf(kval, "1");
	E(mdb_put(txn, dbi, &key, &data, 0));

	E(mdb_txn_commit(txn));
	mdb_env_close(env);

	free(sval);
	return 0;
}
