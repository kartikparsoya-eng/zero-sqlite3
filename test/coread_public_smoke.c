/*
** WAL2 co-located-read PUBLIC API smoke test.
**
** Unlike coread_smoke.c (which reaches the SQLITE_PRIVATE wal functions
** directly), this drives the public sqlite3_wal2_coread_get/open/free chain
** end-to-end -- the exact surface the Go cgo binding (coread.go) calls. It
** proves the public -> pager -> wal layering wires up and that a second
** connection latches the anchor's frame through the public API.
**
**   A (anchor): BEGIN + warm read -> pins a frame; sqlite3_wal2_coread_get.
**   W (writer): INSERT            -> advances the head past A's frame.
**   B (coread): BEGIN + sqlite3_wal2_coread_open(co) -> MUST see A's OLD
**               snapshot (1 row), not the writer's new row (2).
**
** Build (against either amalgamation copy):
**   cc -DSQLITE_ENABLE_WAL2_COREAD -DSQLITE_THREADSAFE=1 \
**      -I<dir-with-sqlite3.c> coread_public_smoke.c -o /tmp/x -lpthread
*/
#include "sqlite3.c"
#include <stdio.h>

static int xexec(sqlite3 *db, const char *sql){
  char *e=0; int rc=sqlite3_exec(db, sql, 0,0,&e);
  if( rc!=SQLITE_OK ) printf("  exec rc=%d (%s) <- %s\n", rc, e?e:"-", sql);
  return rc;
}
static int countRows(sqlite3 *db){
  sqlite3_stmt *st; int n=-1;
  if( sqlite3_prepare_v2(db,"SELECT count(*) FROM t",-1,&st,0)==SQLITE_OK ){
    if( sqlite3_step(st)==SQLITE_ROW ) n=sqlite3_column_int(st,0);
    sqlite3_finalize(st);
  }
  return n;
}

int main(void){
  setvbuf(stdout, 0, _IONBF, 0);
  sqlite3 *W=0,*A=0,*B=0;
  remove("/tmp/cop.db"); remove("/tmp/cop.db-wal"); remove("/tmp/cop.db-wal2"); remove("/tmp/cop.db-shm");

  /* Seed in wal2. */
  if( sqlite3_open("/tmp/cop.db",&W)!=SQLITE_OK ) return 2;
  xexec(W,"PRAGMA journal_mode=wal2;");
  xexec(W,"CREATE TABLE t(x);");
  xexec(W,"INSERT INTO t VALUES(1);");
  printf("seed: W sees %d row(s)\n", countRows(W));

  /* Anchor A: BEGIN + warm read pins A's frame at 1 row, then capture. */
  if( sqlite3_open("/tmp/cop.db",&A)!=SQLITE_OK ) return 2;
  if( xexec(A,"BEGIN;")!=SQLITE_OK ) return 2;
  printf("A pinned at %d row(s)\n", countRows(A));
  sqlite3_wal2_coread *co=0;
  int rc = sqlite3_wal2_coread_get(A, "main", &co);
  printf("sqlite3_wal2_coread_get rc=%d co=%p\n", rc, (void*)co);
  if( rc!=SQLITE_OK || co==0 ) return 3;

  /* Writer advances the head past A's frame. */
  xexec(W,"INSERT INTO t VALUES(2);");
  printf("writer advanced: W sees %d row(s)\n", countRows(W));

  /* Co-located reader B: a throwaway autocommit read opens B's Wal lazily,
  ** then BEGIN + coread_open latches B onto A's captured frame. */
  if( sqlite3_open("/tmp/cop.db",&B)!=SQLITE_OK ) return 2;
  printf("B warmup read sees %d row(s)\n", countRows(B));
  if( xexec(B,"BEGIN;")!=SQLITE_OK ) return 2;
  rc = sqlite3_wal2_coread_open(B, "main", co);
  int bCount = countRows(B);
  printf("B (co-located at A's frame) open rc=%d, sees %d row(s)\n", rc, bCount);

  xexec(A,"COMMIT;"); xexec(B,"COMMIT;");
  sqlite3_wal2_coread_free(co);
  sqlite3_close(A); sqlite3_close(B); sqlite3_close(W);

  if( rc==SQLITE_OK && bCount==1 ){
    printf("\nPASS: public co-read API latched the anchor's frame (saw 1, not 2)\n");
    return 0;
  }
  printf("\nFAIL: rc=%d bCount=%d (want rc=0 bCount=1)\n", rc, bCount);
  return 1;
}
