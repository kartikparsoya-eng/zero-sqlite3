/*
** WAL2 co-located-read smoke test (experimental scaffold).
**
** Compiles the amalgamation INTO this TU so the SQLITE_PRIVATE
** sqlite3WalCoRead* functions are callable directly (no public API yet).
**
**   A (anchor): BEGIN+read  -> pins a frame; we capture it.
**   W (writer): INSERT      -> advances the head past A's frame.
**   B (coread): armed with A's capture, BEGIN+read -> MUST see A's OLD
**               snapshot (1 row), not the writer's new row (2).
**
** Build: cc -DSQLITE_ENABLE_WAL2_COREAD -DSQLITE_THREADSAFE=1 \
**           -I../deps/sqlite3 coread_smoke.c -o /tmp/coread_smoke -lpthread
*/
#include "sqlite3.c"
#include <stdio.h>

static Wal *dbWal(sqlite3 *db){
  Pager *pPager = sqlite3BtreePager(db->aDb[0].pBt);
  return pPager->pWal;
}
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
  remove("/tmp/co.db"); remove("/tmp/co.db-wal"); remove("/tmp/co.db-wal2"); remove("/tmp/co.db-shm");

  /* Seed in wal2 (must be set from the initial mode). */
  if( sqlite3_open("/tmp/co.db",&W)!=SQLITE_OK ) return 2;
  xexec(W,"PRAGMA journal_mode=wal2;");
  xexec(W,"CREATE TABLE t(x);");
  xexec(W,"INSERT INTO t VALUES(1);");
  printf("seed: W sees %d row(s), wal2=%d\n", countRows(W), isWalMode2(dbWal(W)));

  /* Anchor A: open a read transaction -> pins A's frame at 1 row. */
  if( sqlite3_open("/tmp/co.db",&A)!=SQLITE_OK ) return 2;
  if( xexec(A,"BEGIN;")!=SQLITE_OK ) return 2;
  printf("A pinned at %d row(s)\n", countRows(A));

  /* Capture A's read point. */
  WalCoRead co;
  int rc = sqlite3WalCoReadGet(dbWal(A), &co);
  printf("sqlite3WalCoReadGet rc=%d eLock=%d\n", rc, co.eLock);
  if( rc!=SQLITE_OK ) return 3;

  /* Writer advances the head past A's frame. */
  xexec(W,"INSERT INTO t VALUES(2);");
  printf("writer advanced: W sees %d row(s)\n", countRows(W));

  /* Co-located reader B: a throwaway read first forces B's pager to open
  ** its Wal (lazy), so dbWal(B) is non-NULL before we arm it. */
  if( sqlite3_open("/tmp/co.db",&B)!=SQLITE_OK ) return 2;
  printf("B warmup read sees %d row(s)\n", countRows(B));
  if( dbWal(B)==0 ){ printf("B Wal still NULL\n"); return 4; }
  sqlite3WalCoReadOpen(dbWal(B), &co);
  rc = xexec(B,"BEGIN;");
  int bCount = countRows(B);
  sqlite3WalCoReadOpen(dbWal(B), 0);  /* disarm */
  printf("B (co-located at A's frame) BEGIN rc=%d, sees %d row(s)\n", rc, bCount);

  xexec(A,"COMMIT;"); xexec(B,"COMMIT;");
  sqlite3_close(A); sqlite3_close(B); sqlite3_close(W);

  if( rc==SQLITE_OK && bCount==1 ){
    printf("\nPASS: co-located reader latched the anchor's frame (saw 1, not 2)\n");
    return 0;
  }
  printf("\nFAIL: rc=%d bCount=%d (want rc=0 bCount=1)\n", rc, bCount);
  return 1;
}
