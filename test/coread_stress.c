/*
** WAL2 co-located-read STRESS / corruption gauntlet (experimental scaffold).
**
** A background writer hammers INSERTs + periodic checkpoints (forcing wal2
** file rotations + resets) while, in a tight loop, an anchor pins a frame and
** N co-located readers repeatedly verify they see a STABLE, INTERNALLY
** CONSISTENT snapshot at that frame.
**
** Row invariant: the writer inserts x = max(x)+1, so a snapshot with count=c
** always has rows {1..c}: max(x)==c AND sum(x)==c*(c+1)/2. A co-located reader
** pinned to the anchor's frame (count=Na) must therefore see, on EVERY read:
**   - rc==SQLITE_OK, count==Na, max==Na, sum==Na*(Na+1)/2   -> latch held  (good)
**   - OR rc==SQLITE_ERROR_SNAPSHOT (the file rotated; fail-closed)          -> fallback (ok)
** Anything else is a bug:
**   - count==Na but sum/max inconsistent -> TORN READ / CORRUPTION (hard fail)
**   - count!=Na (some other consistent snapshot) -> LATCH MISS (bug)
**   - any other rc / SQLITE_CORRUPT -> hard fail
**
** Build: cc -O1 -DSQLITE_ENABLE_WAL2_COREAD -DSQLITE_THREADSAFE=1 \
**           -I../deps/sqlite3 coread_stress.c -o /tmp/coread_stress -lpthread
** Run:   /tmp/coread_stress [seconds] [nReaders] [readsPerCycle]
*/
#include "sqlite3.c"
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define DB "/tmp/costress.db"

static volatile int g_stop = 0;

/* global tallies (gcc atomics) */
static long g_latchHeld=0, g_fallback=0, g_latchMiss=0, g_corrupt=0, g_otherErr=0, g_anchors=0, g_writes=0, g_ckpts=0;
#define INC(x) __sync_fetch_and_add(&(x),1)

static Wal *dbWal(sqlite3 *db){
  Pager *p = sqlite3BtreePager(db->aDb[0].pBt);
  return p->pWal;
}

/* Read count, max(x), sum(x) in ONE statement (one consistent point). Returns
** the sqlite rc; pc, pmax, psum set on SQLITE_ROW. */
static int snap(sqlite3 *db, long *pc, long *pmax, long *psum){
  sqlite3_stmt *st; int rc;
  rc = sqlite3_prepare_v2(db, "SELECT count(*), coalesce(max(x),0), coalesce(sum(x),0) FROM t", -1, &st, 0);
  if( rc!=SQLITE_OK ) return rc;
  rc = sqlite3_step(st);
  if( rc==SQLITE_ROW ){
    *pc  = sqlite3_column_int64(st,0);
    *pmax= sqlite3_column_int64(st,1);
    *psum= sqlite3_column_int64(st,2);
    rc = SQLITE_OK;
  }
  sqlite3_finalize(st);
  return rc;
}

/* Background writer: insert x=max+1, checkpoint(RESTART) periodically to force
** wal2 rotation + file reset (the dangerous case for a held co-read). */
static void *writer(void *arg){
  (void)arg;
  sqlite3 *W=0;
  if( sqlite3_open(DB,&W)!=SQLITE_OK ) return 0;
  long n=0;
  while( !g_stop ){
    if( sqlite3_exec(W,"INSERT INTO t(x) VALUES((SELECT coalesce(max(x),0)+1 FROM t))",0,0,0)==SQLITE_OK ) INC(g_writes);
    if( (++n % 137)==0 ){ sqlite3_exec(W,"PRAGMA wal_checkpoint(RESTART)",0,0,0); INC(g_ckpts); }
  }
  sqlite3_close(W);
  return 0;
}

/* One co-located reader: latch the anchor's frame, read it `reads` times,
** verifying the invariant on each. */
typedef struct { WalCoRead co; long Na; int reads; } ReaderArg;
static void *reader(void *arg){
  ReaderArg *a = (ReaderArg*)arg;
  sqlite3 *B=0;
  if( sqlite3_open(DB,&B)!=SQLITE_OK ) return 0;
  long c,mx,sm;
  snap(B,&c,&mx,&sm);              /* warmup: force B's pager to open its Wal */
  if( dbWal(B)==0 ){ sqlite3_close(B); return 0; }
  for(int i=0;i<a->reads && !g_stop;i++){
    sqlite3WalCoReadOpen(dbWal(B), &a->co);
    int rc = sqlite3_exec(B,"BEGIN",0,0,0);
    if( rc==SQLITE_OK ) rc = snap(B,&c,&mx,&sm);
    sqlite3WalCoReadOpen(dbWal(B), 0);   /* disarm */
    if( rc==SQLITE_OK ){
      long want_sum = a->Na*(a->Na+1)/2;
      if( c==a->Na && mx==a->Na && sm==want_sum )      INC(g_latchHeld);
      else if( c==a->Na )                              INC(g_corrupt);   /* torn read */
      else if( mx==c && sm==c*(c+1)/2 )                INC(g_latchMiss); /* consistent but wrong frame */
      else                                             INC(g_corrupt);   /* inconsistent */
    } else if( (rc&0xFF)==SQLITE_ERROR ){              /* SQLITE_ERROR_SNAPSHOT family */
      INC(g_fallback);
    } else if( (rc&0xFF)==SQLITE_CORRUPT ){
      INC(g_corrupt);
    } else {
      INC(g_otherErr);
    }
    sqlite3_exec(B,"COMMIT",0,0,0);
    sqlite3_exec(B,"ROLLBACK",0,0,0); /* in case BEGIN failed, leave no open tx */
  }
  sqlite3_close(B);
  return 0;
}

int main(int argc, char **argv){
  setvbuf(stdout,0,_IONBF,0);
  int secs   = argc>1 ? atoi(argv[1]) : 5;
  int nRead  = argc>2 ? atoi(argv[2]) : 4;
  int reads  = argc>3 ? atoi(argv[3]) : 40;

  remove(DB); remove(DB"-wal"); remove(DB"-wal2"); remove(DB"-shm");
  sqlite3 *S=0; sqlite3_open(DB,&S);
  sqlite3_exec(S,"PRAGMA journal_mode=wal2;",0,0,0);
  sqlite3_exec(S,"PRAGMA wal_autocheckpoint=0;",0,0,0);  /* we drive checkpoints */
  sqlite3_exec(S,"CREATE TABLE t(x INTEGER);",0,0,0);
  sqlite3_exec(S,"INSERT INTO t(x) VALUES(1);",0,0,0);
  if( !isWalMode2(dbWal(S)) ){ printf("not wal2; abort\n"); return 2; }
  sqlite3_close(S);

  pthread_t wt; pthread_create(&wt,0,writer,0);

  time_t end = time(0)+secs;
  while( time(0) < end ){
    /* Anchor A: open a read tx + pin its frame. Stays open for the whole
    ** co-read window (the safety contract). */
    sqlite3 *A=0; if( sqlite3_open(DB,&A)!=SQLITE_OK ) break;
    long Na,Amx,Asm;
    if( sqlite3_exec(A,"BEGIN",0,0,0)!=SQLITE_OK || snap(A,&Na,&Amx,&Asm)!=SQLITE_OK ){ sqlite3_close(A); continue; }
    WalCoRead co;
    if( sqlite3WalCoReadGet(dbWal(A),&co)!=SQLITE_OK ){ sqlite3_exec(A,"COMMIT",0,0,0); sqlite3_close(A); continue; }
    INC(g_anchors);

    pthread_t rt[16]; ReaderArg ra[16];
    if( nRead>16 ) nRead=16;
    for(int i=0;i<nRead;i++){ ra[i].co=co; ra[i].Na=Na; ra[i].reads=reads; pthread_create(&rt[i],0,reader,&ra[i]); }
    for(int i=0;i<nRead;i++) pthread_join(rt[i],0);

    sqlite3_exec(A,"COMMIT",0,0,0);
    sqlite3_close(A);
  }
  g_stop=1; pthread_join(wt,0);

  long total = g_latchHeld+g_fallback+g_latchMiss+g_corrupt+g_otherErr;
  printf("\n=== co-read stress: %ds, %d readers, %d reads/cycle ===\n", secs,nRead,reads);
  printf("writer: %ld inserts, %ld checkpoints | anchors: %ld\n", g_writes,g_ckpts,g_anchors);
  printf("co-reads: %ld total\n", total);
  printf("  latch held (saw anchor frame, consistent) : %ld\n", g_latchHeld);
  printf("  graceful fallback (SQLITE_ERROR_SNAPSHOT)  : %ld\n", g_fallback);
  printf("  LATCH MISS (consistent but wrong frame=bug): %ld\n", g_latchMiss);
  printf("  CORRUPTION (torn/inconsistent=HARD FAIL)   : %ld\n", g_corrupt);
  printf("  other errors                               : %ld\n", g_otherErr);
  int ok = (g_corrupt==0 && g_otherErr==0);
  printf("\n%s\n", ok ? "PASS: no corruption, no torn reads" : "FAIL: corruption/torn reads detected");
  return ok?0:1;
}
