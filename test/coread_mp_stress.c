/*
** WAL2 co-located-read MULTI-PROCESS stress / corruption gauntlet (scaffold).
**
** Like coread_stress.c but the writer, the anchor, and the readers are
** SEPARATE PROCESSES sharing one wal2 database via its -shm file. This is the
** real test of wal2's cross-process coordination: the anchor process holds a
** read-lock (a system-wide file lock) that must stop the writer process from
** resetting the file the co-located readers are pinned to.
**
** The anchor's WalCoRead capture (its WalIndexHdr + lock class) is published
** to reader processes through a MAP_SHARED region, guarded by a seqlock so a
** reader never copies a half-written capture. Same row invariant as the
** single-process test: count=c => max=c AND sum=c*(c+1)/2.
**
** Build: cc -O1 -DSQLITE_ENABLE_WAL2_COREAD -DSQLITE_THREADSAFE=1 \
**           -I../deps/sqlite3 coread_mp_stress.c -o /tmp/coread_mp -lpthread
** Run:   /tmp/coread_mp [seconds] [nReaderProcs]
*/
#include "sqlite3.c"
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#define DB "/tmp/comp.db"
#define INC(x) __sync_fetch_and_add(&(x),1)

typedef struct {
  volatile int  stop;
  volatile int  seq;     /* seqlock: odd => anchor mid-write; even => stable */
  WalCoRead     co;      /* current anchor's capture (shared across procs) */
  volatile long Na;      /* current anchor's frame row-count */
  volatile long latchHeld, fallback, latchMiss, corrupt, otherErr, coReads;
  volatile long writes, ckpts, anchors;
} Shared;

static Shared *sh;

static Wal *dbWal(sqlite3 *db){ return sqlite3BtreePager(db->aDb[0].pBt)->pWal; }

static int snap(sqlite3 *db, long *pc, long *pmax, long *psum){
  sqlite3_stmt *st; int rc;
  rc = sqlite3_prepare_v2(db,"SELECT count(*),coalesce(max(x),0),coalesce(sum(x),0) FROM t",-1,&st,0);
  if( rc!=SQLITE_OK ) return rc;
  rc = sqlite3_step(st);
  if( rc==SQLITE_ROW ){ *pc=sqlite3_column_int64(st,0); *pmax=sqlite3_column_int64(st,1); *psum=sqlite3_column_int64(st,2); rc=SQLITE_OK; }
  sqlite3_finalize(st);
  return rc;
}

/* WRITER process: insert x=max+1, periodic checkpoint(RESTART) to force wal2
** rotation+reset. RESTART may return BUSY when the anchor holds the file -
** that's expected and fine. */
static void writerProc(void){
  sqlite3 *W=0;
  if( sqlite3_open(DB,&W)!=SQLITE_OK ) _exit(0);
  long n=0;
  while( !sh->stop ){
    if( sqlite3_exec(W,"INSERT INTO t(x) VALUES((SELECT coalesce(max(x),0)+1 FROM t))",0,0,0)==SQLITE_OK ) INC(sh->writes);
    if( (++n%103)==0 ){ sqlite3_exec(W,"PRAGMA wal_checkpoint(RESTART)",0,0,0); INC(sh->ckpts); }
  }
  sqlite3_close(W);
  _exit(0);
}

/* READER process: seqlock-read the current capture, latch it, verify. */
static void readerProc(void){
  sqlite3 *B=0;
  if( sqlite3_open(DB,&B)!=SQLITE_OK ) _exit(0);
  long c,mx,sm;
  snap(B,&c,&mx,&sm);                 /* warmup: open this process's Wal */
  if( dbWal(B)==0 ) _exit(0);
  while( !sh->stop ){
    /* seqlock: copy {co,Na} consistently w.r.t. the anchor's publish. */
    int s1 = sh->seq;
    if( s1&1 ) continue;              /* anchor mid-write */
    WalCoRead co = sh->co; long Na = sh->Na;
    __sync_synchronize();
    if( sh->seq != s1 || s1==0 ) continue;   /* changed under us, or no anchor yet */

    sqlite3WalCoReadOpen(dbWal(B), &co);
    int rc = sqlite3_exec(B,"BEGIN",0,0,0);
    if( rc==SQLITE_OK ) rc = snap(B,&c,&mx,&sm);
    sqlite3WalCoReadOpen(dbWal(B), 0);
    INC(sh->coReads);
    if( rc==SQLITE_OK ){
      long want = Na*(Na+1)/2;
      if( c==Na && mx==Na && sm==want )         INC(sh->latchHeld);
      else if( c==Na )                          INC(sh->corrupt);    /* torn */
      else if( mx==c && sm==c*(c+1)/2 )         INC(sh->latchMiss);  /* consistent, wrong frame */
      else                                      INC(sh->corrupt);    /* inconsistent */
    } else if( (rc&0xFF)==SQLITE_ERROR ){       INC(sh->fallback);
    } else if( (rc&0xFF)==SQLITE_CORRUPT ){     INC(sh->corrupt);
    } else {                                    INC(sh->otherErr); }
    sqlite3_exec(B,"COMMIT",0,0,0);
    sqlite3_exec(B,"ROLLBACK",0,0,0);
  }
  sqlite3_close(B);
  _exit(0);
}

int main(int argc, char **argv){
  setvbuf(stdout,0,_IONBF,0);
  int secs  = argc>1 ? atoi(argv[1]) : 15;
  int nRead = argc>2 ? atoi(argv[2]) : 4;

  remove(DB); remove(DB"-wal"); remove(DB"-wal2"); remove(DB"-shm");
  { sqlite3 *S=0; sqlite3_open(DB,&S);
    sqlite3_exec(S,"PRAGMA journal_mode=wal2;",0,0,0);
    sqlite3_exec(S,"PRAGMA wal_autocheckpoint=0;",0,0,0);
    sqlite3_exec(S,"CREATE TABLE t(x INTEGER);",0,0,0);
    sqlite3_exec(S,"INSERT INTO t(x) VALUES(1);",0,0,0);
    if( !isWalMode2(dbWal(S)) ){ printf("not wal2; abort\n"); return 2; }
    sqlite3_close(S);
  }

  sh = mmap(0, sizeof(Shared), PROT_READ|PROT_WRITE, MAP_SHARED|MAP_ANONYMOUS, -1, 0);
  if( sh==MAP_FAILED ){ perror("mmap"); return 2; }
  memset((void*)sh, 0, sizeof(Shared));

  pid_t kids[64]; int nk=0;
  pid_t w = fork(); if( w==0 ) writerProc(); kids[nk++]=w;
  for(int i=0;i<nRead;i++){ pid_t r=fork(); if(r==0) readerProc(); kids[nk++]=r; }

  /* Parent = anchor process: pin a frame, publish it (seqlock), hold it open a
  ** short window so reader processes can latch, then re-anchor. */
  time_t end = time(0)+secs;
  while( time(0) < end ){
    sqlite3 *A=0;
    if( sqlite3_open(DB,&A)!=SQLITE_OK ) break;
    long Na,Amx,Asm;
    if( sqlite3_exec(A,"BEGIN",0,0,0)!=SQLITE_OK || snap(A,&Na,&Amx,&Asm)!=SQLITE_OK ){ sqlite3_close(A); continue; }
    WalCoRead co;
    if( sqlite3WalCoReadGet(dbWal(A),&co)!=SQLITE_OK ){ sqlite3_exec(A,"COMMIT",0,0,0); sqlite3_close(A); continue; }
    /* publish under seqlock */
    sh->seq++; __sync_synchronize();
    sh->co = co; sh->Na = Na; __sync_synchronize();
    sh->seq++;  INC(sh->anchors);
    usleep(15000);                 /* hold A open ~15ms so readers latch a live anchor */
    sqlite3_exec(A,"COMMIT",0,0,0);
    sqlite3_close(A);
  }
  sh->stop = 1;
  for(int i=0;i<nk;i++){ int st; waitpid(kids[i],&st,0); }

  long total = sh->latchHeld+sh->fallback+sh->latchMiss+sh->corrupt+sh->otherErr;
  printf("\n=== MULTI-PROCESS co-read stress: %ds, %d reader procs ===\n", secs, nRead);
  printf("writer proc: %ld inserts, %ld checkpoints | anchors: %ld\n", sh->writes, sh->ckpts, sh->anchors);
  printf("co-reads: %ld total\n", total);
  printf("  latch held (anchor frame, consistent)      : %ld\n", sh->latchHeld);
  printf("  graceful fallback (SQLITE_ERROR_SNAPSHOT)  : %ld\n", sh->fallback);
  printf("  LATCH MISS (consistent but wrong frame=bug): %ld\n", sh->latchMiss);
  printf("  CORRUPTION (torn/inconsistent=HARD FAIL)   : %ld\n", sh->corrupt);
  printf("  other errors                               : %ld\n", sh->otherErr);
  int ok = (sh->corrupt==0 && sh->otherErr==0 && sh->latchMiss==0);
  printf("\n%s\n", ok ? "PASS: cross-process, no corruption / torn / latch-miss" : "FAIL: see counters above");
  return ok?0:1;
}
