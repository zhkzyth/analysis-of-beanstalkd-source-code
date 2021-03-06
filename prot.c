#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include <sys/resource.h>
#include <sys/uio.h>
#include <sys/types.h>
#include <sys/utsname.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <inttypes.h>
#include <stdarg.h>
#include "dat.h"

/* job body cannot be greater than this many bytes long */
size_t job_data_size_limit = JOB_DATA_SIZE_LIMIT_DEFAULT;

#define NAME_CHARS                              \
  "ABCDEFGHIJKLMNOPQRSTUVWXYZ"                  \
  "abcdefghijklmnopqrstuvwxyz"                  \
  "0123456789-+/;.$_()"

#define CMD_PUT "put "
#define CMD_PEEKJOB "peek "
#define CMD_PEEK_READY "peek-ready"
#define CMD_PEEK_DELAYED "peek-delayed"
#define CMD_PEEK_BURIED "peek-buried"
#define CMD_RESERVE "reserve"
#define CMD_RESERVE_TIMEOUT "reserve-with-timeout "
#define CMD_DELETE "delete "
#define CMD_RELEASE "release "
#define CMD_BURY "bury "
#define CMD_KICK "kick "
#define CMD_JOBKICK "kick-job "
#define CMD_TOUCH "touch "
#define CMD_STATS "stats"
#define CMD_JOBSTATS "stats-job "
#define CMD_USE "use "
#define CMD_WATCH "watch "
#define CMD_IGNORE "ignore "
#define CMD_LIST_TUBES "list-tubes"
#define CMD_LIST_TUBE_USED "list-tube-used"
#define CMD_LIST_TUBES_WATCHED "list-tubes-watched"
#define CMD_STATS_TUBE "stats-tube "
#define CMD_QUIT "quit"
#define CMD_PAUSE_TUBE "pause-tube"

#define CONSTSTRLEN(m) (sizeof(m) - 1)  // 考虑到m本身字符串的null结尾

#define CMD_PEEK_READY_LEN CONSTSTRLEN(CMD_PEEK_READY)
#define CMD_PEEK_DELAYED_LEN CONSTSTRLEN(CMD_PEEK_DELAYED)
#define CMD_PEEK_BURIED_LEN CONSTSTRLEN(CMD_PEEK_BURIED)
#define CMD_PEEKJOB_LEN CONSTSTRLEN(CMD_PEEKJOB)
#define CMD_RESERVE_LEN CONSTSTRLEN(CMD_RESERVE)
#define CMD_RESERVE_TIMEOUT_LEN CONSTSTRLEN(CMD_RESERVE_TIMEOUT)
#define CMD_DELETE_LEN CONSTSTRLEN(CMD_DELETE)
#define CMD_RELEASE_LEN CONSTSTRLEN(CMD_RELEASE)
#define CMD_BURY_LEN CONSTSTRLEN(CMD_BURY)
#define CMD_KICK_LEN CONSTSTRLEN(CMD_KICK)
#define CMD_JOBKICK_LEN CONSTSTRLEN(CMD_JOBKICK)
#define CMD_TOUCH_LEN CONSTSTRLEN(CMD_TOUCH)
#define CMD_STATS_LEN CONSTSTRLEN(CMD_STATS)
#define CMD_JOBSTATS_LEN CONSTSTRLEN(CMD_JOBSTATS)
#define CMD_USE_LEN CONSTSTRLEN(CMD_USE)
#define CMD_WATCH_LEN CONSTSTRLEN(CMD_WATCH)
#define CMD_IGNORE_LEN CONSTSTRLEN(CMD_IGNORE)
#define CMD_LIST_TUBES_LEN CONSTSTRLEN(CMD_LIST_TUBES)
#define CMD_LIST_TUBE_USED_LEN CONSTSTRLEN(CMD_LIST_TUBE_USED)
#define CMD_LIST_TUBES_WATCHED_LEN CONSTSTRLEN(CMD_LIST_TUBES_WATCHED)
#define CMD_STATS_TUBE_LEN CONSTSTRLEN(CMD_STATS_TUBE)
#define CMD_PAUSE_TUBE_LEN CONSTSTRLEN(CMD_PAUSE_TUBE)

#define MSG_FOUND "FOUND"
#define MSG_NOTFOUND "NOT_FOUND\r\n"
#define MSG_RESERVED "RESERVED"
#define MSG_DEADLINE_SOON "DEADLINE_SOON\r\n"
#define MSG_TIMED_OUT "TIMED_OUT\r\n"
#define MSG_DELETED "DELETED\r\n"
#define MSG_RELEASED "RELEASED\r\n"
#define MSG_BURIED "BURIED\r\n"
#define MSG_KICKED "KICKED\r\n"
#define MSG_TOUCHED "TOUCHED\r\n"
#define MSG_BURIED_FMT "BURIED %"PRIu64"\r\n"
#define MSG_INSERTED_FMT "INSERTED %"PRIu64"\r\n"
#define MSG_NOT_IGNORED "NOT_IGNORED\r\n"

#define MSG_NOTFOUND_LEN CONSTSTRLEN(MSG_NOTFOUND)
#define MSG_DELETED_LEN CONSTSTRLEN(MSG_DELETED)
#define MSG_TOUCHED_LEN CONSTSTRLEN(MSG_TOUCHED)
#define MSG_RELEASED_LEN CONSTSTRLEN(MSG_RELEASED)
#define MSG_BURIED_LEN CONSTSTRLEN(MSG_BURIED)
#define MSG_KICKED_LEN CONSTSTRLEN(MSG_KICKED)
#define MSG_NOT_IGNORED_LEN CONSTSTRLEN(MSG_NOT_IGNORED)

#define MSG_OUT_OF_MEMORY "OUT_OF_MEMORY\r\n"
#define MSG_INTERNAL_ERROR "INTERNAL_ERROR\r\n"
#define MSG_DRAINING "DRAINING\r\n"
#define MSG_BAD_FORMAT "BAD_FORMAT\r\n"
#define MSG_UNKNOWN_COMMAND "UNKNOWN_COMMAND\r\n"
#define MSG_EXPECTED_CRLF "EXPECTED_CRLF\r\n"
#define MSG_JOB_TOO_BIG "JOB_TOO_BIG\r\n"

#define STATE_WANTCOMMAND 0
#define STATE_WANTDATA 1
#define STATE_SENDJOB 2
#define STATE_SENDWORD 3
#define STATE_WAIT 4
#define STATE_BITBUCKET 5
#define STATE_CLOSE 6

#define OP_UNKNOWN 0
#define OP_PUT 1
#define OP_PEEKJOB 2
#define OP_RESERVE 3
#define OP_DELETE 4
#define OP_RELEASE 5
#define OP_BURY 6
#define OP_KICK 7
#define OP_STATS 8
#define OP_JOBSTATS 9
#define OP_PEEK_BURIED 10
#define OP_USE 11
#define OP_WATCH 12
#define OP_IGNORE 13
#define OP_LIST_TUBES 14
#define OP_LIST_TUBE_USED 15
#define OP_LIST_TUBES_WATCHED 16
#define OP_STATS_TUBE 17
#define OP_PEEK_READY 18
#define OP_PEEK_DELAYED 19
#define OP_RESERVE_TIMEOUT 20
#define OP_TOUCH 21
#define OP_QUIT 22
#define OP_PAUSE_TUBE 23
#define OP_JOBKICK 24
#define TOTAL_OPS 25

// 这些都是yaml格式的
#define STATS_FMT "---\n"                       \
  "current-jobs-urgent: %u\n"                   \
  "current-jobs-ready: %u\n"                    \
  "current-jobs-reserved: %u\n"                 \
  "current-jobs-delayed: %u\n"                  \
  "current-jobs-buried: %u\n"                   \
  "cmd-put: %" PRIu64 "\n"                      \
  "cmd-peek: %" PRIu64 "\n"                     \
  "cmd-peek-ready: %" PRIu64 "\n"               \
  "cmd-peek-delayed: %" PRIu64 "\n"             \
  "cmd-peek-buried: %" PRIu64 "\n"              \
  "cmd-reserve: %" PRIu64 "\n"                  \
  "cmd-reserve-with-timeout: %" PRIu64 "\n"     \
  "cmd-delete: %" PRIu64 "\n"                   \
  "cmd-release: %" PRIu64 "\n"                  \
  "cmd-use: %" PRIu64 "\n"                      \
  "cmd-watch: %" PRIu64 "\n"                    \
  "cmd-ignore: %" PRIu64 "\n"                   \
  "cmd-bury: %" PRIu64 "\n"                     \
  "cmd-kick: %" PRIu64 "\n"                     \
  "cmd-touch: %" PRIu64 "\n"                    \
  "cmd-stats: %" PRIu64 "\n"                    \
  "cmd-stats-job: %" PRIu64 "\n"                \
  "cmd-stats-tube: %" PRIu64 "\n"               \
  "cmd-list-tubes: %" PRIu64 "\n"               \
  "cmd-list-tube-used: %" PRIu64 "\n"           \
  "cmd-list-tubes-watched: %" PRIu64 "\n"       \
  "cmd-pause-tube: %" PRIu64 "\n"               \
  "job-timeouts: %" PRIu64 "\n"                 \
  "total-jobs: %" PRIu64 "\n"                   \
  "max-job-size: %zu\n"                         \
  "current-tubes: %zu\n"                        \
  "current-connections: %u\n"                   \
  "current-producers: %u\n"                     \
  "current-workers: %u\n"                       \
  "current-waiting: %u\n"                       \
  "total-connections: %u\n"                     \
  "pid: %ld\n"                                  \
  "version: \"%s\"\n"                           \
  "rusage-utime: %d.%06d\n"                     \
  "rusage-stime: %d.%06d\n"                     \
  "uptime: %u\n"                                \
  "binlog-oldest-index: %d\n"                   \
  "binlog-current-index: %d\n"                  \
  "binlog-records-migrated: %" PRId64 "\n"      \
  "binlog-records-written: %" PRId64 "\n"       \
  "binlog-max-size: %d\n"                       \
  "id: %s\n"                                    \
  "hostname: %s\n"                              \
  "\r\n"

#define STATS_TUBE_FMT "---\n"                  \
  "name: %s\n"                                  \
  "current-jobs-urgent: %u\n"                   \
  "current-jobs-ready: %u\n"                    \
  "current-jobs-reserved: %u\n"                 \
  "current-jobs-delayed: %u\n"                  \
  "current-jobs-buried: %u\n"                   \
  "total-jobs: %" PRIu64 "\n"                   \
  "current-using: %u\n"                         \
  "current-watching: %u\n"                      \
  "current-waiting: %u\n"                       \
  "cmd-delete: %" PRIu64 "\n"                   \
  "cmd-pause-tube: %u\n"                        \
  "pause: %" PRIu64 "\n"                        \
  "pause-time-left: %" PRId64 "\n"              \
  "\r\n"

#define STATS_JOB_FMT "---\n"                   \
  "id: %" PRIu64 "\n"                           \
  "tube: %s\n"                                  \
  "state: %s\n"                                 \
  "pri: %u\n"                                   \
  "age: %" PRId64 "\n"                          \
  "delay: %" PRId64 "\n"                        \
  "ttr: %" PRId64 "\n"                          \
  "time-left: %" PRId64 "\n"                    \
  "file: %d\n"                                  \
  "reserves: %u\n"                              \
  "timeouts: %u\n"                              \
  "releases: %u\n"                              \
  "buries: %u\n"                                \
  "kicks: %u\n"                                 \
  "\r\n"

/* this number is pretty arbitrary */
#define BUCKET_BUF_SIZE 1024

static char bucket[BUCKET_BUF_SIZE];

static uint ready_ct = 0; // 记录全局job处于ready状态数，主要是统计用
static struct stats global_stat = {0, 0, 0, 0, 0};

static tube default_tube;

static int drain_mode = 0; // 是否处于维护模式
static int64 started_at;

enum {
  NumIdBytes = 8
};

static char id[NumIdBytes * 2 + 1]; // hex-encoded len of NumIdBytes

static struct utsname node_info;
static uint64 op_ct[TOTAL_OPS], timeout_ct = 0;

static Conn *dirty;

static const char * op_names[] = {
  "<unknown>",
  CMD_PUT,
  CMD_PEEKJOB,
  CMD_RESERVE,
  CMD_DELETE,
  CMD_RELEASE,
  CMD_BURY,
  CMD_KICK,
  CMD_STATS,
  CMD_JOBSTATS,
  CMD_PEEK_BURIED,
  CMD_USE,
  CMD_WATCH,
  CMD_IGNORE,
  CMD_LIST_TUBES,
  CMD_LIST_TUBE_USED,
  CMD_LIST_TUBES_WATCHED,
  CMD_STATS_TUBE,
  CMD_PEEK_READY,
  CMD_PEEK_DELAYED,
  CMD_RESERVE_TIMEOUT,
  CMD_TOUCH,
  CMD_QUIT,
  CMD_PAUSE_TUBE,
  CMD_JOBKICK,
};

static job remove_buried_job(job j);

static int
buried_job_p(tube t)
{
  return job_list_any_p(&t->buried);
}

// 回复内容
static void
reply(Conn *c, char *line, int len, int state)
{
  if (!c) return;

  // 触发c的写事件，等着reactor来调用自己的socket来写出内容
  connwant(c, 'w');

  // ??
  c->next = dirty;
  dirty = c;

  // 准备好数据
  c->reply = line;
  c->reply_len = len;
  c->reply_sent = 0;
  c->state = state;

  if (verbose >= 2) {
    printf(">%d reply %.*s\n", c->sock.fd, len-2, line);
  }

}

//
static void
protrmdirty(Conn *c)
{
  Conn *x, *newdirty = NULL;

  while (dirty) {
    x = dirty;
    dirty = dirty->next;
    x->next = NULL;

    if (x != c) {
      x->next = newdirty;
      newdirty = x;
    }
  }
  dirty = newdirty;
}


#define reply_msg(c,m) reply((c),(m),CONSTSTRLEN(m),STATE_SENDWORD)

#define reply_serr(c,e) (twarnx("server error: %s",(e)),  \
                         reply_msg((c),(e)))

static void
reply_line(Conn*, int, const char*, ...)
  __attribute__((format(printf, 3, 4)));

//
static void
reply_line(Conn *c, int state, const char *fmt, ...)
{
  int r;
  va_list ap;

  // 用模糊参数，做fmt匹配
  va_start(ap, fmt);
  r = vsnprintf(c->reply_buf, LINE_BUF_SIZE, fmt, ap);
  va_end(ap);

  /* Make sure the buffer was big enough. If not, we have a bug. */
  if (r >= LINE_BUF_SIZE) return reply_serr(c, MSG_INTERNAL_ERROR);

  // 回复connection的客户端
  return reply(c, c->reply_buf, r, state);
}

//
static void
reply_job(Conn *c, job j, const char *word)
{
  /* tell this connection which job to send */
  c->out_job = j;
  c->out_job_sent = 0;

  return reply_line(c, STATE_SENDJOB, "%s %"PRIu64" %u\r\n",
                    word, j->r.id, j->r.body_size - 2);
}

// 把conn从每个tube的waiting结构里面去掉
/* - "current-waiting" is the number of open connections that have issued a */
/* reserve command while watching this tube but not yet received a response. */
Conn *
remove_waiting_conn(Conn *c)
{
  tube t;
  size_t i;

  if (!conn_waiting(c)) return NULL;

  c->type &= ~CONN_TYPE_WAITING;

  global_stat.waiting_ct--;

  for (i = 0; i < c->watch.used; i++) {
    t = c->watch.items[i];
    t->stat.waiting_ct--;
    ms_remove(&t->waiting, c);
  }

  return c;
}

// 调整job状态，并标记conection
static void
reserve_job(Conn *c, job j)
{
  // 更新job的deadling信息
  j->r.deadline_at = nanoseconds() + j->r.ttr;

  // 更新统计信息
  global_stat.reserved_ct++; /* stats */
  j->tube->stat.reserved_ct++;
  j->r.reserve_ct++;

  // 调整job的状态
  j->r.state = Reserved;

  // 把job插入到connection的reserved_jobs列表中
  job_insert(&c->reserved_jobs, j);

  j->reserver = c;
  c->pending_timeout = -1;

  if (c->soonest_job && j->r.deadline_at < c->soonest_job->r.deadline_at) {
    c->soonest_job = j;
  }

  return reply_job(c, j, MSG_RESERVED);
}

// 找出下一个合法的job
static job
next_eligible_job(int64 now)
{
  tube t;
  size_t i;
  job j = NULL, candidate;

  // 遍历队列，找出可以用的那个job
  for (i = 0; i < tubes.used; i++) {

    t = tubes.items[i];

    // 如果tube的状态是pause
    if (t->pause) {

      if (t->deadline_at > now) continue; // 如果停止的时间已经过了，就调整为正常，并参与到job的遍历中

      t->pause = 0;
    }

    if (t->waiting.used && t->ready.len) {

      candidate = t->ready.data[0];

      // 找到权重最小的job
      if (!j || job_pri_less(candidate, j)) {

        j = candidate;

      }
    }

  }

  return j;
}

// 操作队列
static void
process_queue()
{
  job j;
  int64 now = nanoseconds();

  // while循环
  while ((j = next_eligible_job(now))) { // 查找合适的job

    // 从ready堆里面移除
    heapremove(&j->tube->ready, j->heap_index);

    // 修改ready_ct
    ready_ct--;

    // 如果任务属于urgent级别的，就调整相应的统计信息
    if (j->r.pri < URGENT_THRESHOLD) {
      global_stat.urgent_ct--;
      j->tube->stat.urgent_ct--;
    }

    // 拿走一个job，并标记reserved状态
    // 把幸运拿到job的conn连接从waiting列表里面移除
    reserve_job(remove_waiting_conn(ms_take(&j->tube->waiting)), j);
  }

}

// 遍历所有的tube列表的delay列表，把第一个要到期的job找出来
// TODO 这里可以再优化下
static job
delay_q_peek()
{
  int i;
  tube t;
  job j = NULL, nj;

  for (i = 0; i < tubes.used; i++) {

    t = tubes.items[i];

    if (t->delay.len == 0) {
      continue;
    }

    nj = t->delay.data[0];

    if (!j || nj->r.deadline_at < j->r.deadline_at) j = nj;
  }

  return j;
}

// 入队操作
static int
enqueue_job(Server *s, job j, int64 delay, char update_store)
{
  int r;

  j->reserver = NULL;

  // 插入到delay堆
  if (delay) {

    // 记录record
    j->r.deadline_at = nanoseconds() + delay;

    // delay结构是一个堆结构来的，所以需要heap来插入
    r = heapinsert(&j->tube->delay, j);

    if (!r) return 0;

    // 调整state
    j->r.state = Delayed;

  } else {

    // 插入到ready堆
    r = heapinsert(&j->tube->ready, j);

    if (!r) return 0;

    j->r.state = Ready;

    ready_ct++;

    /* job优先级：job可以有0~2^32个优先级，0代表最高优先级，小于1024的优先级beanstalkd认为是urgent。 */
    if (j->r.pri < URGENT_THRESHOLD) {
      global_stat.urgent_ct++; // 全局的统计信息
      j->tube->stat.urgent_ct++; // 单个tube的统计信息
    }

  }

  // TODO
  if (update_store) {
    if (!walwrite(&s->wal, j)) {
      return 0;
    }
    walmaint(&s->wal);
  }

  // 处理队列
  process_queue();

  return 1;
}

static int
bury_job(Server *s, job j, char update_store)
{
  int z;

  // 更新store
  if (update_store) {
    z = walresvupdate(&s->wal, j);
    if (!z) return 0;
    j->walresv += z;
  }

  // 把这个job插入到tube的buried列表里面
  job_insert(&j->tube->buried, j);

  // 更新统计参数
  global_stat.buried_ct++;
  j->tube->stat.buried_ct++; // 更新tube的次数
  j->r.state = Buried; // 更新record的状态
  j->reserver = NULL; // 移除这个job的空间，应该是对应一些data类的空间
  j->r.bury_ct++; // 增加这个record的被bury次数，可以用来发现一些问题，比如这个job执行次数失败多次这样，或者做一些优化

  //
  if (update_store) {
    if (!walwrite(&s->wal, j)) {
      return 0;
    }
    walmaint(&s->wal);
  }

  return 1;
}

void
enqueue_reserved_jobs(Conn *c)
{
  int r;
  job j;

  while (job_list_any_p(&c->reserved_jobs)) {
    j = job_remove(c->reserved_jobs.next);
    r = enqueue_job(c->srv, j, 0, 0);
    if (r < 1) bury_job(c->srv, j, 0);
    global_stat.reserved_ct--;
    j->tube->stat.reserved_ct--;
    c->soonest_job = NULL;
  }
}

static job
delay_q_take()
{
  // 拿出一个job
  job j = delay_q_peek();

  if (!j) {
    return 0;
  }

  heapremove(&j->tube->delay, j->heap_index);

  return j;
}

static int
kick_buried_job(Server *s, job j)
{
  int r;
  int z;

  z = walresvupdate(&s->wal, j);
  if (!z) return 0;
  j->walresv += z;

  remove_buried_job(j);

  j->r.kick_ct++;
  r = enqueue_job(s, j, 0, 1);
  if (r == 1) return 1;

  /* ready queue is full, so bury it */
  bury_job(s, j, 0);
  return 0;
}

static uint
get_delayed_job_ct()
{
  tube t;
  size_t i;
  uint count = 0;

  for (i = 0; i < tubes.used; i++) {
    t = tubes.items[i];
    count += t->delay.len;
  }
  return count;
}

static int
kick_delayed_job(Server *s, job j)
{
  int r;
  int z;

  z = walresvupdate(&s->wal, j);
  if (!z) return 0;
  j->walresv += z;

  heapremove(&j->tube->delay, j->heap_index);

  j->r.kick_ct++;
  r = enqueue_job(s, j, 0, 1);
  if (r == 1) return 1;

  /* ready queue is full, so delay it again */
  r = enqueue_job(s, j, j->r.delay, 0);
  if (r == 1) return 0;

  /* last resort */
  bury_job(s, j, 0);
  return 0;
}

/* return the number of jobs successfully kicked */
static uint
kick_buried_jobs(Server *s, tube t, uint n)
{
  uint i;
  for (i = 0; (i < n) && buried_job_p(t); ++i) {
    kick_buried_job(s, t->buried.next);
  }
  return i;
}

/* return the number of jobs successfully kicked */
static uint
kick_delayed_jobs(Server *s, tube t, uint n)
{
  uint i;
  for (i = 0; (i < n) && (t->delay.len > 0); ++i) {
    kick_delayed_job(s, (job)t->delay.data[0]);
  }
  return i;
}

static uint
kick_jobs(Server *s, tube t, uint n)
{
  if (buried_job_p(t)) return kick_buried_jobs(s, t, n);
  return kick_delayed_jobs(s, t, n);
}

static job
remove_buried_job(job j)
{
  if (!j || j->r.state != Buried) return NULL;
  j = job_remove(j);
  if (j) {
    global_stat.buried_ct--;
    j->tube->stat.buried_ct--;
  }
  return j;
}

static job
remove_delayed_job(job j)
{
  if (!j || j->r.state != Delayed) return NULL;
  heapremove(&j->tube->delay, j->heap_index);

  return j;
}

static job
remove_ready_job(job j)
{
  if (!j || j->r.state != Ready) return NULL;
  heapremove(&j->tube->ready, j->heap_index);
  ready_ct--;
  if (j->r.pri < URGENT_THRESHOLD) {
    global_stat.urgent_ct--;
    j->tube->stat.urgent_ct--;
  }
  return j;
}

/*  - "current-waiting" is the number of open connections that have issued a */
/*    reserve command while watching this tube but not yet received a response. */
static void
enqueue_waiting_conn(Conn *c)
{
  tube t;
  size_t i;

  global_stat.waiting_ct++;
  c->type |= CONN_TYPE_WAITING;
  for (i = 0; i < c->watch.used; i++) {
    t = c->watch.items[i];
    t->stat.waiting_ct++;
    ms_append(&t->waiting, c);
  }
}

static job
find_reserved_job_in_conn(Conn *c, job j)
{
  return (j && j->reserver == c && j->r.state == Reserved) ? j : NULL;
}

static job
touch_job(Conn *c, job j)
{
  j = find_reserved_job_in_conn(c, j);
  if (j) {
    j->r.deadline_at = nanoseconds() + j->r.ttr;
    c->soonest_job = NULL;
  }
  return j;
}

static job
peek_job(uint64 id)
{
  return job_find(id);
}

static void
check_err(Conn *c, const char *s)
{
  if (errno == EAGAIN) return;
  if (errno == EINTR) return;
  if (errno == EWOULDBLOCK) return;

  twarn("%s", s);
  c->state = STATE_CLOSE;
  return;
}

/* Scan the given string for the sequence "\r\n" and return the line length.
 * Always returns at least 2 if a match is found. Returns 0 if no match. */
static int
scan_line_end(const char *s, int size)
{
  char *match;

  match = memchr(s, '\r', size - 1);
  if (!match) return 0;

  /* this is safe because we only scan size - 1 chars above */
  if (match[1] == '\n') return match - s + 2;

  return 0;
}

static int
cmd_len(Conn *c)
{
  return scan_line_end(c->cmd, c->cmd_read);
}

/* parse the command line */
static int
which_cmd(Conn *c)
{
#define TEST_CMD(s,c,o) if (strncmp((s), (c), CONSTSTRLEN(c)) == 0) return (o);
  TEST_CMD(c->cmd, CMD_PUT, OP_PUT);
  TEST_CMD(c->cmd, CMD_PEEKJOB, OP_PEEKJOB);
  TEST_CMD(c->cmd, CMD_PEEK_READY, OP_PEEK_READY);
  TEST_CMD(c->cmd, CMD_PEEK_DELAYED, OP_PEEK_DELAYED);
  TEST_CMD(c->cmd, CMD_PEEK_BURIED, OP_PEEK_BURIED);
  TEST_CMD(c->cmd, CMD_RESERVE_TIMEOUT, OP_RESERVE_TIMEOUT);
  TEST_CMD(c->cmd, CMD_RESERVE, OP_RESERVE);
  TEST_CMD(c->cmd, CMD_DELETE, OP_DELETE);
  TEST_CMD(c->cmd, CMD_RELEASE, OP_RELEASE);
  TEST_CMD(c->cmd, CMD_BURY, OP_BURY);
  TEST_CMD(c->cmd, CMD_KICK, OP_KICK);
  TEST_CMD(c->cmd, CMD_JOBKICK, OP_JOBKICK);
  TEST_CMD(c->cmd, CMD_TOUCH, OP_TOUCH);
  TEST_CMD(c->cmd, CMD_JOBSTATS, OP_JOBSTATS);
  TEST_CMD(c->cmd, CMD_STATS_TUBE, OP_STATS_TUBE);
  TEST_CMD(c->cmd, CMD_STATS, OP_STATS);
  TEST_CMD(c->cmd, CMD_USE, OP_USE);
  TEST_CMD(c->cmd, CMD_WATCH, OP_WATCH);
  TEST_CMD(c->cmd, CMD_IGNORE, OP_IGNORE);
  TEST_CMD(c->cmd, CMD_LIST_TUBES_WATCHED, OP_LIST_TUBES_WATCHED);
  TEST_CMD(c->cmd, CMD_LIST_TUBE_USED, OP_LIST_TUBE_USED);
  TEST_CMD(c->cmd, CMD_LIST_TUBES, OP_LIST_TUBES);
  TEST_CMD(c->cmd, CMD_QUIT, OP_QUIT);
  TEST_CMD(c->cmd, CMD_PAUSE_TUBE, OP_PAUSE_TUBE);
  return OP_UNKNOWN;
}

/* Copy up to body_size trailing bytes into the job, then the rest into the cmd
 * buffer. If c->in_job exists, this assumes that c->in_job->body is empty.
 * This function is idempotent(). */
static void
fill_extra_data(Conn *c)
// 这里的假设是，有可能我们收到的数据，包含了额外的command数据，这个时候我们不丢弃，而是放到下一次comamnd的调用里面
{
  int extra_bytes, job_data_bytes = 0, cmd_bytes;

  if (!c->sock.fd) return; /* the connection was closed */
  if (!c->cmd_len) return; /* we don't have a complete command */

  /* how many extra bytes did we read? */
  extra_bytes = c->cmd_read - c->cmd_len;

  /* how many bytes should we put into the job body? */
  if (c->in_job) {
    job_data_bytes = min(extra_bytes, c->in_job->r.body_size);
    memcpy(c->in_job->body, c->cmd + c->cmd_len, job_data_bytes);
    c->in_job_read = job_data_bytes;
  } else if (c->in_job_read) {
    /* we are in bit-bucket mode, throwing away data */
    job_data_bytes = min(extra_bytes, c->in_job_read);
    c->in_job_read -= job_data_bytes;
  }

  /* how many bytes are left to go into the future cmd? */
  cmd_bytes = extra_bytes - job_data_bytes;
  /* void* memmove( void* dest, const void* src, size_t count ); */
  memmove(c->cmd, c->cmd + c->cmd_len + job_data_bytes, cmd_bytes);
  c->cmd_read = cmd_bytes;
  c->cmd_len = 0; /* we no longer know the length of the new command */
}

// put过来的job数据包太大啦，扔掉当前的job数据
// 直接扔掉不行吗？
static void
_skip(Conn *c, int n, char *line, int len)
{
  /* Invert the meaning of in_job_read while throwing away data -- it
   * counts the bytes that remain to be thrown away. */
  c->in_job = 0;
  c->in_job_read = n;

  fill_extra_data(c);

  // 如果数据扔完了，就返回错误信息给客户端
  // 还有很多数据没接收完，继续接收客户端的数据，慢慢扔...
  if (c->in_job_read == 0) return reply(c, line, len, STATE_SENDWORD);

  c->reply = line;
  c->reply_len = len;
  c->reply_sent = 0;

  c->state = STATE_BITBUCKET; // 收集数据中

  return;
}

#define skip(c,n,m) (_skip(c,n,m,CONSTSTRLEN(m)))


static void
enqueue_incoming_job(Conn *c)
{
  int r;
  job j = c->in_job;

  c->in_job = NULL; /* the connection no longer owns this job */
  c->in_job_read = 0;

  /* check if the trailer is present and correct */
  if (memcmp(j->body + j->r.body_size - 2, "\r\n", 2)) {
    job_free(j);
    return reply_msg(c, MSG_EXPECTED_CRLF);
  }

  if (verbose >= 2) {
    printf("<%d job %"PRIu64"\n", c->sock.fd, j->r.id);
  }

  // server不再接受新job
  if (drain_mode) {
    job_free(j);
    return reply_serr(c, MSG_DRAINING);
  }

  // TODO
  if (j->walresv) return reply_serr(c, MSG_INTERNAL_ERROR);
  j->walresv = walresvput(&c->srv->wal, j);
  if (!j->walresv) return reply_serr(c, MSG_OUT_OF_MEMORY);

  /* we have a complete job, so let's stick it in the pqueue */
  r = enqueue_job(c->srv, j, j->r.delay, 1);
  if (r < 0) return reply_serr(c, MSG_INTERNAL_ERROR);

  global_stat.total_jobs_ct++;
  j->tube->stat.total_jobs_ct++;

  // ok，加入job成功
  if (r == 1) return reply_line(c, STATE_SENDWORD, MSG_INSERTED_FMT, j->r.id);

  /* out of memory trying to grow the queue, so it gets buried */
  bury_job(c->srv, j, 0);
  reply_line(c, STATE_SENDWORD, MSG_BURIED_FMT, j->r.id);

}

static uint
uptime()
{
  return (nanoseconds() - started_at) / 1000000000;
}

static int
fmt_stats(char *buf, size_t size, void *x)
{
  int whead = 0, wcur = 0;
  Server *srv;
  struct rusage ru = {{0, 0}, {0, 0}};

  srv = x;

  if (srv->wal.head) {
    whead = srv->wal.head->seq;
  }

  if (srv->wal.cur) {
    wcur = srv->wal.cur->seq;
  }

  getrusage(RUSAGE_SELF, &ru); /* don't care if it fails */
  return snprintf(buf, size, STATS_FMT,
                  global_stat.urgent_ct,
                  ready_ct,
                  global_stat.reserved_ct,
                  get_delayed_job_ct(),
                  global_stat.buried_ct,
                  op_ct[OP_PUT],
                  op_ct[OP_PEEKJOB],
                  op_ct[OP_PEEK_READY],
                  op_ct[OP_PEEK_DELAYED],
                  op_ct[OP_PEEK_BURIED],
                  op_ct[OP_RESERVE],
                  op_ct[OP_RESERVE_TIMEOUT],
                  op_ct[OP_DELETE],
                  op_ct[OP_RELEASE],
                  op_ct[OP_USE],
                  op_ct[OP_WATCH],
                  op_ct[OP_IGNORE],
                  op_ct[OP_BURY],
                  op_ct[OP_KICK],
                  op_ct[OP_TOUCH],
                  op_ct[OP_STATS],
                  op_ct[OP_JOBSTATS],
                  op_ct[OP_STATS_TUBE],
                  op_ct[OP_LIST_TUBES],
                  op_ct[OP_LIST_TUBE_USED],
                  op_ct[OP_LIST_TUBES_WATCHED],
                  op_ct[OP_PAUSE_TUBE],
                  timeout_ct,
                  global_stat.total_jobs_ct,
                  job_data_size_limit,
                  tubes.used,
                  count_cur_conns(),
                  count_cur_producers(),
                  count_cur_workers(),
                  global_stat.waiting_ct,
                  count_tot_conns(),
                  (long) getpid(),
                  version,
                  (int) ru.ru_utime.tv_sec, (int) ru.ru_utime.tv_usec,
                  (int) ru.ru_stime.tv_sec, (int) ru.ru_stime.tv_usec,
                  uptime(),
                  whead,
                  wcur,
                  srv->wal.nmig,
                  srv->wal.nrec,
                  srv->wal.filesize,
                  id,
                  node_info.nodename);

}

/* Read a priority value from the given buffer and place it in pri.
 * Update end to point to the address after the last character consumed.
 * Pri and end can be NULL. If they are both NULL, read_pri() will do the
 * conversion and return the status code but not update any values. This is an
 * easy way to check for errors.
 * If end is NULL, read_pri will also check that the entire input string was
 * consumed and return an error code otherwise.
 * Return 0 on success, or nonzero on failure.
 * If a failure occurs, pri and end are not modified. */
static int
read_pri(uint *pri, const char *buf, char **end)
{
  char *tend;
  uint tpri;

  errno = 0;
  while (buf[0] == ' ') buf++;
  if (buf[0] < '0' || '9' < buf[0]) return -1;
  tpri = strtoul(buf, &tend, 10);
  if (tend == buf) return -1;
  if (errno && errno != ERANGE) return -1;
  if (!end && tend[0] != '\0') return -1;

  if (pri) *pri = tpri;
  if (end) *end = tend;
  return 0;
}

/* Read a delay value from the given buffer and place it in delay.
 * The interface and behavior are analogous to read_pri(). */
static int
read_delay(int64 *delay, const char *buf, char **end)
{
  int r;
  uint delay_sec;

  r = read_pri(&delay_sec, buf, end);
  if (r) return r;
  *delay = ((int64) delay_sec) * 1000000000;
  return 0;
}

/* Read a timeout value from the given buffer and place it in ttr.
 * The interface and behavior are the same as in read_delay(). */
static int
read_ttr(int64 *ttr, const char *buf, char **end)
{
  return read_delay(ttr, buf, end);
}

/* Read a tube name from the given buffer moving the buffer to the name start */
static int
read_tube_name(char **tubename, char *buf, char **end)
{
  size_t len;

  while (buf[0] == ' ') buf++;
  len = strspn(buf, NAME_CHARS);
  if (len == 0) return -1;
  if (tubename) *tubename = buf;
  if (end) *end = buf + len;
  return 0;
}

// 等待server分配队列的job给worker
static void
wait_for_job(Conn *c, int timeout)
{
  c->state = STATE_WAIT;
  enqueue_waiting_conn(c); // 把这个c加入到等待获取job的conn列表里面

  /* Set the pending timeout to the requested timeout amount */
  c->pending_timeout = timeout;

  connwant(c, 'h'); // only care if they hang up

  // 这些都是马上就要检查是否有hang up情况的
  c->next = dirty;
  dirty = c;
}

typedef int(*fmt_fn)(char *, size_t, void *);

static void
do_stats(Conn *c, fmt_fn fmt, void *data)
{
  int r, stats_len;

  /* first, measure how big a buffer we will need */
  stats_len = fmt(NULL, 0, data) + 16;

  c->out_job = allocate_job(stats_len); /* fake job to hold stats data */
  if (!c->out_job) return reply_serr(c, MSG_OUT_OF_MEMORY);

  /* Mark this job as a copy so it can be appropriately freed later on */
  c->out_job->r.state = Copy;

  /* now actually format the stats data */
  r = fmt(c->out_job->body, stats_len, data);
  /* and set the actual body size */
  c->out_job->r.body_size = r;
  if (r > stats_len) return reply_serr(c, MSG_INTERNAL_ERROR);

  c->out_job_sent = 0;
  return reply_line(c, STATE_SENDJOB, "OK %d\r\n", r - 2);
}

// 列出当前在监听的tubes
static void
do_list_tubes(Conn *c, ms l)
{
  char *buf;
  tube t;
  size_t i, resp_z;

  /* first, measure how big a buffer we will need */
  resp_z = 6; /* initial "---\n" and final "\r\n" */

  // 分配需要的空间
  for (i = 0; i < l->used; i++) {
    t = l->items[i];
    resp_z += 3 + strlen(t->name); /* including "- " and "\n" */
  }

  c->out_job = allocate_job(resp_z); /* fake job to hold response data */

  if (!c->out_job) return reply_serr(c, MSG_OUT_OF_MEMORY);

  /* Mark this job as a copy so it can be appropriately freed later on */
  c->out_job->r.state = Copy;

  /* now actually format the response */
  buf = c->out_job->body; // buf是body内存空间的指针

  /* snprintf automatically appends a null character to the character sequence resulting from format substitution */
  /* This automatically appended character is not exempt from the size check. */
  /* This means "%d", 100 will occupy 4 bytes went 'redirected' to the *str buffer */
  buf += snprintf(buf, 5, "---\n"); // 长度需要包括末尾的null
  for (i = 0; i < l->used; i++) {
    t = l->items[i];
    // TIPS: 虽然每次用snprintf都会要求多写入一个null用来结尾，但作者很聪明的通过跟踪buf的位置，不断覆盖掉不需要的null
    buf += snprintf(buf, 4 + strlen(t->name), "- %s\n", t->name);
  }

  // 为什么这里放在第1、2位，难道是逆序？
  // snprintf调用返回后，是返回替换的数字的，这样我们的buf指针就能一直往前走了
  // 好诡异的语法...LOL
  buf[0] = '\r';
  buf[1] = '\n';

  // 标记发送的内容长度为0
  c->out_job_sent = 0;

  return reply_line(c, STATE_SENDJOB, "OK %zu\r\n", resp_z - 2); // 直接忽略掉最后的\r\n两个字符得到的长度
}

static int
fmt_job_stats(char *buf, size_t size, job j)
{
  int64 t;
  int64 time_left;
  int file = 0;

  t = nanoseconds();
  if (j->r.state == Reserved || j->r.state == Delayed) {
    time_left = (j->r.deadline_at - t) / 1000000000;
  } else {
    time_left = 0;
  }
  if (j->file) {
    file = j->file->seq;
  }
  return snprintf(buf, size, STATS_JOB_FMT,
                  j->r.id,
                  j->tube->name,
                  job_state(j),
                  j->r.pri,
                  (t - j->r.created_at) / 1000000000,
                  j->r.delay / 1000000000,
                  j->r.ttr / 1000000000,
                  time_left,
                  file,
                  j->r.reserve_ct,
                  j->r.timeout_ct,
                  j->r.release_ct,
                  j->r.bury_ct,
                  j->r.kick_ct);
}

// 格式化输出tube的统计信息
static int
fmt_stats_tube(char *buf, size_t size, tube t)
{
  uint64 time_left;

  if (t->pause > 0) {
    time_left = (t->deadline_at - nanoseconds()) / 1000000000;
  } else {
    time_left = 0;
  }
  return snprintf(buf, size, STATS_TUBE_FMT,
                  t->name,
                  t->stat.urgent_ct,
                  t->ready.len,
                  t->stat.reserved_ct,
                  t->delay.len,
                  t->stat.buried_ct,
                  t->stat.total_jobs_ct,
                  t->using_ct,
                  t->watching_ct,
                  t->stat.waiting_ct,
                  t->stat.total_delete_ct,
                  t->stat.pause_ct,
                  t->pause / 1000000000,
                  time_left);
}

// 把job入队？
static void
maybe_enqueue_incoming_job(Conn *c)
{
  job j = c->in_job;

  /* do we have a complete job? */
  if (c->in_job_read == j->r.body_size) return enqueue_incoming_job(c);

  /* otherwise we have incomplete data, so just keep waiting */
  c->state = STATE_WANTDATA;
}

/* j can be NULL */
static job
remove_this_reserved_job(Conn *c, job j)
{
  j = job_remove(j);
  if (j) {
    global_stat.reserved_ct--;
    j->tube->stat.reserved_ct--;
    j->reserver = NULL;
  }
  c->soonest_job = NULL; // 很好奇这里的操作，一个conn对应一个job？
  return j;
}

// 从conn移除掉reserve住的job
static job
remove_reserved_job(Conn *c, job j)
{
  return remove_this_reserved_job(c, find_reserved_job_in_conn(c, j));
}

static int
name_is_ok(const char *name, size_t max)
{
  size_t len = strlen(name);
  return len > 0 && len <= max &&
    strspn(name, NAME_CHARS) == len && name[0] != '-';
}

void
prot_remove_tube(tube t)
{
  ms_remove(&tubes, t);
}

// 分发命令
static void
dispatch_cmd(Conn *c)
{
  int r, i, timeout = -1;
  int z;
  uint count;
  job j = 0;
  byte type;
  char *size_buf, *delay_buf, *ttr_buf, *pri_buf, *end_buf, *name;
  uint pri, body_size;
  int64 delay, ttr;
  uint64 id;
  tube t = NULL;

  /* NUL-terminate this string so we can use strtol and friends */
  c->cmd[c->cmd_len - 2] = '\0';

  /* check for possible maliciousness */
  if (strlen(c->cmd) != c->cmd_len - 2) {
    return reply_msg(c, MSG_BAD_FORMAT);
  }

  // 查找命令类型
  type = which_cmd(c);
  if (verbose >= 2) {
    printf("<%d command %s\n", c->sock.fd, op_names[type]);
  }

  switch (type) {

    // put命令
  case OP_PUT:

    // 找出pri值
    r = read_pri(&pri, c->cmd + 4, &delay_buf);
    if (r) return reply_msg(c, MSG_BAD_FORMAT);

    // 找出delay值
    r = read_delay(&delay, delay_buf, &ttr_buf);
    if (r) return reply_msg(c, MSG_BAD_FORMAT);

    // 找出ttr值
    r = read_ttr(&ttr, ttr_buf, &size_buf);
    if (r) return reply_msg(c, MSG_BAD_FORMAT);

    errno = 0;
    body_size = strtoul(size_buf, &end_buf, 10);
    if (errno) return reply_msg(c, MSG_BAD_FORMAT);

    // 记录操作信息
    op_ct[type]++;

    if (body_size > job_data_size_limit) {
      /* throw away the job body and respond with JOB_TOO_BIG */
      return skip(c, body_size + 2, MSG_JOB_TOO_BIG);
    }

    /* don't allow trailing garbage */
    if (end_buf[0] != '\0') return reply_msg(c, MSG_BAD_FORMAT);

    // 把当前链接标记为producer
    connsetproducer(c);

    // 如果ttr小于1s，把它弄回1s
    if (ttr < 1000000000) {
      ttr = 1000000000;
    }

    c->in_job = make_job(pri, delay, ttr, body_size + 2, c->use);

    /* OOM? */
    if (!c->in_job) {
      /* throw away the job body and respond with OUT_OF_MEMORY */
      twarnx("server error: " MSG_OUT_OF_MEMORY);
      return skip(c, body_size + 2, MSG_OUT_OF_MEMORY);
    }

    fill_extra_data(c);

    /* it's possible we already have a complete job */
    maybe_enqueue_incoming_job(c);

    break;

  case OP_PEEK_READY:
    /* don't allow trailing garbage */
    if (c->cmd_len != CMD_PEEK_READY_LEN + 2) {
      return reply_msg(c, MSG_BAD_FORMAT);
    }
    op_ct[type]++;

    if (c->use->ready.len) {
      j = job_copy(c->use->ready.data[0]);
    }

    if (!j) return reply(c, MSG_NOTFOUND, MSG_NOTFOUND_LEN, STATE_SENDWORD);

    reply_job(c, j, MSG_FOUND);
    break;

  case OP_PEEK_DELAYED:
    /* don't allow trailing garbage */
    if (c->cmd_len != CMD_PEEK_DELAYED_LEN + 2) {
      return reply_msg(c, MSG_BAD_FORMAT);
    }
    op_ct[type]++;

    if (c->use->delay.len) {
      j = job_copy(c->use->delay.data[0]);
    }

    if (!j) return reply(c, MSG_NOTFOUND, MSG_NOTFOUND_LEN, STATE_SENDWORD);

    reply_job(c, j, MSG_FOUND);
    break;

  case OP_PEEK_BURIED:
    /* don't allow trailing garbage */
    if (c->cmd_len != CMD_PEEK_BURIED_LEN + 2) {
      return reply_msg(c, MSG_BAD_FORMAT);
    }
    op_ct[type]++;

    j = job_copy(buried_job_p(c->use)? j = c->use->buried.next : NULL);

    if (!j) return reply(c, MSG_NOTFOUND, MSG_NOTFOUND_LEN, STATE_SENDWORD);

    reply_job(c, j, MSG_FOUND);
    break;

  case OP_PEEKJOB:
    errno = 0;
    id = strtoull(c->cmd + CMD_PEEKJOB_LEN, &end_buf, 10);
    if (errno) return reply_msg(c, MSG_BAD_FORMAT);
    op_ct[type]++;

    /* So, peek is annoying, because some other connection might free the
     * job while we are still trying to write it out. So we copy it and
     * then free the copy when it's done sending. */
    j = job_copy(peek_job(id));

    if (!j) return reply(c, MSG_NOTFOUND, MSG_NOTFOUND_LEN, STATE_SENDWORD);

    reply_job(c, j, MSG_FOUND);
    break;

  case OP_RESERVE_TIMEOUT:
    errno = 0;
    timeout = strtol(c->cmd + CMD_RESERVE_TIMEOUT_LEN, &end_buf, 10);
    if (errno) return reply_msg(c, MSG_BAD_FORMAT);

  case OP_RESERVE: /* FALLTHROUGH */
    /* don't allow trailing garbage */
    if (type == OP_RESERVE && c->cmd_len != CMD_RESERVE_LEN + 2) {
      return reply_msg(c, MSG_BAD_FORMAT);
    }

    op_ct[type]++;
    connsetworker(c);

    // 检查这个conn之前已经获取过一个job，然后快要过期了
    // 同时，这个conn关心的那几个tube都木有数据的时候
    // 就跟客户端说，hello，这里木有更多数据啦，而且你自己那个还没搞定
    // 还是先搞定你自己吧
    if (conndeadlinesoon(c) && !conn_ready(c)) {
      return reply_msg(c, MSG_DEADLINE_SOON);
    }

    /* try to get a new job for this guy */
    wait_for_job(c, timeout);
    process_queue();
    break;

  case OP_DELETE:
    errno = 0;
    id = strtoull(c->cmd + CMD_DELETE_LEN, &end_buf, 10);
    if (errno) return reply_msg(c, MSG_BAD_FORMAT);
    op_ct[type]++;

    j = job_find(id);
    j = remove_reserved_job(c, j) ? :
      remove_ready_job(j) ? :
      remove_buried_job(j) ? :
      remove_delayed_job(j);

    if (!j) return reply(c, MSG_NOTFOUND, MSG_NOTFOUND_LEN, STATE_SENDWORD);

    j->tube->stat.total_delete_ct++;

    j->r.state = Invalid;
    r = walwrite(&c->srv->wal, j);
    walmaint(&c->srv->wal);
    job_free(j);

    if (!r) return reply_serr(c, MSG_INTERNAL_ERROR);

    reply(c, MSG_DELETED, MSG_DELETED_LEN, STATE_SENDWORD);
    break;

  case OP_RELEASE:
    errno = 0;
    id = strtoull(c->cmd + CMD_RELEASE_LEN, &pri_buf, 10);
    if (errno) return reply_msg(c, MSG_BAD_FORMAT);

    r = read_pri(&pri, pri_buf, &delay_buf);
    if (r) return reply_msg(c, MSG_BAD_FORMAT);

    r = read_delay(&delay, delay_buf, NULL);
    if (r) return reply_msg(c, MSG_BAD_FORMAT);
    op_ct[type]++;

    j = remove_reserved_job(c, job_find(id));

    if (!j) return reply(c, MSG_NOTFOUND, MSG_NOTFOUND_LEN, STATE_SENDWORD);

    /* We want to update the delay deadline on disk, so reserve space for
     * that. */
    if (delay) {
      z = walresvupdate(&c->srv->wal, j);
      if (!z) return reply_serr(c, MSG_OUT_OF_MEMORY);
      j->walresv += z;
    }

    j->r.pri = pri;
    j->r.delay = delay;
    j->r.release_ct++;

    r = enqueue_job(c->srv, j, delay, !!delay);
    if (r < 0) return reply_serr(c, MSG_INTERNAL_ERROR);
    if (r == 1) {
      return reply(c, MSG_RELEASED, MSG_RELEASED_LEN, STATE_SENDWORD);
    }

    /* out of memory trying to grow the queue, so it gets buried */
    bury_job(c->srv, j, 0);
    reply(c, MSG_BURIED, MSG_BURIED_LEN, STATE_SENDWORD);
    break;

  case OP_BURY:
    errno = 0;
    id = strtoull(c->cmd + CMD_BURY_LEN, &pri_buf, 10);
    if (errno) return reply_msg(c, MSG_BAD_FORMAT);

    r = read_pri(&pri, pri_buf, NULL);
    if (r) return reply_msg(c, MSG_BAD_FORMAT);
    op_ct[type]++;

    j = remove_reserved_job(c, job_find(id));

    if (!j) return reply(c, MSG_NOTFOUND, MSG_NOTFOUND_LEN, STATE_SENDWORD);

    j->r.pri = pri;
    r = bury_job(c->srv, j, 1);
    if (!r) return reply_serr(c, MSG_INTERNAL_ERROR);
    reply(c, MSG_BURIED, MSG_BURIED_LEN, STATE_SENDWORD);
    break;

  case OP_KICK:
    errno = 0;
    count = strtoul(c->cmd + CMD_KICK_LEN, &end_buf, 10);
    if (end_buf == c->cmd + CMD_KICK_LEN) {
      return reply_msg(c, MSG_BAD_FORMAT);
    }
    if (errno) return reply_msg(c, MSG_BAD_FORMAT);

    op_ct[type]++;

    i = kick_jobs(c->srv, c->use, count);

    return reply_line(c, STATE_SENDWORD, "KICKED %u\r\n", i);

  case OP_JOBKICK:
    errno = 0;
    id = strtoull(c->cmd + CMD_JOBKICK_LEN, &end_buf, 10);
    if (errno) return twarn("strtoull"), reply_msg(c, MSG_BAD_FORMAT);

    op_ct[type]++;

    j = job_find(id);
    if (!j) return reply(c, MSG_NOTFOUND, MSG_NOTFOUND_LEN, STATE_SENDWORD);

    if ((j->r.state == Buried && kick_buried_job(c->srv, j)) ||
        (j->r.state == Delayed && kick_delayed_job(c->srv, j))) {
      reply(c, MSG_KICKED, MSG_KICKED_LEN, STATE_SENDWORD);
    } else {
      return reply(c, MSG_NOTFOUND, MSG_NOTFOUND_LEN, STATE_SENDWORD);
    }
    break;

  case OP_TOUCH:
    errno = 0;
    id = strtoull(c->cmd + CMD_TOUCH_LEN, &end_buf, 10);
    if (errno) return twarn("strtoull"), reply_msg(c, MSG_BAD_FORMAT);

    op_ct[type]++;

    j = touch_job(c, job_find(id));

    if (j) {
      reply(c, MSG_TOUCHED, MSG_TOUCHED_LEN, STATE_SENDWORD);
    } else {
      return reply(c, MSG_NOTFOUND, MSG_NOTFOUND_LEN, STATE_SENDWORD);
    }
    break;

  case OP_STATS:
    /* don't allow trailing garbage */
    if (c->cmd_len != CMD_STATS_LEN + 2) {
      return reply_msg(c, MSG_BAD_FORMAT);
    }

    op_ct[type]++;

    do_stats(c, fmt_stats, c->srv);
    break;

  case OP_JOBSTATS:
    errno = 0;
    id = strtoull(c->cmd + CMD_JOBSTATS_LEN, &end_buf, 10);
    if (errno) return reply_msg(c, MSG_BAD_FORMAT);

    op_ct[type]++;

    j = peek_job(id);
    if (!j) return reply(c, MSG_NOTFOUND, MSG_NOTFOUND_LEN, STATE_SENDWORD);

    if (!j->tube) return reply_serr(c, MSG_INTERNAL_ERROR);
    do_stats(c, (fmt_fn) fmt_job_stats, j);
    break;

  case OP_STATS_TUBE:
    name = c->cmd + CMD_STATS_TUBE_LEN;
    if (!name_is_ok(name, 200)) return reply_msg(c, MSG_BAD_FORMAT);

    op_ct[type]++;

    t = tube_find(name);
    if (!t) return reply_msg(c, MSG_NOTFOUND);

    do_stats(c, (fmt_fn) fmt_stats_tube, t);
    t = NULL;
    break;

  case OP_LIST_TUBES:
    /* don't allow trailing garbage */
    if (c->cmd_len != CMD_LIST_TUBES_LEN + 2) {
      return reply_msg(c, MSG_BAD_FORMAT);
    }

    op_ct[type]++;
    do_list_tubes(c, &tubes);
    break;

    // 列出tube列表
  case OP_LIST_TUBE_USED:
    /* don't allow trailing garbage */
    if (c->cmd_len != CMD_LIST_TUBE_USED_LEN + 2) {
      return reply_msg(c, MSG_BAD_FORMAT);
    }

    op_ct[type]++;
    reply_line(c, STATE_SENDWORD, "USING %s\r\n", c->use->name);
    break;

  case OP_LIST_TUBES_WATCHED:
    /* don't allow trailing garbage */
    if (c->cmd_len != CMD_LIST_TUBES_WATCHED_LEN + 2) {
      return reply_msg(c, MSG_BAD_FORMAT);
    }

    op_ct[type]++;
    do_list_tubes(c, &c->watch);
    break;

  case OP_USE:
    name = c->cmd + CMD_USE_LEN;
    if (!name_is_ok(name, 200)) return reply_msg(c, MSG_BAD_FORMAT);
    op_ct[type]++;

    TUBE_ASSIGN(t, tube_find_or_make(name));
    if (!t) return reply_serr(c, MSG_OUT_OF_MEMORY);

    c->use->using_ct--;
    TUBE_ASSIGN(c->use, t);
    TUBE_ASSIGN(t, NULL);
    c->use->using_ct++;

    reply_line(c, STATE_SENDWORD, "USING %s\r\n", c->use->name);
    break;

  case OP_WATCH:
    name = c->cmd + CMD_WATCH_LEN;
    if (!name_is_ok(name, 200)) return reply_msg(c, MSG_BAD_FORMAT);
    op_ct[type]++;

    TUBE_ASSIGN(t, tube_find_or_make(name));
    if (!t) return reply_serr(c, MSG_OUT_OF_MEMORY);

    r = 1;
    if (!ms_contains(&c->watch, t)) r = ms_append(&c->watch, t);
    TUBE_ASSIGN(t, NULL);
    if (!r) return reply_serr(c, MSG_OUT_OF_MEMORY);

    reply_line(c, STATE_SENDWORD, "WATCHING %zu\r\n", c->watch.used);
    break;

  case OP_IGNORE:
    name = c->cmd + CMD_IGNORE_LEN;
    if (!name_is_ok(name, 200)) return reply_msg(c, MSG_BAD_FORMAT);
    op_ct[type]++;

    t = NULL;
    for (i = 0; i < c->watch.used; i++) {
      t = c->watch.items[i];
      if (strncmp(t->name, name, MAX_TUBE_NAME_LEN) == 0) break;
      t = NULL;
    }

    if (t && c->watch.used < 2) return reply_msg(c, MSG_NOT_IGNORED);

    if (t) ms_remove(&c->watch, t); /* may free t if refcount => 0 */
    t = NULL;

    reply_line(c, STATE_SENDWORD, "WATCHING %zu\r\n", c->watch.used);
    break;

  case OP_QUIT:
    c->state = STATE_CLOSE;
    break;

    // 暂停tube
    /* The pause-tube command can delay any new job being reserved for a given time. Its form is: */
    /* pause-tube <tube-name> <delay>\r\n */
    /* - <tube> is the tube to pause */
    /* - <delay> is an integer number of seconds < 2**32 to wait before reserving any more */
    /* jobs from the queue */
  case OP_PAUSE_TUBE:
    op_ct[type]++;

    r = read_tube_name(&name, c->cmd + CMD_PAUSE_TUBE_LEN, &delay_buf);
    if (r) return reply_msg(c, MSG_BAD_FORMAT);

    r = read_delay(&delay, delay_buf, NULL);
    if (r) return reply_msg(c, MSG_BAD_FORMAT);

    *delay_buf = '\0';
    if (!name_is_ok(name, 200)) return reply_msg(c, MSG_BAD_FORMAT);
    t = tube_find(name);
    if (!t) return reply_msg(c, MSG_NOTFOUND);

    // Always pause for a positive amount of time, to make sure
    // that waiting clients wake up when the deadline arrives.
    if (delay == 0) {
      delay = 1;
    }

    t->deadline_at = nanoseconds() + delay;
    t->pause = delay;
    t->stat.pause_ct++;

    reply_line(c, STATE_SENDWORD, "PAUSED\r\n");
    break;

    // 未知命令，直接返回了
  default:
    return reply_msg(c, MSG_UNKNOWN_COMMAND);
  }
}

/* There are three reasons this function may be called. We need to check for
 * all of them.
 *
 *  1. A reserved job has run out of time.
 *  2. A waiting client's reserved job has entered the safety margin.
 *  3. A waiting client's requested timeout has occurred.
 *
 * If any of these happen, we must do the appropriate thing. */
static void
conn_timeout(Conn *c)
{
  int r, should_timeout = 0;
  job j;

  /* Check if the client was trying to reserve a job. */
  if (conn_waiting(c) && conndeadlinesoon(c)) should_timeout = 1;

  /* Check if any reserved jobs have run out of time. We should do this
   * whether or not the client is waiting for a new reservation. */
  while ((j = connsoonestjob(c))) {

    if (j->r.deadline_at >= nanoseconds()) break;

    /* This job is in the middle of being written out. If we return it to
     * the ready queue, someone might free it before we finish writing it
     * out to the socket. So we'll copy it here and free the copy when it's
     * done sending. */
    if (j == c->out_job) {
      c->out_job = job_copy(c->out_job);
    }

    timeout_ct++; /* stats */
    j->r.timeout_ct++;

    r = enqueue_job(c->srv, remove_this_reserved_job(c, j), 0, 0);

    if (r < 1) bury_job(c->srv, j, 0); /* out of memory, so bury it */

    connsched(c);
  }

  if (should_timeout) {

    return reply_msg(remove_waiting_conn(c), MSG_DEADLINE_SOON);

  } else if (conn_waiting(c) && c->pending_timeout >= 0) {

    c->pending_timeout = -1;

    return reply_msg(remove_waiting_conn(c), MSG_TIMED_OUT);

  }
}

// 进入drain mode模式
void
enter_drain_mode(int sig)
{
  drain_mode = 1;
}

// 执行命令
static void
do_cmd(Conn *c)
{
  // 分发conn
  dispatch_cmd(c);

  // 填充额外的数据
  fill_extra_data(c);
}

// 重置conn
static void
reset_conn(Conn *c)
{
  connwant(c, 'r');
  c->next = dirty;
  dirty = c;

  /* was this a peek or stats command? */
  if (c->out_job && c->out_job->r.state == Copy) job_free(c->out_job);
  c->out_job = NULL;

  c->reply_sent = 0; /* now that we're done, reset this */

  c->state = STATE_WANTCOMMAND;
}

// 获取这个conn的数据
static void
conn_data(Conn *c)
{
  int r, to_read;
  job j;
  struct iovec iov[2];

  switch (c->state) {

    // 所有conn一上来都是这个状态
  case STATE_WANTCOMMAND:

    r = read(c->sock.fd, c->cmd + c->cmd_read, LINE_BUF_SIZE - c->cmd_read);
    if (r == -1) return check_err(c, "read()");
    if (r == 0) {
      c->state = STATE_CLOSE;
      return;
    }

    c->cmd_read += r; /* we got some bytes */

    // 获取命令行的长度
    c->cmd_len = cmd_len(c); /* find the EOL */

    /* yay, complete command line */
    if (c->cmd_len) return do_cmd(c);

    /* c->cmd_read > LINE_BUF_SIZE can't happen */

    /* command line too long? */
    if (c->cmd_read == LINE_BUF_SIZE) {
      c->cmd_read = 0; /* discard the input so far */
      return reply_msg(c, MSG_BAD_FORMAT);
    }

    // 没有读完一整条命令，等socket下次好了，我们还会进来这里的
    /* otherwise we have an incomplete line, so just keep waiting */
    break;

    // 收到的数据不完整，但是对方还是继续送过来
    // 比如我发了一个超级大的job过来，已经发了1/3，然后已经超过了beanstalkd最大的限制了
    // 这个时候，我继续接受producer的请求，直到把数据全部扔完
  case STATE_BITBUCKET:

    /* Invert the meaning of in_job_read while throwing away data -- it
     * counts the bytes that remain to be thrown away. */
    to_read = min(c->in_job_read, BUCKET_BUF_SIZE);

    r = read(c->sock.fd, bucket, to_read);
    if (r == -1) return check_err(c, "read()");
    if (r == 0) {
      c->state = STATE_CLOSE;
      return;
    }

    c->in_job_read -= r; /* we got some bytes */

    /* (c->in_job_read < 0) can't happen */

    if (c->in_job_read == 0) {
      return reply(c, c->reply, c->reply_len, STATE_SENDWORD);
    }
    break;

  case STATE_WANTDATA:
    j = c->in_job;

    r = read(c->sock.fd, j->body + c->in_job_read, j->r.body_size -c->in_job_read);
    if (r == -1) return check_err(c, "read()");
    if (r == 0) {
      c->state = STATE_CLOSE;
      return;
    }

    c->in_job_read += r; /* we got some bytes */

    /* (j->in_job_read > j->r.body_size) can't happen */

    maybe_enqueue_incoming_job(c);
    break;

  case STATE_SENDWORD:
    r= write(c->sock.fd, c->reply + c->reply_sent, c->reply_len - c->reply_sent);
    if (r == -1) return check_err(c, "write()");
    if (r == 0) {
      c->state = STATE_CLOSE;
      return;
    }

    c->reply_sent += r; /* we got some bytes */

    /* (c->reply_sent > c->reply_len) can't happen */

    if (c->reply_sent == c->reply_len) return reset_conn(c);

    /* otherwise we sent an incomplete reply, so just keep waiting */
    break;

  case STATE_SENDJOB:
    j = c->out_job;

    iov[0].iov_base = (void *)(c->reply + c->reply_sent);
    iov[0].iov_len = c->reply_len - c->reply_sent; /* maybe 0 */
    iov[1].iov_base = j->body + c->out_job_sent;
    iov[1].iov_len = j->r.body_size - c->out_job_sent;

    r = writev(c->sock.fd, iov, 2);
    if (r == -1) return check_err(c, "writev()");
    if (r == 0) {
      c->state = STATE_CLOSE;
      return;
    }

    /* update the sent values */
    c->reply_sent += r;
    if (c->reply_sent >= c->reply_len) {
      c->out_job_sent += c->reply_sent - c->reply_len;
      c->reply_sent = c->reply_len;
    }

    /* (c->out_job_sent > j->r.body_size) can't happen */

    /* are we done? */
    if (c->out_job_sent == j->r.body_size) {
      if (verbose >= 2) {
        printf(">%d job %"PRIu64"\n", c->sock.fd, j->r.id);
      }
      return reset_conn(c);
    }

    /* otherwise we sent incomplete data, so just keep waiting */
    break;

  case STATE_WAIT:
    if (c->halfclosed) {
      c->pending_timeout = -1;
      return reply_msg(remove_waiting_conn(c), MSG_TIMED_OUT);
    }
    break;

  }
}

#define want_command(c) ((c)->sock.fd && ((c)->state == STATE_WANTCOMMAND))
#define cmd_data_ready(c) (want_command(c) && (c)->cmd_read)

// 更新conn情况
// dirty的意思是当前连接有读写的需求，把读写请求放到reactor里面，请求更新
static void
update_conns()
{
  int r;
  Conn *c;

  while (dirty) {
    c = dirty;
    dirty = dirty->next;
    c->next = NULL;
    r = sockwant(&c->sock, c->rw);
    if (r == -1) {
      twarn("sockwant");
      connclose(c);
    }
  }

}

// 完成连接
static void
h_conn(const int fd, const short which, Conn *c)
{
  // 一定是哪里搞错了=。=
  if (fd != c->sock.fd) {
    twarnx("Argh! event fd doesn't match conn fd.");
    close(fd);
    connclose(c);
    update_conns();
    return;
  }

  // half closed是什么意思？
  if (which == 'h') {
    c->halfclosed = 1;
  }

  // 把客户端请求的数据拉过来
  conn_data(c);

  while (cmd_data_ready(c) && (c->cmd_len = cmd_len(c))) do_cmd(c);

  if (c->state == STATE_CLOSE) {
    protrmdirty(c); // 如果关闭了，就从dirty里面去掉这个socket
    connclose(c); // 关闭连接
  }

  update_conns();
}

// 接受协议的请求
static void
prothandle(Conn *c, int ev)
{
  h_conn(c->sock.fd, ev, c);
}

// 获取最小的period时间?
int64
prottick(Server *s)
{
  int r;
  job j;
  int64 now;
  int i;
  tube t;
  int64 period = 0x34630B8A000LL; /* 1 hour in nanoseconds */
  int64 d;

  now = nanoseconds();

  // 把所有的快到期的job拿出来，入ready队去跑
  while ((j = delay_q_peek())) { // 找出所有快要过期的job里面最接近的

    d = j->r.deadline_at - now; // 算出下个轮询的时间点

    if (d > 0) {
      // 还没好呢，下次再来吧
      period = min(period, d);
      break;
    }

    // 从delay堆拿走一个job
    j = delay_q_take();

    // 入ready队
    r = enqueue_job(s, j, 0, 0);

    // 入队失败，放到buried队列
    if (r < 1) bury_job(s, j, 0); /* out of memory, so bury it */
  }

  // 遍历tube
  for (i = 0; i < tubes.used; i++) {

    t = tubes.items[i];

    d = t->deadline_at - now;

    // 重新开始这个tube
    if (t->pause && d <= 0) {
      t->pause = 0;
      process_queue(); // ??
    }
    // 找出最小的轮询时间
    else if (d > 0) {
      period = min(period, d);
    }

  }

  //
  while (s->conns.len) {

    Conn *c = s->conns.data[0];

    // connection上面为啥会有tickat?
    d = c->tickat - now;

    if (d > 0) {
      period = min(period, d);
      break;
    }

    // 移除conns堆最顶层的那个conn
    heapremove(&s->conns, 0);

    // 放弃掉这个conn，通过超时的机制？
    conn_timeout(c);
  }

  update_conns();

  return period;
}

// 接受客户端的连接请求
void
h_accept(const int fd, const short which, Server *s)
{
  Conn *c;
  int cfd, flags, r;
  socklen_t addrlen;
  struct sockaddr_in6 addr;

  addrlen = sizeof addr;

  cfd = accept(fd, (struct sockaddr *)&addr, &addrlen);

  if (cfd == -1) {
    if (errno != EAGAIN && errno != EWOULDBLOCK) twarn("accept()");
    update_conns();
    return;
  }

  if (verbose) {
    printf("accept %d\n", cfd);
  }

  flags = fcntl(cfd, F_GETFL, 0);
  if (flags < 0) {
    twarn("getting flags");
    close(cfd);
    if (verbose) {
      printf("close %d\n", cfd);
    }
    update_conns();
    return;
  }

  // 设置socket的non blocking属性
  r = fcntl(cfd, F_SETFL, flags | O_NONBLOCK);
  if (r < 0) {
    twarn("setting O_NONBLOCK");
    close(cfd);
    if (verbose) {
      printf("close %d\n", cfd);
    }
    update_conns();
    return;
  }

  // 创建连接
  // 默认的state分配为STATE_WANTCOMMAND
  c = make_conn(cfd, STATE_WANTCOMMAND, default_tube, default_tube); // 默认让conn关注default_tube
  if (!c) {
    twarnx("make_conn() failed");
    close(cfd);
    if (verbose) {
      printf("close %d\n", cfd);
    }
    update_conns();
    return;
  }

  // 设置c的配置信息
  c->srv = s;
  c->sock.x = c;
  c->sock.f = (Handle)prothandle; // 这个f最后是怎么被调用的?
  c->sock.fd = cfd;

  // 监听这个socket的读请求
  r = sockwant(&c->sock, 'r');
  if (r == -1) {
    twarn("sockwant");
    close(cfd);
    if (verbose) {
      printf("close %d\n", cfd);
    }
    update_conns();
    return;
  }

  update_conns();
}

// 初始化协议
void
prot_init()
{
  // 初始化所有统计信息
  started_at = nanoseconds();
  memset(op_ct, 0, sizeof(op_ct));

  int dev_random = open("/dev/urandom", O_RDONLY);
  if (dev_random < 0) {
    twarn("open /dev/urandom");
    exit(50);
  }

  int i, r;
  byte rand_data[NumIdBytes];
  r = read(dev_random, &rand_data, NumIdBytes);
  if (r != NumIdBytes) {
    twarn("read /dev/urandom");
    exit(50);
  }
  for (i = 0; i < NumIdBytes; i++) {
    sprintf(id + (i * 2), "%02x", rand_data[i]);
  }
  close(dev_random);

  if (uname(&node_info) == -1) {
    warn("uname");
    exit(50);
  }

  ms_init(&tubes, NULL, NULL);

  TUBE_ASSIGN(default_tube, tube_find_or_make("default"));
  if (!default_tube) twarnx("Out of memory during startup!");
}

// For each job in list, inserts the job into the appropriate data
// structures and adds it to the log.
//
// Returns 1 on success, 0 on failure.
int
prot_replay(Server *s, job list)
{

  job j, nj;
  int64 t, delay;
  int r, z;

  for (j = list->next ; j != list ; j = nj) {

    nj = j->next;

    // 把当前这个job从双向链表中移除
    job_remove(j);

    // 回收空间?
    z = walresvupdate(&s->wal, j);
    if (!z) {
      twarnx("failed to reserve space");
      return 0;
    }

    delay = 0;

    // server结构里面，有对应的几个job堆，和buried状态的队列
    // 根据job的状态不同，扔到不同的结构里面
    switch (j->r.state) {

      // 处理buried状态的job
    case Buried:
      bury_job(s, j, 0);
      break;

      // 延迟处理的
      // 从bin log恢复后，有可能还没到这个job的dead line时间，还不能马上把它扔到ready状态的结构里面
    case Delayed:
      t = nanoseconds();

      if (t < j->r.deadline_at) {
        delay = j->r.deadline_at - t;
      }

      /* fall through */
    default:
      r = enqueue_job(s, j, delay, 0);
      if (r < 1) twarnx("error recovering job %"PRIu64, j->r.id); // what is PRIu64??

    }
  }
  return 1;
}
