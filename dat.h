// Requirements:
// #include <stdint.h>
// #include <stdlib.h>

// 重新定义自己的数据结构
typedef unsigned char uchar;
typedef uchar         byte;
typedef unsigned int  uint;
typedef int32_t       int32;
typedef uint32_t      uint32;
typedef int64_t       int64;
typedef uint64_t      uint64;

// 用define的方法，提示用户不要使用这些数据集
#define int8_t   do_not_use_int8_t
#define uint8_t  do_not_use_uint8_t
#define int32_t  do_not_use_int32_t
#define uint32_t do_not_use_uint32_t
#define int64_t  do_not_use_int64_t
#define uint64_t do_not_use_uint64_t

// 类型定义，定义一些要用的结构
typedef struct ms     *ms;
typedef struct job    *job;
typedef struct tube   *tube;
typedef struct Conn   Conn;
typedef struct Heap   Heap;
typedef struct Jobrec Jobrec;
typedef struct File   File;
typedef struct Socket Socket;
typedef struct Server Server;
typedef struct Wal    Wal; // 这个是bin log的目录结构信息

// 定义一些函数
typedef void(*ms_event_fn)(ms a, void *item, size_t i);
typedef void(*Handle)(void*, int rw);
typedef int(*Less)(void*, void*);
typedef void(*Record)(void*, int);
typedef int(FAlloc)(int, int);

#if _LP64
#define NUM_PRIMES 48
#else
#define NUM_PRIMES 19
#endif

#define MAX_TUBE_NAME_LEN 201

/* A command can be at most LINE_BUF_SIZE chars, including "\r\n". This value
 * MUST be enough to hold the longest possible command ("pause-tube a{200} 4294967295\r\n")
 * or reply line ("USING a{200}\r\n"). */
#define LINE_BUF_SIZE 224 // 约束一行最大的字符数

/* CONN_TYPE_* are bit masks */
#define CONN_TYPE_PRODUCER 1
#define CONN_TYPE_WORKER   2
#define CONN_TYPE_WAITING  4

#define min(a,b) ((a)<(b)?(a):(b))

#define URGENT_THRESHOLD 1024 // ?
#define JOB_DATA_SIZE_LIMIT_DEFAULT ((1 << 16) - 1) // 最大的data数据大小

extern const char version[];
extern int verbose;
extern struct Server srv; // 定义一个结构？

// Replaced by tests to simulate failures.
extern FAlloc *falloc; // ?

// 这个应该是用来记录单个tube的
struct stats {
  uint urgent_ct; // 紧急状态的个数
  uint waiting_ct; // waiting状态的个数
  uint buried_ct; // buried状态的个数
  uint reserved_ct; // 处于reserved状态的个数
  uint pause_ct; // ??
  uint64   total_delete_ct; // 总共的删除次数
  uint64   total_jobs_ct; // 总共接收到的job个数
};


// 堆结构
struct Heap {
  int     cap; // 当前分配空间的容量大小
  int     len; // 已经使用的容量大小
  void    **data; // 这个应该是指向真实堆实现的那个结构的指针的
  Less    less; // Less方法指针
  Record  rec; // Record方法指针
};
int   heapinsert(Heap *h, void *x); // 插入堆
void* heapremove(Heap *h, int k); // 从堆中移除


// socket结构
// 为什么需要单独new一个出来呢
struct Socket {
  int    fd;
  Handle f;
  void   *x;
  int    added;
};
int sockinit(void);
int sockwant(Socket*, int);
int socknext(Socket**, int64);

// 貌似是用来统计全局的tube信息的
// 比如used会记录当前server使用了多少条tube
struct ms {
  size_t used, cap, last; // used记录当前tube条数，cap待定，last待定
  void **items;
  ms_event_fn oninsert, onremove; // 记录tube事件，插入、移除相关tube的事件
};

// ??
enum
  {
    Walver = 7
  };

// 标记这个job的状态
enum // Jobrec.state
  {
    Invalid, // 非法的
    Ready, // 准备好的
    Reserved, // 被客户端请求锁定中的
    Buried, // 被丢弃的
    Delayed, // 延迟状态
    Copy // 过渡状态？
  };

// if you modify this struct, you must increment Walver above
struct Jobrec {
  uint64 id; // 唯一值
  uint32 pri; // 优先级，int32
  int64  delay; // 延迟
  // 客户端拿走这个任务最大能保留的时间，如果超过ttr，服务端会认为客户端没完成任务，重新把job扔回到ready堆，并且拒绝响应客户端关于这个job id的任何操作请求
  // delete请求（如果失败的话，就buried请求）
  int64  ttr; // 客户端最大保留的处理时间，超过这个时间，这个job就会从reserve->ready状态，直接忽视客户端的任何请求
  int32  body_size; // body size
  int64  created_at; // 创建时间
  int64  deadline_at; // 什么时候这个任务会被服务端判断失效
  uint32 reserve_ct; // 被reserve多少次
  uint32 timeout_ct; // 超时的次数
  uint32 release_ct; // release次数
  uint32 bury_ct; // 被摧毁的次数
  uint32 kick_ct; // 被kick回到ready状态的次数
  byte   state; // 一个字节的state信息，具体内部内容是啥?
};

// job结构，beanstalkd的4大结构之一
struct job {

  // persistent fields; these get written to the wal
  // 用来恢复job数据的
  Jobrec r;

  /* bookeeping fields; these are in-memory only */
  char pad[6]; // ?

  tube tube; // 记录当前job属于哪个tube
  job prev, next; /* linked list of jobs */

  job ht_next; /* Next job in a hash table list */
  size_t heap_index; /* where is this job in its current heap */

  File *file; // 这个job会被存储到哪个文件？

  job  fnext; // 找到下一个job，感觉这两个指针都是用来方便查找buried队列的时候用的，待验证
  job  fprev; // 找到上一个job

  void *reserver; // 当前的server指针，记录进程

  int walresv;    // ??
  int walused;    // ??

  char body[]; // written separately to the wal
};

// 工作队列，也可以理解管道，job就是放进这里的
struct tube {

  uint refs; // 当前引用这个tube的个数

  char name[MAX_TUBE_NAME_LEN];

  Heap ready; // 准备好了的job堆
  Heap delay; // 延迟状态的job堆

  // 指针
  struct ms waiting; /* set of conns */

  // 指针
  struct stats stat; // 记录当前这个tube的状态信息

  uint using_ct; // 使用次数？
  uint watching_ct; // 被观察次数？

  int64 pause;  // 停止次数？
  int64 deadline_at; // deadline应该算到每个job上面，为什么这里会有数据呢？

  // 指针
  struct job buried; // 处于buried状态的job，用双向链表来做
};


// handy logging functions
#define twarn(fmt, args...) warn("%s:%d in %s: " fmt,                   \
                                 __FILE__, __LINE__, __func__, ##args)
#define twarnx(fmt, args...) warnx("%s:%d in %s: " fmt,                 \
                                   __FILE__, __LINE__, __func__, ##args)
void warn(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
void warnx(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
char* fmtalloc(char *fmt, ...) __attribute__((format(printf, 1, 2)));

// handy mem alloc funcs
void* zalloc(int n);
#define new(T) zalloc(sizeof(T))

// 解析命令行参数
void optparse(Server*, char**);

extern const char *progname; // 进程名

//
int64 nanoseconds(void);
int   rawfalloc(int fd, int len);

// what is ms?
// ms相关的操作函数
void ms_init(ms a, ms_event_fn oninsert, ms_event_fn onremove);
void ms_clear(ms a);
int ms_append(ms a, void *item);
int ms_remove(ms a, void *item);
int ms_contains(ms a, void *item);
void *ms_take(ms a);


// 创建job的函数
#define make_job(pri,delay,ttr,body_size,tube) make_job_with_id(pri,delay,ttr,body_size,tube,0)

// job相关的操作函数
job allocate_job(int body_size);
job make_job_with_id(uint pri, int64 delay, int64 ttr,
                     int body_size, tube tube, uint64 id);
void job_free(job j);

/* Lookup a job by job ID */
job job_find(uint64 job_id);

/* the void* parameters are really job pointers */
void job_setheappos(void*, int);
int job_pri_less(void*, void*);
int job_delay_less(void*, void*);

// 复制一个job
job job_copy(job j); // 为什么需要复制呢？

// 查看job的状态
const char * job_state(job j);

// any_p?
int job_list_any_p(job head);
job job_remove(job j);
void job_insert(job head, job j);

uint64 total_jobs(void);

/* for unit tests */
size_t get_all_jobs_used(void);


extern struct ms tubes; // 难道ms就是tube列表？

tube make_tube(const char *name);
void tube_dref(tube t);
void tube_iref(tube t);
tube tube_find(const char *name);
tube tube_find_or_make(const char *name);
#define TUBE_ASSIGN(a,b) (tube_dref(a), (a) = (b), tube_iref(a)) // ?

// 创建链接
Conn *make_conn(int fd, char start_state, tube use, tube watch);

// 计算connection相关的数据
int count_cur_conns(void);
uint count_tot_conns(void);
int count_cur_producers(void);
int count_cur_workers(void);


extern size_t primes[];


extern size_t job_data_size_limit;

// 协议初始化、协议运行？
void prot_init(void);
int64 prottick(Server *s);

Conn *remove_waiting_conn(Conn *c);

void enqueue_reserved_jobs(Conn *c);

void enter_drain_mode(int sig); // drain mode是什么意思？
void h_accept(const int fd, const short which, Server* srv);
void prot_remove_tube(tube t); // 移除tube
int  prot_replay(Server *s, job list); // 重放bin log日志，把job放回到合适的tube上面去


// 创建server端的socket
int make_server_socket(char *host_addr, char *port);

// 连接结构
struct Conn {

  Server *srv;
  Socket sock;
  char   state;
  char   type;
  Conn   *next;
  tube   use;
  int64  tickat;      // time at which to do more work
  int    tickpos;     // position in srv->conns
  job    soonest_job; // memoization of the soonest job
  int    rw;          // currently want: 'r', 'w', or 'h'
  int    pending_timeout;
  char   halfclosed;

  char cmd[LINE_BUF_SIZE]; // this string is NOT NUL-terminated
  int  cmd_len;
  int  cmd_read;

  char *reply;
  int  reply_len;
  int  reply_sent;
  char reply_buf[LINE_BUF_SIZE]; // this string IS NUL-terminated

  // How many bytes of in_job->body have been read so far. If in_job is NULL
  // while in_job_read is nonzero, we are in bit bucket mode and
  // in_job_read's meaning is inverted -- then it counts the bytes that
  // remain to be thrown away.
  int in_job_read;
  job in_job; // a job to be read from the client

  job out_job;
  int out_job_sent;

  struct ms  watch;

  struct job reserved_jobs; // linked list header
};
int  connless(Conn *a, Conn *b); // ?
void connrec(Conn *c, int i); // ?
void connwant(Conn *c, int rw); // ?
void connsched(Conn *c); // ?
void connclose(Conn *c); // 关闭链接

void connsetproducer(Conn *c); // 标识当前链接是worker，还是producer
void connsetworker(Conn *c);

job  connsoonestjob(Conn *c); // 获取ttr快到了的job
int  conndeadlinesoon(Conn *c); // 获取快要进入deadline状态的链接
int conn_ready(Conn *c); // 获取conn进入ready状态的链接

#define conn_waiting(c) ((c)->type & CONN_TYPE_WAITING) // 判定函数，conn是不是处于waiting状态


enum
  {
    Filesizedef = (10 << 20)  // 写入的文件最大大小
  };

// 这个貌似是binglog的格式，但是具体还要看下
struct Wal {

  int    filesize; // 文件夹的大小

  int    use; // ？？

  char   *dir; // 具体目录路径
  File   *head; // 第一个bin log位置
  File   *cur; // 当前bin log位置
  File   *tail; // 最后一个bin log位置

  int    nfile;
  int    next;

  int64  resv;  // bytes reserved
  int64  alive; // bytes in use
  int64  nmig;  // migrations
  int64  nrec;  // records written ever

  int    wantsync; // 是否需要同步
  int64  syncrate; // 同步的频率，单位待确定
  int64  lastsync; // 最后一次同步的时间？
  int    nocomp; // disable binlog compaction?
};
int  waldirlock(Wal*); // 锁目录
void walinit(Wal*, job list); // 初始化
int  walwrite(Wal*, job); // 写目录
void walmaint(Wal*); // ?
int  walresvput(Wal*, job); // ?
int  walresvupdate(Wal*, job); // ?
void walgc(Wal*); // 回收？

// ok，这个有可能也是binlog相关的结构
// TODO
struct File {
  File *next; // 单向链表
  uint refs; // 当前引用数
  int  seq; // ?
  int  iswopen; // is open for writing
  int  fd; // 打开的文件句柄
  int  free; // ?
  int  resv; // ?
  char *path; // 文件保存的目录
  Wal  *w; // ?

  struct job jlist; // jobs written in this file
};
// 文件操作相关的handy funcs
int  fileinit(File*, Wal*, int);
Wal* fileadd(File*, Wal*);
void fileincref(File*);
void filedecref(File*);
void fileaddjob(File*, job);
void filermjob(File*, job);
int  fileread(File*, job list);
void filewopen(File*);
void filewclose(File*);
int  filewrjobshort(File*, job);
int  filewrjobfull(File*, job);

// 定义端口
#define Portdef "11300"

// server结构，用来管理服务端信息
// 比如连接管理、wal之类的
struct Server {
  char *port;
  char *addr;
  char *user;

  Wal    wal;   // 这个应该是保存job元数据的数据结构，具体是干嘛的，还要往后面看
  Socket sock;  // 服务器的listen socket
  Heap   conns; // 用堆结构来管理tcp链接，这里记录当前客户端发过来的链接
};
void srvserve(Server *srv);
void srvaccept(Server *s, int ev);
