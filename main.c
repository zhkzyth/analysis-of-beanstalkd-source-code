#include <stdint.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/stat.h>
#include <unistd.h>
#include <pwd.h>
#include <fcntl.h>
#include "dat.h"

// 用户切换
static void
su(const char *user)
{
  int r;
  struct passwd *pwent;

  errno = 0;
  pwent = getpwnam(user);
  if (errno) twarn("getpwnam(\"%s\")", user), exit(32);
  if (!pwent) twarnx("getpwnam(\"%s\"): no such user", user), exit(33);

  r = setgid(pwent->pw_gid);
  if (r == -1) twarn("setgid(%d \"%s\")", pwent->pw_gid, user), exit(34);

  r = setuid(pwent->pw_uid);
  if (r == -1) twarn("setuid(%d \"%s\")", pwent->pw_uid, user), exit(34);
}


// 设置信号处理函数
static void
set_sig_handlers()
{
  int r;
  struct sigaction sa;

  sa.sa_handler = SIG_IGN;
  sa.sa_flags = 0;
  r = sigemptyset(&sa.sa_mask);
  if (r == -1) twarn("sigemptyset()"), exit(111);

  /* 当服务器close一个连接时，若client端接着发数据。根据TCP协议的规定，会收到一个RST响应，client再往这个服务器发送数据时，系统会发出一个SIGPIPE信号给进程，告诉进程这个连接已经断开了，不要再写了。 */
  /* 根据信号的默认处理规则SIGPIPE信号的默认执行动作是terminate(终止、退出),所以client会退出。若不想客户端退出可以把SIGPIPE设为SIG_IGN */
  /* 参考连接： http://www.cppblog.com/elva/archive/2008/09/10/61544.html */
  r = sigaction(SIGPIPE, &sa, 0); // 忽略掉SIGPIPE的信号
  if (r == -1) twarn("sigaction(SIGPIPE)"), exit(111);

  // 进入drain mode的server不再接受新的连接请求
  sa.sa_handler = enter_drain_mode;
  r = sigaction(SIGUSR1, &sa, 0);
  if (r == -1) twarn("sigaction(SIGUSR1)"), exit(111);
}


// 我们进程的入口在这里
int
main(int argc, char **argv)
{
  int r;
  struct job list = {}; // 构造一个空的job，表头？

  progname = argv[0]; // 进程名
  setlinebuf(stdout); // 把标准输出的输出缓冲区改成线性的，有什么好处吗？
  optparse(&srv, argv+1); // 构造srv结构，话说这个srv结构从哪里来的？

  if (verbose) {
    printf("pid %d\n", getpid());
  }

  // 构造服务端的socket，并监听响应的地址：端口
  r = make_server_socket(srv.addr, srv.port);
  if (r == -1) twarnx("make_server_socket()"), exit(111);
  srv.sock.fd = r;

  // 初始化协议栈
  prot_init();

  // 切换用户（这里应该需要root用户来启，然后切换到对应的用户）
  if (srv.user) su(srv.user);

  // 设置信号处理函数
  set_sig_handlers();

  // 是否启用bin log日志
  // 这里的假设是，bin log只有一个文件夹在用，一个beanstalkd进程自己占用一个
  // 如果后续做ha方案的话，这里要考虑加入对应的bin log参数，做数据的分区处理
  if (srv.wal.use) {

    // We want to make sure that only one beanstalkd tries
    // to use the wal directory at a time. So acquire a lock
    // now and never release it.
    if (!waldirlock(&srv.wal)) { // 锁住bin log的文件夹
      twarnx("failed to lock wal dir %s", srv.wal.dir);
      exit(10);
    }

    list.prev = list.next = &list;

    // 把bin log文件夹的日志重写回job list里
    // 为后续的数据还原做准备
    walinit(&srv.wal, &list);

    // 重放日志文件，把数据塞到内存里面
    r = prot_replay(&srv, &list);
    if (!r) {
      twarnx("failed to replay log");
      return 1;
    }

  }

  // ok，基本准备完毕，可以开始我们的server了
  srvserve(&srv);

  return 0;
}
