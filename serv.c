#include <stdint.h>
#include <stdlib.h>
#include <sys/socket.h>
#include "dat.h"

struct Server srv = {
  Portdef,
  NULL,
  NULL,
  {
    Filesizedef,
  },
};

// beanstalkd进程的程序入口，基本是while,loop的形式
void
srvserve(Server *s)
{
  int r;
  Socket *sock;
  int64 period;

  // 使用epoll
  if (sockinit() == -1) {
    twarnx("sockinit");
    exit(1);
  }

  s->sock.x = s;
  s->sock.f = (Handle)srvaccept; // 命令处理函数
  s->conns.less = (Less)connless; // ?
  s->conns.rec = (Record)connrec; // ?

  r = listen(s->sock.fd, 1024); // 最大1024个并发链接？
  if (r == -1) {
    twarn("listen");
    return;
  }

  r = sockwant(&s->sock, 'r');
  if (r == -1) {
    twarn("sockwant");
    exit(2);
  }

  for (;;) {

    period = prottick(s);

    int rw = socknext(&sock, period);
    if (rw == -1) {
      twarnx("socknext");
      exit(1);
    }

    if (rw) {
      sock->f(sock->x, rw);
    }
  }
}


void
srvaccept(Server *s, int ev)
{
  h_accept(s->sock.fd, ev, s);
}
