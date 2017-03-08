/*
 * linux版的socket操作，主要是socket io相关的
 */

#define _XOPEN_SOURCE 600
#include <stdint.h>
#include <fcntl.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/epoll.h>
#include "dat.h"

#ifndef EPOLLRDHUP
#define EPOLLRDHUP 0x2000
#endif

static int epfd;


/* Allocate disk space.
 * Expects fd's offset to be 0; may also reset fd's offset to 0.
 * Returns 0 on success, and a positive errno otherwise. */
int
rawfalloc(int fd, int len)
{
  return posix_fallocate(fd, 0, len);
}


// 创建基于epoll的事件循环
int
sockinit(void)
{
  epfd = epoll_create(1);

  if (epfd == -1) {
    twarn("epoll_create");
    return -1;
  }

  return 0;
}


// 调整socket在epoll里面的事件，读、写、删除等
int
sockwant(Socket *s, int rw)
{
  int op;

  // 重新生成一个event
  struct epoll_event ev = {};

  // 如果socket加入过，而且不是做读写操作，则返回
  if (!s->added && !rw) {

    return 0;

    // 如果没加入过，而且是需要监听读写事件，则op改成加入操作
  } else if (!s->added && rw) {

    s->added = 1;
    op = EPOLL_CTL_ADD;

    // 如果加入了，但需要删掉，那就删掉这个env
  } else if (!rw) {

    op = EPOLL_CTL_DEL;

  } else {

    // 都不是，那就做一些其他调整

    op = EPOLL_CTL_MOD;

  }

  switch (rw) {
  case 'r':
    ev.events = EPOLLIN;
    break;
  case 'w':
    ev.events = EPOLLOUT;
    break;
  }

  /* 关于EPOLLRDHUP */
  /* 在使用 epoll 时，对端正常断开连接（调用 close()），在服务器端会触发一个 epoll 事件。在低于 2.6.17 版本的内核中，这个 epoll 事件一般是 EPOLLIN，即 0x1，代表连接可读。 */
  /* 连接池检测到某个连接发生 EPOLLIN 事件且没有错误后，会认为有请求到来，将连接交给上层进行处理。这样一来，上层尝试在对端已经 close() 的连接上读取请求，只能读到 EOF，会认为发生异常，报告一个错误。 */
  /* 因此在使用 2.6.17 之前版本内核的系统中，我们无法依赖封装 epoll 的底层连接库来实现对对端关闭连接事件的检测，只能通过上层读取数据时进行区分处理。 */
  /* 不过，2.6.17 版本内核中增加了 EPOLLRDHUP 事件，代表对端断开连接，关于添加这个事件的理由可以参见 “[Patch][RFC] epoll and half closed TCP connections”。 */
  /* 在使用 2.6.17 之后版本内核的服务器系统中，对端连接断开触发的 epoll 事件会包含 EPOLLIN | EPOLLRDHUP，即 0x2001。有了这个事件，对端断开连接的异常就可以在底层进行处理了，不用再移交到上层。 */
  /* https://yangwenbo.com/articles/epoll-event-epollrdhup.html */

  /*   EPOLLPRI in epoll(7) as well as POLLPRI in poll(2) is used to receive these urgent data. */
  /* Sometimes it's necessary to send high-priority (urgent) data over a connection that may have unread low-priority data at the other end. For example, a user interface process may be interpreting commands and sending them on to another process through a stream connection. The user interface may have filled the stream with as yet unprocessed requests when the user types a command to cancel all outstanding requests. Rather than have the high-priority data wait to be processed after the low-priority data, you can have it sent as out-of-band (OOB) data or urgent data. */
  /* https://stackoverflow.com/questions/10681624/epollpri-when-does-this-case-happen/12507409#12507409 */

  /* 关于EPOLLPRI */
  ev.events |= EPOLLRDHUP | EPOLLPRI;

  // 原来ev.data的ptr是在这里指定的
  ev.data.ptr = s;

  return epoll_ctl(epfd, op, s->fd, &ev);
}


// 找出下一个可以进行读写操作的socket
int
socknext(Socket **s, int64 timeout)
{
  int r;
  struct epoll_event ev;

  // 轮询，看监听的socket有没读写请求进来
  r = epoll_wait(epfd, &ev, 1, (int)(timeout/1000000));
  if (r == -1 && errno != EINTR) {
    twarn("epoll_wait");
    exit(1);
  }

  if (r > 0) {

    *s = ev.data.ptr;

    /* EPOLLHUP : 文件被挂断。这个事件是一直监控的，即使没有明确指定 */
    /* EPOLLRDHUP : 对端关闭连接或者shutdown写入半连接 */
    // 客户端已经关闭了连接
    if (ev.events & (EPOLLHUP|EPOLLRDHUP)) {

      return 'h';

      // 有数据可读
    } else if (ev.events & EPOLLIN) {

      return 'r';

      // 有数据可写
    } else if (ev.events & EPOLLOUT) {

      return 'w';

    }

  }

  return 0;
}
