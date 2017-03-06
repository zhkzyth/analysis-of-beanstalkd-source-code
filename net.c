/*
 * net相关
 * 这里存放的是网络相关的函数，主要是构造server socket用
 *
*/

#include <netdb.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include "dat.h"
#include "sd-daemon.h" // 这个应该是借用了sd-daemon的一些工具函数

// 构造服务器的socket
int
make_server_socket(char *host, char *port)
{
    int fd = -1, flags, r;
    struct linger linger = {0, 0};
    struct addrinfo *airoot, *ai, hints;

    /* See if we got a listen fd from systemd. If so, all socket options etc
     * are already set, so we check that the fd is a TCP listen socket and
     * return. */
    r = sd_listen_fds(1);
    if (r < 0) {
        return twarn("sd_listen_fds"), -1;
    }
    if (r > 0) {
        if (r > 1) {
            twarnx("inherited more than one listen socket;"
                   " ignoring all but the first");
        }
        fd = SD_LISTEN_FDS_START;
        r = sd_is_socket_inet(fd, 0, SOCK_STREAM, 1, 0);
        if (r < 0) {
            errno = -r;
            twarn("sd_is_socket_inet");
            return -1;
        }
        if (!r) {
            twarnx("inherited fd is not a TCP listen socket");
            return -1;
        }
        return fd;
    }

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = PF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;
    r = getaddrinfo(host, port, &hints, &airoot);
    if (r == -1)
      return twarn("getaddrinfo()"), -1;

    for(ai = airoot; ai; ai = ai->ai_next) {
      fd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
      if (fd == -1) {
        twarn("socket()");
        continue;
      }

      flags = fcntl(fd, F_GETFL, 0);
      if (flags < 0) {
        twarn("getting flags");
        close(fd);
        continue;
      }

      r = fcntl(fd, F_SETFL, flags | O_NONBLOCK);
      if (r == -1) {
        twarn("setting O_NONBLOCK");
        close(fd);
        continue;
      }

      flags = 1;

      // 重用端口
      r = setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &flags, sizeof flags);
      if (r == -1) {
        twarn("setting SO_REUSEADDR on fd %d", fd);
        close(fd);
        continue;
      }

      // 做心跳包，不过这个心跳包在服务端主动实现?
      r = setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &flags, sizeof flags);
      if (r == -1) {
        twarn("setting SO_KEEPALIVE on fd %d", fd);
        close(fd);
        continue;
      }

      /* SO_LINGER作用 */
      /* 设置函数close()关闭TCP连接时的行为。缺省close()的行为是，如果有数据残留在socket发送缓冲区中则系统将继续发送这些数据给对方，等待被确认，然后返回。 */
      /* 利用此选项，可以将此缺省行为设置为以下两种 */
      /*   a.立即关闭该连接，通过发送RST分组(而不是用正常的FIN|ACK|FIN|ACK四个分组)来关闭该连接。至于发送缓冲区中如果有未发送完的数据，则丢弃。主动关闭一方的TCP状态则跳过TIMEWAIT，直接进入CLOSED。网上很多人想利用这一点来解决服务器上出现大量的TIMEWAIT状态的socket的问题，但是，这并不是一个好主意，这种关闭方式的用途并不在这儿，实际用途在于服务器在应用层的需求。 */
      /*   b.将连接的关闭设置一个超时。如果socket发送缓冲区中仍残留数据，进程进入睡眠，内核进入定时状态去尽量去发送这些数据。 */
      /*     在超时之前，如果所有数据都发送完且被对方确认，内核用正常的FIN|ACK|FIN|ACK四个分组来关闭该连接，close()成功返回。 */
      /*     如果超时之时，数据仍然未能成功发送及被确认，用上述a方式来关闭此连接。close()返回EWOULDBLOCK。 */
      /* http://blog.chinaunix.net/uid-29075379-id-3904022.html */
      r = setsockopt(fd, SOL_SOCKET, SO_LINGER, &linger, sizeof linger);
      if (r == -1) {
        twarn("setting SO_LINGER on fd %d", fd);
        close(fd);
        continue;
      }

      // 数据包马上发出去，不save buffer
      r = setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flags, sizeof flags);
      if (r == -1) {
        twarn("setting TCP_NODELAY on fd %d", fd);
        close(fd);
        continue;
      }

      if (verbose) {

        char hbuf[NI_MAXHOST], pbuf[NI_MAXSERV], *h = host, *p = port;

        r = getnameinfo(ai->ai_addr, ai->ai_addrlen,
                        hbuf, sizeof hbuf,
                        pbuf, sizeof pbuf,
                        NI_NUMERICHOST|NI_NUMERICSERV);
        if (!r) {
          h = hbuf;
          p = pbuf;
        }

        if (ai->ai_family == AF_INET6) {
          printf("bind %d [%s]:%s\n", fd, h, p);
        } else {
          printf("bind %d %s:%s\n", fd, h, p);
        }
      }

      // 绑定地址
      r = bind(fd, ai->ai_addr, ai->ai_addrlen);
      if (r == -1) {
        twarn("bind()");
        close(fd);
        continue;
      }

      r = listen(fd, 1024); // 最大队列等待的连接数，1024个，考虑到不需要那么多的worker，这个数字是合理的
      if (r == -1) {
        twarn("listen()");
        close(fd);
        continue;
      }

      break; // 绑定成功，直接break出来
    }

    freeaddrinfo(airoot); // 释放不需要的资源，避免内存占用

    if(ai == NULL)
      fd = -1;

    return fd;
}
