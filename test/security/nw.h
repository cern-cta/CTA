

#ifndef __NW_H
#define __NW_H

int nw_create_socket(unsigned short port);
ssize_t nw_readn(int fd, void *vptr, size_t n);
ssize_t nw_writen(int fd, const void *vptr, size_t n);
int nw_connect_to_server(char *host, unsigned short port);
ssize_t nw_readline(int fd, void *vptr, size_t maxlen);
#endif
