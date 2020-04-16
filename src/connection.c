/*
 * Copyright (c) 2019, Redis Labs
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "server.h"
#include "connhelpers.h"

/* The connections module provides a lean abstraction of network connections
 * to avoid direct socket and async event management across the Redis code base.
 *
 * It does NOT provide advanced connection features commonly found in similar
 * libraries such as complete in/out buffer management, throttling, etc. These
 * functions remain in networking.c.
 *
 * The primary goal is to allow transparent handling of TCP and TLS based
 * connections. To do so, connections have the following properties:
 *
 * 1. A connection may live before its corresponding socket exists.  This
 *    allows various context and configuration setting to be handled before
 *    establishing the actual connection.
 * 2. The caller may register/unregister logical read/write handlers to be
 *    called when the connection has data to read from/can accept writes.
 *    These logical handlers may or may not correspond to actual AE events,
 *    depending on the implementation (for TCP they are; for TLS they aren't).
 */

ConnectionType CT_Socket;

/* When a connection is created we must know its type already, but the
 * underlying socket may or may not exist:
 *
 * - For accepted connections, it exists as we do not model the listen/accept
 *   part; So caller calls connCreateSocket() followed by connAccept().
 * - For outgoing connections, the socket is created by the connection module
 *   itself; So caller calls connCreateSocket() followed by connConnect(),
 *   which registers a connect callback that fires on connected/error state
 *   (and after any transport level handshake was done).
 *
 * NOTE: An earlier version relied on connections being part of other structs
 * and not independently allocated. This could lead to further optimizations
 * like using container_of(), etc.  However it was discontinued in favor of
 * this approach for these reasons:
 *
 * 1. In some cases conns are created/handled outside the context of the
 * containing struct, in which case it gets a bit awkward to copy them.
 * 2. Future implementations may wish to allocate arbitrary data for the
 * connection.
 * 3. The container_of() approach is anyway risky because connections may
 * be embedded in different structs, not just client.
 */

// 初始化一个connection结构体，此时并没关联真正的网络连接
connection *connCreateSocket() {
    connection *conn = zcalloc(sizeof(connection));
    conn->type = &CT_Socket;
    conn->fd = -1;

    return conn;
}

/* Create a new socket-type connection that is already associated with
 * an accepted connection.
 *
 * The socket is not read for I/O until connAccept() was called and
 * invoked the connection-level accept handler.
 */
// 关联一个socket类型的连接，此时还没有从该连接上读取任何数据，执行connSocketAccept之后state状态会变为CONN_STATE_CONNECTED
connection *connCreateAcceptedSocket(int fd) {
    connection *conn = connCreateSocket();
    conn->fd = fd;
    conn->state = CONN_STATE_ACCEPTING;
    return conn;
}

// 客户端发起连接，并设置为非阻塞模式
static int connSocketConnect(connection *conn, const char *addr, int port, const char *src_addr,
        ConnectionCallbackFunc connect_handler) {
    // client使用绑定源地址的方式执行connect操作，连接服务端，此时状态设置为CONN_STATE_CONNECTING
    int fd = anetTcpNonBlockBestEffortBindConnect(NULL,addr,port,src_addr);
    if (fd == -1) {
        conn->state = CONN_STATE_ERROR;
        conn->last_errno = errno;
        return C_ERR;
    }

    conn->fd = fd;
    // CONN_STATE_CONNECTING状态下需要执行连接处理函数connect_handler
    conn->state = CONN_STATE_CONNECTING;

    conn->conn_handler = connect_handler;
    // 对连接的描述符注册写事件通知
    aeCreateFileEvent(server.el, conn->fd, AE_WRITABLE,
            conn->type->ae_handler, conn);

    return C_OK;
}

/* Returns true if a write handler is registered */
// 判断是否注册了写处理函数
int connHasWriteHandler(connection *conn) {
    return conn->write_handler != NULL;
}

/* Returns true if a read handler is registered */
// 判断是否注册了读处理函数
int connHasReadHandler(connection *conn) {
    return conn->read_handler != NULL;
}

/* Associate a private data pointer with the connection */
void connSetPrivateData(connection *conn, void *data) {
    conn->private_data = data;
}

/* Get the associated private data pointer */
void *connGetPrivateData(connection *conn) {
    return conn->private_data;
}

/* ------ Pure socket connections ------- */

/* A very incomplete list of implementation-specific calls.  Much of the above shall
 * move here as we implement additional connection types.
 */

/* Close the connection and free resources. */
// 关闭连接，并去注册事件通知
static void connSocketClose(connection *conn) {
    if (conn->fd != -1) {
        // 去注册conn->fd描述符对应的读写事件后关闭该socket。redis的文本事件只支持读写。
        aeDeleteFileEvent(server.el,conn->fd,AE_READABLE);
        aeDeleteFileEvent(server.el,conn->fd,AE_WRITABLE);
        close(conn->fd);
        conn->fd = -1;
    }

    /* If called from within a handler, schedule the close but
     * keep the connection until the handler returns.
     */
    // 如果连接引用计数非0，则将该连接标记为CONN_FLAG_CLOSE_SCHEDULED；否则保留该连接，等最后一个执行引用者通过执行本函数释放内存
    if (connHasRefs(conn)) {
        conn->flags |= CONN_FLAG_CLOSE_SCHEDULED;
        return;
    }

    // 如果引用计数为0，则释放该连接
    zfree(conn);
}

// 向连接写入数据
static int connSocketWrite(connection *conn, const void *data, size_t data_len) {
    int ret = write(conn->fd, data, data_len);
    if (ret < 0 && errno != EAGAIN) {
        conn->last_errno = errno;
        conn->state = CONN_STATE_ERROR;
    }

    return ret;
}

// 从连接读取数据
static int connSocketRead(connection *conn, void *buf, size_t buf_len) {
    int ret = read(conn->fd, buf, buf_len);
    if (!ret) {
        conn->state = CONN_STATE_CLOSED;
    } else if (ret < 0 && errno != EAGAIN) {
        conn->last_errno = errno;
        conn->state = CONN_STATE_ERROR;
    }

    return ret;
}

// 接收连接，类似监听socket接收到连接，状态设置为CONN_STATE_CONNECTED
static int connSocketAccept(connection *conn, ConnectionCallbackFunc accept_handler) {
    int ret = C_OK;

    if (conn->state != CONN_STATE_ACCEPTING) return C_ERR;
    conn->state = CONN_STATE_CONNECTED;

    connIncrRefs(conn);
    if (!callHandler(conn, accept_handler)) ret = C_ERR;
    connDecrRefs(conn);

    return ret;
}

/* Register a write handler, to be called when the connection is writable.
 * If NULL, the existing handler is removed.
 *
 * The barrier flag indicates a write barrier is requested, resulting with
 * CONN_FLAG_WRITE_BARRIER set. This will ensure that the write handler is
 * always called before and not after the read handler in a single event
 * loop.
 */
/* 注册事件的写处理函数，如果conn->write_handler为NULL，则删除注册的文件写事件；否则注册文件写事件。
   文件处理函数conn->type->ae_handler即connSocketEventHandler，如果conn->flags设置了写屏障CONN_FLAG_WRITE_BARRIER，
   它不会在aeProcessEvents函数中判断写屏障时生效，而是会在调用事件处理函数时生效，相当于写屏障延后处理 */
static int connSocketSetWriteHandler(connection *conn, ConnectionCallbackFunc func, int barrier) {
    if (func == conn->write_handler) return C_OK;

    conn->write_handler = func;
    // 如果禁用写屏障，则清除conn->flags上的CONN_FLAG_WRITE_BARRIER；否则设置CONN_FLAG_WRITE_BARRIER
    if (barrier)
        conn->flags |= CONN_FLAG_WRITE_BARRIER;
    else
        conn->flags &= ~CONN_FLAG_WRITE_BARRIER;
    if (!conn->write_handler)
        aeDeleteFileEvent(server.el,conn->fd,AE_WRITABLE);
    else
        if (aeCreateFileEvent(server.el,conn->fd,AE_WRITABLE,
                    conn->type->ae_handler,conn) == AE_ERR) return C_ERR;
    return C_OK;
}

/* 注册事件的读处理函数，如果conn->read_handler为NULL，则删除注册的文件读事件；否则注册文件读事件 */
/* Register a read handler, to be called when the connection is readable.
 * If NULL, the existing handler is removed.
 */
static int connSocketSetReadHandler(connection *conn, ConnectionCallbackFunc func) {
    if (func == conn->read_handler) return C_OK;

    conn->read_handler = func;
    if (!conn->read_handler)
        aeDeleteFileEvent(server.el,conn->fd,AE_READABLE);
    else
        if (aeCreateFileEvent(server.el,conn->fd,
                    AE_READABLE,conn->type->ae_handler,conn) == AE_ERR) return C_ERR;
    return C_OK;
}

static const char *connSocketGetLastError(connection *conn) {
    return strerror(conn->last_errno);
}

// 该函数被赋值到CT_Socket.ae_handler，作为读写事件处理函数
static void connSocketEventHandler(struct aeEventLoop *el, int fd, void *clientData, int mask)
{
    UNUSED(el);
    UNUSED(fd);
    connection *conn = clientData;
    
    /* 如果处理CONN_STATE_CONNECTING状态，且注册了写事件，且注册了连接处理函数，对应connSocketConnect函数的处理，即
       在连接开始时进行一些处理 */
    if (conn->state == CONN_STATE_CONNECTING &&
            (mask & AE_WRITABLE) && conn->conn_handler) {

        // 如果连接没有错误，则将状态设置为CONN_STATE_CONNECTED
        if (connGetSocketError(conn)) {
            conn->last_errno = errno;
            conn->state = CONN_STATE_ERROR;
        } else {
            conn->state = CONN_STATE_CONNECTED;
        }

        // 如果没有写处理函数，则去注册写事件
        if (!conn->write_handler) aeDeleteFileEvent(server.el,conn->fd,AE_WRITABLE);

        // 调用连接处理函数,对连接进行相关处理
        if (!callHandler(conn, conn->conn_handler)) return;
        conn->conn_handler = NULL;
    }

    /* Normally we execute the readable event first, and the writable
     * event later. This is useful as sometimes we may be able
     * to serve the reply of a query immediately after processing the
     * query.
     *
     * However if WRITE_BARRIER is set in the mask, our application is
     * asking us to do the reverse: never fire the writable event
     * after the readable. In such a case, we invert the calls.
     * This is useful when, for instance, we want to do things
     * in the beforeSleep() hook, like fsync'ing a file to disk,
     * before replying to a client. */
    // 下面的处理与aeProcessEvents中对写屏障的处理一致。如果没有设置写屏障，则先读后写，否则反过来
    int invert = conn->flags & CONN_FLAG_WRITE_BARRIER;

    int call_write = (mask & AE_WRITABLE) && conn->write_handler;
    int call_read = (mask & AE_READABLE) && conn->read_handler;

    /* Handle normal I/O flows */
    if (!invert && call_read) {
        if (!callHandler(conn, conn->read_handler)) return;
    }
    /* Fire the writable event. */
    if (call_write) {
        if (!callHandler(conn, conn->write_handler)) return;
    }
    /* If we have to invert the call, fire the readable event now
     * after the writable one. */
    if (invert && call_read) {
        if (!callHandler(conn, conn->read_handler)) return;
    }
}

/* 客户端进行连接，并将socket设置为非阻塞模式，等待事件的超时时间为timeout。连接成功且有写事件时将状态设置为
   CONN_STATE_CONNECTED，可以判断一个连接是否可写 */
static int connSocketBlockingConnect(connection *conn, const char *addr, int port, long long timeout) {
    int fd = anetTcpNonBlockConnect(NULL,addr,port);
    if (fd == -1) {
        conn->state = CONN_STATE_ERROR;
        conn->last_errno = errno;
        return C_ERR;
    }

    // 如果超时后没有任何事件发生，aeWait返回0，将连接状态标记为CONN_STATE_ERROR。否则返回AE_WRITABLE
    if ((aeWait(fd, AE_WRITABLE, timeout) & AE_WRITABLE) == 0) {
        conn->state = CONN_STATE_ERROR;
        conn->last_errno = ETIMEDOUT;
    }

    conn->fd = fd;
    conn->state = CONN_STATE_CONNECTED;
    return C_OK;
}

/* Connection-based versions of syncio.c functions.
 * NOTE: This should ideally be refactored out in favor of pure async work.
 */

static ssize_t connSocketSyncWrite(connection *conn, char *ptr, ssize_t size, long long timeout) {
    return syncWrite(conn->fd, ptr, size, timeout);
}

static ssize_t connSocketSyncRead(connection *conn, char *ptr, ssize_t size, long long timeout) {
    return syncRead(conn->fd, ptr, size, timeout);
}

static ssize_t connSocketSyncReadLine(connection *conn, char *ptr, ssize_t size, long long timeout) {
    return syncReadLine(conn->fd, ptr, size, timeout);
}

// 初始化ConnectionType
ConnectionType CT_Socket = {
    .ae_handler = connSocketEventHandler,
    .close = connSocketClose,
    .write = connSocketWrite,
    .read = connSocketRead,
    .accept = connSocketAccept,
    .connect = connSocketConnect,
    .set_write_handler = connSocketSetWriteHandler,
    .set_read_handler = connSocketSetReadHandler,
    .get_last_error = connSocketGetLastError,
    .blocking_connect = connSocketBlockingConnect,
    .sync_write = connSocketSyncWrite,
    .sync_read = connSocketSyncRead,
    .sync_readline = connSocketSyncReadLine
};


int connGetSocketError(connection *conn) {
    int sockerr = 0;
    socklen_t errlen = sizeof(sockerr);

    if (getsockopt(conn->fd, SOL_SOCKET, SO_ERROR, &sockerr, &errlen) == -1)
        sockerr = errno;
    return sockerr;
}

int connPeerToString(connection *conn, char *ip, size_t ip_len, int *port) {
    return anetPeerToString(conn ? conn->fd : -1, ip, ip_len, port);
}

int connFormatPeer(connection *conn, char *buf, size_t buf_len) {
    return anetFormatPeer(conn ? conn->fd : -1, buf, buf_len);
}

int connSockName(connection *conn, char *ip, size_t ip_len, int *port) {
    return anetSockName(conn->fd, ip, ip_len, port);
}

int connBlock(connection *conn) {
    if (conn->fd == -1) return C_ERR;
    return anetBlock(NULL, conn->fd);
}

int connNonBlock(connection *conn) {
    if (conn->fd == -1) return C_ERR;
    return anetNonBlock(NULL, conn->fd);
}

int connEnableTcpNoDelay(connection *conn) {
    if (conn->fd == -1) return C_ERR;
    return anetEnableTcpNoDelay(NULL, conn->fd);
}

int connDisableTcpNoDelay(connection *conn) {
    if (conn->fd == -1) return C_ERR;
    return anetDisableTcpNoDelay(NULL, conn->fd);
}

int connKeepAlive(connection *conn, int interval) {
    if (conn->fd == -1) return C_ERR;
    return anetKeepAlive(NULL, conn->fd, interval);
}

int connSendTimeout(connection *conn, long long ms) {
    return anetSendTimeout(NULL, conn->fd, ms);
}

int connRecvTimeout(connection *conn, long long ms) {
    return anetRecvTimeout(NULL, conn->fd, ms);
}

int connGetState(connection *conn) {
    return conn->state;
}

/* Return a text that describes the connection, suitable for inclusion
 * in CLIENT LIST and similar outputs.
 *
 * For sockets, we always return "fd=<fdnum>" to maintain compatibility.
 */
const char *connGetInfo(connection *conn, char *buf, size_t buf_len) {
    snprintf(buf, buf_len-1, "fd=%i", conn->fd);
    return buf;
}

