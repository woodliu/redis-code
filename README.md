基于redis [6.0-rc3](https://github.com/antirez/redis/archive/6.0-rc3.tar.gz)。

参考：[Redis-Code](https://github.com/linyiqun/Redis-Code)

建议看下官方的[README](./README-Original.md)文档，对模块的介绍内容放在了对应的头文件中。

当前进度如下：

- 正在进行：

  [sds.c](./src/sds.c)

  [connection.h](./src/connection.h)/[connection.c](

- 已完成：

  [anet.c](./src/anet.c)

  [ae.h](./src/ae.h)/[ae.c](./src/ae.c)

  [ae_epoll.c](./src/ae_epoll.c)

  [connection.h](./src/connection.h)/[connection.c](./src/connection.c)