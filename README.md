基于redis [6.0-rc3](https://github.com/antirez/redis/archive/6.0-rc3.tar.gz)。

参考：[Redis-Code](https://github.com/linyiqun/Redis-Code)

建议看下官方的[README](./README-Original.md)文档，对模块的介绍内容放在了对应的头文件中。

当前进度如下：

- 正在进行：

  [db.c](./src/db.c)

  [networking.c](./src/networking.c)

  [object.c](./src/object.c)

  [evict.c](./src/evict.c)

  [lazyfree.c](./src/lazyfree.c)

- 已完成：

  [ae.h](./src/ae.h)/[ae.c](./src/ae.c)

  [connection.h](./src/connection.h)/[connection.c](./src/connection.c)/[ae_epoll.c](./src/ae_epoll.c)/[anet.c](./src/anet.c)

  [sds.c](./src/sds.c)/[sds.h](./src/sds.h)/[adlist.c](./src/adlist.c)/[adlist.h](./src/adlist.h)
  
  [dict.c](./src/dict.c)/[dict.h](./src/dict.h)

