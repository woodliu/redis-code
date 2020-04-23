/* Maxmemory directive handling (LRU eviction and other policies).
 *
 * ----------------------------------------------------------------------------
 *
 * Copyright (c) 2009-2016, Salvatore Sanfilippo <antirez at gmail dot com>
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
#include "bio.h"
#include "atomicvar.h"

/* ----------------------------------------------------------------------------
 * Data structures
 * --------------------------------------------------------------------------*/

/* To improve the quality of the LRU approximation we take a set of keys
 * that are good candidate for eviction across freeMemoryIfNeeded() calls.
 *
 * Entries inside the eviciton pool are taken ordered by idle time, putting
 * greater idle times to the right (ascending order).
 *
 * When an LFU policy is used instead, a reverse frequency indication is used
 * instead of the idle time, so that we still evict by larger value (larger
 * inverse frequency means to evict keys with the least frequent accesses).
 *
 * Empty entries have the key pointer set to NULL. */
/* 此处试下了两种内存回收策略，LRU和LFU，前者根据最近最少原则驱逐内存，后者根据使用频率原则驱逐内存，eviction pool中的元素按照空闲时间由小到大排序；
   当使用LFU策略时，使用反向频率(驱逐概率与使用频率成反比)代替空闲时间，这样就可能导致驱逐具有大量空闲内存的元素(如果该元素使用频率最低)*/
#define EVPOOL_SIZE 16
#define EVPOOL_CACHED_SDS_SIZE 255
struct evictionPoolEntry {
    unsigned long long idle;    /* Object idle time (inverse frequency for LFU) */
    sds key;                    /* Key name. */
    sds cached;                 /* Cached SDS object for key name. */
    int dbid;                   /* Key DB number. */
};

static struct evictionPoolEntry *EvictionPoolLRU;

/* ----------------------------------------------------------------------------
 * Implementation of eviction, aging and LRU
 * --------------------------------------------------------------------------*/

/* Return the LRU clock, based on the clock resolution. This is a time
 * in a reduced-bits format that can be used to set and check the
 * object->lru field of redisObject structures. */
// 获取LRU的时钟，分辨率为1s。最大值所占的比特位为24位(LRU_BITS)
unsigned int getLRUClock(void) {
    return (mstime()/LRU_CLOCK_RESOLUTION) & LRU_CLOCK_MAX;
}

/* This function is used to obtain the current LRU clock.
 * If the current resolution is lower than the frequency we refresh the
 * LRU clock (as it should be in production servers) we return the
 * precomputed value, otherwise we need to resort to a system call. */
// 选择更高分辨率的lru clock
unsigned int LRU_CLOCK(void) {
    unsigned int lruclock;
    // server.hz表示server.lruclock每秒的刷新次数。如果当前的分辨率大于1000ms，则使用server.lruclock，否则使用分辨率为1000ms
    if (1000/server.hz <= LRU_CLOCK_RESOLUTION) {
        lruclock = server.lruclock;
    } else {
        lruclock = getLRUClock();
    }
    return lruclock;
}

/* Given an object returns the min number of milliseconds the object was never
 * requested, using an approximated LRU algorithm. */
// 获取redis对象空闲的时间，即距离上一次被访问的时间差
unsigned long long estimateObjectIdleTime(robj *o) {
    unsigned long long lruclock = LRU_CLOCK();
    // 如果当前时间大于对象被调用的时间，则直接取时间差作为该对象idle的时间
    if (lruclock >= o->lru) {
        return (lruclock - o->lru) * LRU_CLOCK_RESOLUTION;
    /* 如果当前刷新时间点小于对象上次被访问的时间点，这是不合理的，说明lruclock发生了回环，比较合理的表达应该是(lruclock + LRU_CLOCK_MAX) - o->lru
      ，另外注意该函数的返回类型为unsigned long long，返回结果不受LRU_CLOCK_MAX的限制*/
    } else {
        return (lruclock + (LRU_CLOCK_MAX - o->lru)) *
                    LRU_CLOCK_RESOLUTION;
    }
}

/* freeMemoryIfNeeded() gets called when 'maxmemory' is set on the config
 * file to limit the max memory used by the server, before processing a
 * command.
 *
 * The goal of the function is to free enough memory to keep Redis under the
 * configured memory limit.
 *
 * The function starts calculating how many bytes should be freed to keep
 * Redis under the limit, and enters a loop selecting the best keys to
 * evict accordingly to the configured policy.
 *
 * If all the bytes needed to return back under the limit were freed the
 * function returns C_OK, otherwise C_ERR is returned, and the caller
 * should block the execution of commands that will result in more memory
 * used by the server.
 *
 * ------------------------------------------------------------------------
 *
 * LRU approximation algorithm
 *
 * Redis uses an approximation of the LRU algorithm that runs in constant
 * memory. Every time there is a key to expire, we sample N keys (with
 * N very small, usually in around 5) to populate a pool of best keys to
 * evict of M keys (the pool size is defined by EVPOOL_SIZE).
 *
 * The N keys sampled are added in the pool of good keys to expire (the one
 * with an old access time) if they are better than one of the current keys
 * in the pool.
 *
 * After the pool is populated, the best key we have in the pool is expired.
 * However note that we don't remove keys from the pool when they are deleted
 * so the pool may contain keys that no longer exist.
 *
 * When we try to evict a key, and all the entries in the pool don't exist
 * we populate it again. This time we'll be sure that the pool has at least
 * one key that can be evicted, if there is at least one key that can be
 * evicted in the whole database. */

/* Create a new eviction pool. */
// 创建一个eviction pool，赋值给全局变量EvictionPoolLRU
void evictionPoolAlloc(void) {
    struct evictionPoolEntry *ep;
    int j;

    ep = zmalloc(sizeof(*ep)*EVPOOL_SIZE);
    for (j = 0; j < EVPOOL_SIZE; j++) {
        ep[j].idle = 0;
        ep[j].key = NULL;
        ep[j].cached = sdsnewlen(NULL,EVPOOL_CACHED_SDS_SIZE); // cached的最大长度为EVPOOL_CACHED_SDS_SIZE
        ep[j].dbid = 0;
    }
    EvictionPoolLRU = ep;
}

/* This is an helper function for freeMemoryIfNeeded(), it is used in order
 * to populate the evictionPool with a few entries every time we want to
 * expire a key. Keys with idle time smaller than one of the current
 * keys are added. Keys are always added if there are free entries.
 *
 * We insert keys on place in ascending order, so keys with the smaller
 * idle time are on the left, and keys with the higher idle time on the
 * right. */

void evictionPoolPopulate(int dbid, dict *sampledict, dict *keydict, struct evictionPoolEntry *pool) {
    int j, k, count;
    //
    dictEntry *samples[server.maxmemory_samples];
    // 从sampledict中采样maxmemory_samples个元素，并放到samples中。返回实际采样到的个数count
    count = dictGetSomeKeys(sampledict,samples,server.maxmemory_samples);
    for (j = 0; j < count; j++) {
        unsigned long long idle;
        sds key;
        robj *o;
        dictEntry *de;
        
        // 获取第j个采样的DictEntry
        de = samples[j];
        // 获取该DictEntry对应的key
        key = dictGetKey(de);

        /* If the dictionary we are sampling from is not the main
         * dictionary (but the expires one) we need to lookup the key
         * again in the key dictionary to obtain the value object. */
        /* 如果maxmemory_policy为MAXMEMORY_VOLATILE_TTL，需要将距离过期时间最近的键清除掉;
           那么直接从redisDB->expire对应的字典中获取键对应的过期时间值即可，此时不需要改变已经存在的de。
           如果maxmemory_policy != MAXMEMORY_VOLATILE_TTL且不是从redisDB->dict中获取键的值(使用redisDB->expire中获取的键),
           那么需要从redisDB->dict中获取键对应的值的对象，才能够获取lru字段 ;*/
        if (server.maxmemory_policy != MAXMEMORY_VOLATILE_TTL) {
            if (sampledict != keydict) de = dictFind(keydict, key);
            // 获取该dictEntry的对应的redis对象
            o = dictGetVal(de);
        }

        /* Calculate the idle time according to the policy. This is called
         * idle just because the code initially handled LRU, but is in fact
         * just a score where an higher score means better candidate. */
        // 如果采用的是LRU策略，直接返回当前对象的idle时间
        if (server.maxmemory_policy & MAXMEMORY_FLAG_LRU) {
            idle = estimateObjectIdleTime(o);
        // 如果采用的是LFU策略，
        } else if (server.maxmemory_policy & MAXMEMORY_FLAG_LFU) {
            /* When we use an LRU policy, we sort the keys by idle time
             * so that we expire keys starting from greater idle time.
             * However when the policy is an LFU one, we have a frequency
             * estimation, and we want to evict keys with lower frequency
             * first. So inside the pool we put objects using the inverted
             * frequency subtracting the actual frequency to the maximum
             * frequency of 255. */
            // counter的idle次数，等于最大值255将去当前的访问次数
            idle = 255-LFUDecrAndReturn(o);
        // 如果采用的是MAXMEMORY_VOLATILE_TTL策略
        } else if (server.maxmemory_policy == MAXMEMORY_VOLATILE_TTL) {
            /* In this case the sooner the expire the better. */
            // 直接获取idle时间即可，没有取当前时间差，而是采用了相对idle时间
            idle = ULLONG_MAX - (long)dictGetVal(de);
        } else {
            serverPanic("Unknown eviction policy in evictionPoolPopulate()");
        }

        /* Insert the element inside the pool.
         * First, find the first empty bucket or the first populated
         * bucket that has an idle time smaller than our idle time. */
        // 从pool中找出第一个空的bucket或idle小于要插入元素的idle的bucket。pool中的元素按照idle从小到达排序
        k = 0;
        while (k < EVPOOL_SIZE &&
               pool[k].key &&
               pool[k].idle < idle) k++;
        // 如果pool中没有空闲的bucket且目前pool中的第一个元素的idle小于要插入的元素的idle，说明待插入的元素不适合被驱逐，继续处理下一个元素
        if (k == 0 && pool[EVPOOL_SIZE-1].key != NULL) {
            /* Can't insert if the element is < the worst element we have
             * and there are no empty buckets. */
            continue;
        // 如果是一个空的bucket，可以直接插入该节点。
        } else if (k < EVPOOL_SIZE && pool[k].key == NULL) {
            /* Inserting into empty position. No setup needed before insert. */
        // 如果该bucket非空，且存在pool中的bucket的idle小于要插入的元素的idle
        } else {
            /* Inserting in the middle. Now k points to the first element
             * greater than the element to insert.  */
            // 当前k指向的是第一个idle大于要插入的元素的idle的bucket，且pool中还有空的bucket。则将所有数据从k处右移一位
            if (pool[EVPOOL_SIZE-1].key == NULL) {
                /* Free space on the right? Insert at k shifting
                 * all the elements from k to end to the right. */

                /* Save SDS before overwriting. */
                // 将pool中最后一个bucket中的cached保存到第k个bucket中
                sds cached = pool[EVPOOL_SIZE-1].cached;
                memmove(pool+k+1,pool+k,
                    sizeof(pool[0])*(EVPOOL_SIZE-k-1));
                pool[k].cached = cached;
            // 如果pool中没有空的bucket，则将pool中的左边k(含k)个元素向左移一位，丢弃最左边的元素。同样需要保存丢弃元素的cached
            } else {
                /* No free space on right? Insert at k-1 */
                k--;
                /* Shift all elements on the left of k (included) to the
                 * left, so we discard the element with smaller idle time. */
                sds cached = pool[0].cached; /* Save SDS before overwriting. */
                if (pool[0].key != pool[0].cached) sdsfree(pool[0].key);
                memmove(pool,pool+1,sizeof(pool[0])*k);
                pool[k].cached = cached;
            }
        }

        /* Try to reuse the cached SDS string allocated in the pool entry,
         * because allocating and deallocating this object is costly
         * (according to the profiler, not my fantasy. Remember:
         * premature optimizbla bla bla bla. */
        int klen = sdslen(key);
        // 如果klen大于EVPOOL_CACHED_SDS_SIZE，则由于cached的最大长度为EVPOOL_CACHED_SDS_SIZE，因此无法保存到cached中
        if (klen > EVPOOL_CACHED_SDS_SIZE) {
            pool[k].key = sdsdup(key);
        // 否则将key
        } else {
            memcpy(pool[k].cached,key,klen+1);
            sdssetlen(pool[k].cached,klen);
            pool[k].key = pool[k].cached;
        }
        // 更新pool k处的数据
        pool[k].idle = idle;
        pool[k].dbid = dbid;
    }
}

/* ----------------------------------------------------------------------------
 * LFU (Least Frequently Used) implementation.

 * We have 24 total bits of space in each object in order to implement
 * an LFU (Least Frequently Used) eviction policy, since we re-use the
 * LRU field for this purpose.
 *
 * We split the 24 bits into two fields:
 *
 *          16 bits      8 bits
 *     +----------------+--------+
 *     + Last decr time | LOG_C  |
 *     +----------------+--------+
 *
 * LOG_C is a logarithmic counter that provides an indication of the access
 * frequency. However this field must also be decremented otherwise what used
 * to be a frequently accessed key in the past, will remain ranked like that
 * forever, while we want the algorithm to adapt to access pattern changes.
 *
 * So the remaining 16 bits are used in order to store the "decrement time",
 * a reduced-precision Unix time (we take 16 bits of the time converted
 * in minutes since we don't care about wrapping around) where the LOG_C
 * counter is halved if it has an high value, or just decremented if it
 * has a low value.
 *
 * New keys don't start at zero, in order to have the ability to collect
 * some accesses before being trashed away, so they start at COUNTER_INIT_VAL.
 * The logarithmic increment performed on LOG_C takes care of COUNTER_INIT_VAL
 * when incrementing the key, so that keys starting at COUNTER_INIT_VAL
 * (or having a smaller value) have a very high chance of being incremented
 * on access.
 *
 * During decrement, the value of the logarithmic counter is halved if
 * its current value is greater than two times the COUNTER_INIT_VAL, otherwise
 * it is just decremented by one.
 * --------------------------------------------------------------------------*/

/* Return the current time in minutes, just taking the least significant
 * 16 bits. The returned time is suitable to be stored as LDT (last decrement
 * time) for the LFU implementation. */
/* 获取当前的时间，单位分钟。保留低16位。与redisObject->lru的高16位对应 */
unsigned long LFUGetTimeInMinutes(void) {
    return (server.unixtime/60) & 65535;
}

/* Given an object last access time, compute the minimum number of minutes
 * that elapsed since the last access. Handle overflow (ldt greater than
 * the current 16 bits minutes time) considering the time as wrapping
 * exactly once. */
// 判断距离上一次访问经过的时间，单位分钟
unsigned long LFUTimeElapsed(unsigned long ldt) {
    unsigned long now = LFUGetTimeInMinutes();
    if (now >= ldt) return now-ldt;
    return 65535-ldt+now;
}

/* Logarithmically increment a counter. The greater is the current counter value
 * the less likely is that it gets really implemented. Saturate it at 255. */
uint8_t LFULogIncr(uint8_t counter) {
    if (counter == 255) return 255;
    double r = (double)rand()/RAND_MAX;
    double baseval = counter - LFU_INIT_VAL;
    if (baseval < 0) baseval = 0;
    double p = 1.0/(baseval*server.lfu_log_factor+1);
    if (r < p) counter++;
    return counter;
}

/* If the object decrement time is reached decrement the LFU counter but
 * do not update LFU fields of the object, we update the access time
 * and counter in an explicit way when the object is really accessed.
 * And we will times halve the counter according to the times of
 * elapsed time than server.lfu_decay_time.
 * Return the object frequency counter.
 *
 * This function is used in order to scan the dataset for the best object
 * to fit: as we check for the candidate, we incrementally decrement the
 * counter of the scanned objects if needed. */
// 根据配置的对象LFU衰退时间计算当前对象的访问次数。lfu_decay_time的值来自redis.conf
unsigned long LFUDecrAndReturn(robj *o) {

/* LFU下o->lru的结构如下：
        访问时间     访问频率          
    +-----------------------+
    |     16 bit    | 8 bit |
    +-----------------------+
*/
    // 获取对象的访问时间，单位分钟
    unsigned long ldt = o->lru >> 8;
    // 获取对象的访问次数
    unsigned long counter = o->lru & 255;
    // 如果启用了lfu衰退时间，则根据经过的时间减少对象的访问次数。每经过lfu_decay_time分钟，则counter减一
    unsigned long num_periods = server.lfu_decay_time ? LFUTimeElapsed(ldt) / server.lfu_decay_time : 0;
    // 如果有衰退的次数，则从当前counter中减去
    if (num_periods)
        counter = (num_periods > counter) ? 0 : counter - num_periods;
    return counter;
}

/* ----------------------------------------------------------------------------
 * The external API for eviction: freeMemroyIfNeeded() is called by the
 * server when there is data to add in order to make space if needed.
 * --------------------------------------------------------------------------*/

/* We don't want to count AOF buffers and slaves output buffers as
 * used memory: the eviction should use mostly data size. This function
 * returns the sum of AOF and slaves buffer. */
// 本函数用于获取AOF和slaves的buffer之和，它们不计入已使用的内存中，这些内存不会被驱逐。可以驱逐的是保存的数据。
size_t freeMemoryGetNotCountedMemory(void) {
    size_t overhead = 0;
    int slaves = listLength(server.slaves);

    if (slaves) {
        listIter li;
        listNode *ln;
        // 使用迭代器计算所有slave使用的buffer之和
        listRewind(server.slaves,&li);
        while((ln = listNext(&li))) {
            client *slave = listNodeValue(ln);
            overhead += getClientOutputBufferMemoryUsage(slave);
        }
    }
    // 获取AOF使用的buffer之和，为server.aof_buf加上aof_rewrite_buf_blocks使用的buffer
    if (server.aof_state != AOF_OFF) {
        overhead += sdsalloc(server.aof_buf)+aofRewriteBufferSize();
    }
    return overhead;
}

/* Get the memory status from the point of view of the maxmemory directive:
 * if the memory used is under the maxmemory setting then C_OK is returned.
 * Otherwise, if we are over the memory limit, the function returns
 * C_ERR.
 *
 * The function may return additional info via reference, only if the
 * pointers to the respective arguments is not NULL. Certain fields are
 * populated only when C_ERR is returned:
 *
 *  'total'     total amount of bytes used.
 *              (Populated both for C_ERR and C_OK)
 *
 *  'logical'   the amount of memory used minus the slaves/AOF buffers.
 *              (Populated when C_ERR is returned)
 *
 *  'tofree'    the amount of memory that should be released
 *              in order to return back into the memory limits.
 *              (Populated when C_ERR is returned)
 *
 *  'level'     this usually ranges from 0 to 1, and reports the amount of
 *              memory currently used. May be > 1 if we are over the memory
 *              limit.
 *              (Populated both for C_ERR and C_OK)
 */
/* 获取当前内存使用情况，返回C_OK则表示不需要进行内存释放，返回C_ERR表示数据占用的内存logical大于maxmemory，需要进行内存释放
  total：上报的内存
  logical：逻辑上已使用的内存。logical = total - (AOF_buffer + slaves_buffer)
  tofree:需要释放的内存量
  level:作为返回的标志位，0表示当前已使用内存没有设置maxmemory限制；非0表示当前已使用内存与maxmemory的比例*/
int getMaxmemoryState(size_t *total, size_t *logical, size_t *tofree, float *level) {
    size_t mem_reported, mem_used, mem_tofree;

    /* Check if we are over the memory usage limit. If we are not, no need
     * to subtract the slaves output buffers. We can just return ASAP. */
    // 获取上报的总内存，上报的总内存内容不能直接与maxmemory对比。见下
    mem_reported = zmalloc_used_memory();
    if (total) *total = mem_reported;

    /* We may return ASAP if there is no need to compute the level. */
    // 如果没有设置maxmemory(值为0),或当前使用的内存小于maxmemory限值，则无需释放内存，直接返回
    int return_ok_asap = !server.maxmemory || mem_reported <= server.maxmemory;
    if (return_ok_asap && !level) return C_OK;

    /* Remove the size of slaves output buffers and AOF buffer from the
     * count of used memory. */
    mem_used = mem_reported;
    // slave和AOF使用的内存不计入已使用内存，这些内存不能被驱逐。
    size_t overhead = freeMemoryGetNotCountedMemory();
    // 计算出已使用的内存，mem_used < overhead的情况可能是因为异步内存操作导致的
    mem_used = (mem_used > overhead) ? mem_used-overhead : 0;

    /* Compute the ratio of memory usage. */
    // 计算level的比例
    if (level) {
        if (!server.maxmemory) {
            *level = 0;
        } else {
            *level = (float)mem_used / (float)server.maxmemory;
        }
    }

    // 如果没有必要进行内存回收，直接返回
    if (return_ok_asap) return C_OK;

    /* Check if we are still over the memory limit. */
    if (mem_used <= server.maxmemory) return C_OK;

    /* Compute how much memory we need to free. */
    // 计算需要释放的内存
    mem_tofree = mem_used - server.maxmemory;
    // 逻辑上使用的内存
    if (logical) *logical = mem_used;
    if (tofree) *tofree = mem_tofree;

    return C_ERR;
}

/* This function is periodically called to see if there is memory to free
 * according to the current "maxmemory" settings. In case we are over the
 * memory limit, the function will try to free some memory to return back
 * under the limit.
 *
 * The function returns C_OK if we are under the memory limit or if we
 * were over the limit, but the attempt to free memory was successful.
 * Otehrwise if we are over the memory limit, but not enough memory
 * was freed to return back under the limit, the function returns C_ERR. */
// 该函数会被周期性地调度，检查当前使用的内存是否达到maxmemory的限定，如果是则会执行内存回收，直到使用的内存小于maxmemory
int freeMemoryIfNeeded(void) {
    int keys_freed = 0;
    /* By default replicas should ignore maxmemory
     * and just be masters exact copies. */
    /* 参见redis.conf中replica-ignore-maxmemory的解释。如果是从且设置了repl_slave_ignore_maxmemory=yes(默认)，
       则从redis不会受maxmemory的限制，由master发生DEL命令来驱逐key。这样做的目的是为了主从的保证一致性。不建议设置为no */
    if (server.masterhost && server.repl_slave_ignore_maxmemory) return C_OK;

    size_t mem_reported, mem_tofree, mem_freed;
    mstime_t latency, eviction_latency;
    long long delta;
    int slaves = listLength(server.slaves);

    /* When clients are paused the dataset should be static not just from the
     * POV of clients not being able to write, but also from the POV of
     * expires and evictions of keys not being performed. */
    if (clientsArePaused()) return C_OK;
    // 如果返回C_OK，则无需进行内存回收，直接返回
    if (getMaxmemoryState(&mem_reported,NULL,&mem_tofree,NULL) == C_OK)
        return C_OK;

    //统计已释放的内存量
    mem_freed = 0;

    // MAXMEMORY_NO_EVICTION策略下不会进行内存回收，直接返回
    if (server.maxmemory_policy == MAXMEMORY_NO_EVICTION)
        goto cant_free; /* We need to free memory, but policy forbids. */

    latencyStartMonitor(latency);
    // 循环进行内存释放，直到已释放的内存不小于需要释放的内存
    while (mem_freed < mem_tofree) {
        int j, k, i;
        static unsigned int next_db = 0;
        sds bestkey = NULL;
        int bestdbid;
        redisDb *db;
        dict *dict;
        dictEntry *de;

        // 采用LRU或LFU或TTL策略进行内存回收，这三个策略都与对象的idle时间有关
        if (server.maxmemory_policy & (MAXMEMORY_FLAG_LRU|MAXMEMORY_FLAG_LFU) ||
            server.maxmemory_policy == MAXMEMORY_VOLATILE_TTL)
        {
            struct evictionPoolEntry *pool = EvictionPoolLRU;

            while(bestkey == NULL) {
                unsigned long total_keys = 0, keys;

                /* We don't want to make local-db choices when expiring keys,
                 * so to start populate the eviction pool sampling keys from
                 * every DB. */
                for (i = 0; i < server.dbnum; i++) {
                    db = server.db+i;
                    // 获取需要执行驱逐的db的key集合。如果采用了MAXMEMORY_FLAG_ALLKEYS则会对db中的所有字典进行采样，否则仅采样过期的字典
                    dict = (server.maxmemory_policy & MAXMEMORY_FLAG_ALLKEYS) ?
                            db->dict : db->expires;
                    if ((keys = dictSize(dict)) != 0) {
                        // 采样获取需要被驱逐的元素，放到pool中，pool中的元素的idle按从小到大排序
                        evictionPoolPopulate(i, dict, db->dict, pool);
                        total_keys += keys;
                    }
                }
                // 如果没有key可以被驱逐，则直接返回。可能的情况是没有使用MAXMEMORY_FLAG_ALLKEYS策略，且所有的key都没有被标记为过期
                if (!total_keys) break; /* No keys to evict. */

                /* Go backward from best to worst element to evict. */
                // 由于pool中的元素的idle按从小到大排序，因此越往后的元素越适合被驱逐
                for (k = EVPOOL_SIZE-1; k >= 0; k--) {
                    // 如果没有元素，则继续遍历下一个元素
                    if (pool[k].key == NULL) continue;
                    // 获取该key所在的dbid
                    bestdbid = pool[k].dbid;
                    // 如果策略包含MAXMEMORY_FLAG_ALLKEYS，则从dict字典中获取对象，否则从expire字典中获取对象
                    if (server.maxmemory_policy & MAXMEMORY_FLAG_ALLKEYS) {
                        de = dictFind(server.db[pool[k].dbid].dict,
                            pool[k].key);
                    } else {
                        de = dictFind(server.db[pool[k].dbid].expires,
                            pool[k].key);
                    }

                    /* Remove the entry from the pool. */
                    // 选出需要释放的元素后，释放掉pool中的元素。
                    if (pool[k].key != pool[k].cached)
                        sdsfree(pool[k].key);
                    pool[k].key = NULL;
                    pool[k].idle = 0;

                    /* If the key exists, is our pick. Otherwise it is
                     * a ghost and we need to try the next element. */
                    if (de) {
                        bestkey = dictGetKey(de);
                        break;
                    } else {
                        /* Ghost... Iterate again. */
                    }
                }
            }
        }

        // 如果采用的是MAXMEMORY_ALLKEYS_RANDOM，MAXMEMORY_VOLATILE_RANDOM。这两种策略是随机选择，与idle无关
        /* volatile-random and allkeys-random policy */
        else if (server.maxmemory_policy == MAXMEMORY_ALLKEYS_RANDOM ||
                 server.maxmemory_policy == MAXMEMORY_VOLATILE_RANDOM)
        {
            /* When evicting a random key, we try to evict a key for
             * each DB, so we use the static 'next_db' variable to
             * incrementally visit all DBs. */
            for (i = 0; i < server.dbnum; i++) {
                j = (++next_db) % server.dbnum;
                db = server.db+j;
                // 如果是所有key随机选择，则从db->dict中选择，否则从过期字典db->expires中选择，
                dict = (server.maxmemory_policy == MAXMEMORY_ALLKEYS_RANDOM) ?
                        db->dict : db->expires;
                if (dictSize(dict) != 0) {
                    de = dictGetRandomKey(dict);
                    bestkey = dictGetKey(de);
                    bestdbid = j;
                    break;
                }
            }
        }
        
        // 释放选择好的db   ----待完成
        /* Finally remove the selected key. */
        if (bestkey) {
            // 通过偏移量获取bestkey对应的db
            db = server.db+bestdbid;
            // 根据得到的sds类型的key创建redisObject对象
            robj *keyobj = createStringObject(bestkey,sdslen(bestkey));
            // 向slave和AOF传递过期消息，用于保持数据的一致性
            propagateExpire(db,keyobj,server.lazyfree_lazy_eviction);
            /* We compute the amount of memory freed by db*Delete() alone.
             * It is possible that actually the memory needed to propagate
             * the DEL in AOF and replication link is greater than the one
             * we are freeing removing the key, but we can't account for
             * that otherwise we would never exit the loop.
             *
             * AOF and Output buffer memory will be freed eventually so
             * we only care about memory used by the key space. */
            delta = (long long) zmalloc_used_memory();
            latencyStartMonitor(eviction_latency);
            if (server.lazyfree_lazy_eviction)
                dbAsyncDelete(db,keyobj);
            else
                dbSyncDelete(db,keyobj);
            latencyEndMonitor(eviction_latency);
            latencyAddSampleIfNeeded("eviction-del",eviction_latency);
            latencyRemoveNestedEvent(latency,eviction_latency);
            delta -= (long long) zmalloc_used_memory();
            mem_freed += delta;
            server.stat_evictedkeys++;
            notifyKeyspaceEvent(NOTIFY_EVICTED, "evicted",
                keyobj, db->id);
            decrRefCount(keyobj);
            keys_freed++;

            /* When the memory to free starts to be big enough, we may
             * start spending so much time here that is impossible to
             * deliver data to the slaves fast enough, so we force the
             * transmission here inside the loop. */
            if (slaves) flushSlavesOutputBuffers();

            /* Normally our stop condition is the ability to release
             * a fixed, pre-computed amount of memory. However when we
             * are deleting objects in another thread, it's better to
             * check, from time to time, if we already reached our target
             * memory, since the "mem_freed" amount is computed only
             * across the dbAsyncDelete() call, while the thread can
             * release the memory all the time. */
            if (server.lazyfree_lazy_eviction && !(keys_freed % 16)) {
                if (getMaxmemoryState(NULL,NULL,NULL,NULL) == C_OK) {
                    /* Let's satisfy our stop condition. */
                    mem_freed = mem_tofree;
                }
            }
        } else {
            latencyEndMonitor(latency);
            latencyAddSampleIfNeeded("eviction-cycle",latency);
            goto cant_free; /* nothing to free... */
        }
    }
    latencyEndMonitor(latency);
    latencyAddSampleIfNeeded("eviction-cycle",latency);
    return C_OK;

cant_free:
    /* We are here if we are not able to reclaim memory. There is only one
     * last thing we can try: check if the lazyfree thread has jobs in queue
     * and wait... */
    while(bioPendingJobsOfType(BIO_LAZY_FREE)) {
        if (((mem_reported - zmalloc_used_memory()) + mem_freed) >= mem_tofree)
            break;
        usleep(1000);
    }
    return C_ERR;
}

/* This is a wrapper for freeMemoryIfNeeded() that only really calls the
 * function if right now there are the conditions to do so safely:
 *
 * - There must be no script in timeout condition.
 * - Nor we are loading data right now.
 *
 */
int freeMemoryIfNeededAndSafe(void) {
    if (server.lua_timedout || server.loading) return C_OK;
    return freeMemoryIfNeeded();
}
