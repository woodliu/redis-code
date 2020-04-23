/* Hash Tables Implementation.
 *
 * This file implements in memory hash tables with insert/del/replace/find/
 * get-random-element operations. Hash tables will auto resize if needed
 * tables of power of two in size are used, collisions are handled by
 * chaining. See the source code for more information... :)
 *
 * Copyright (c) 2006-2012, Salvatore Sanfilippo <antirez at gmail dot com>
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

#include "fmacros.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <stdarg.h>
#include <limits.h>
#include <sys/time.h>

#include "dict.h"
#include "zmalloc.h"
#ifndef DICT_BENCHMARK_MAIN
#include "redisassert.h"
#else
#include <assert.h>
#endif

/* Using dictEnableResize() / dictDisableResize() we make possible to
 * enable/disable resizing of the hash table as needed. This is very important
 * for Redis, as we use copy-on-write and don't want to move too much memory
 * around when there is a child performing saving operations.
 *
 * Note that even when dict_can_resize is set to 0, not all resizes are
 * prevented: a hash table is still allowed to grow if the ratio between
 * the number of elements and the buckets > dict_force_resize_ratio. */
/* dict_can_resize和dict_force_resize_ratio控制是否允许扩大字典的哈希表大小。扩大哈希表大小时需要要保证dict_can_resize为1，
   或保证哈希表已有的dictEntry dictht->used与哈希表bucket的比例达到dict_force_resize_ratio，即dictht->used/dictht->size>=dict_force_resize_ratio*/
static int dict_can_resize = 1;
static unsigned int dict_force_resize_ratio = 5; 

/* -------------------------- private prototypes ---------------------------- */

static int _dictExpandIfNeeded(dict *ht);
static unsigned long _dictNextPower(unsigned long size);
static long _dictKeyIndex(dict *ht, const void *key, uint64_t hash, dictEntry **existing);
static int _dictInit(dict *ht, dictType *type, void *privDataPtr);

/* -------------------------- hash functions -------------------------------- */

static uint8_t dict_hash_function_seed[16];

// 将seed拷贝到dict_hash_function_seed中
void dictSetHashFunctionSeed(uint8_t *seed) {
    memcpy(dict_hash_function_seed,seed,sizeof(dict_hash_function_seed));
}

// 获取dict_hash_function_seed
uint8_t *dictGetHashFunctionSeed(void) {
    return dict_hash_function_seed;
}

/* The default hashing function uses SipHash implementation
 * in siphash.c. */

// siphash哈希算法
uint64_t siphash(const uint8_t *in, const size_t inlen, const uint8_t *k);
uint64_t siphash_nocase(const uint8_t *in, const size_t inlen, const uint8_t *k);

uint64_t dictGenHashFunction(const void *key, int len) {
    return siphash(key,len,dict_hash_function_seed);
}

uint64_t dictGenCaseHashFunction(const unsigned char *buf, int len) {
    return siphash_nocase(buf,len,dict_hash_function_seed);
}

/* ----------------------------- API implementation ------------------------- */

/* Reset a hash table already initialized with ht_init().
 * NOTE: This function should only be called by ht_destroy(). */
// 重置哈希表，只能被ht_destroy调用
static void _dictReset(dictht *ht)
{
    ht->table = NULL;
    ht->size = 0;
    ht->sizemask = 0;
    ht->used = 0;
}

/* Create a new hash table */
// 创建一个新的哈希表，并调用_dictInit进行初始化
dict *dictCreate(dictType *type,
        void *privDataPtr)
{
    dict *d = zmalloc(sizeof(*d));

    _dictInit(d,type,privDataPtr);
    return d;
}

/* Initialize the hash table */
// 初始化字典中的哈希表
int _dictInit(dict *d, dictType *type,
        void *privDataPtr)
{
    // 重置字典中的两个哈希表
    _dictReset(&d->ht[0]);
    _dictReset(&d->ht[1]);
    d->type = type;
    d->privdata = privDataPtr;
    d->rehashidx = -1;
    d->iterators = 0;
    return DICT_OK;
}

/* Resize the table to the minimal size that contains all the elements,
 * but with the invariant of a USED/BUCKETS ratio near to <= 1 */
 // 调整哈希表的大小，使其能够包含所有的元素。如果现有的哈希表大小过大，调用该函数可能会减少哈希表长度
int dictResize(dict *d)
{
    unsigned long minimal;

    // 如果设置了无法修改大小或者正在进行rehash(rehash时会调整元素位置，如果此时执行dictResize，会导致数据丢失)，则直接返回错误
    if (!dict_can_resize || dictIsRehashing(d)) return DICT_ERR;
    // 获取已有的哈希节点数
    minimal = d->ht[0].used;
    // 如果已有的元素(键值对)数量少于哈希表的初始值，则将其设置为哈希表初始值即可
    if (minimal < DICT_HT_INITIAL_SIZE)
        minimal = DICT_HT_INITIAL_SIZE;
    // 初始化或扩展哈希表
    return dictExpand(d, minimal);
}

// 扩展或创建哈希表
/* Expand or create the hash table */
int dictExpand(dict *d, unsigned long size)
{
    /* the size is invalid if it is smaller than the number of
     * elements already inside the hash table */
    // 如果正在进行rehash，或入参的size无效，则直接返回
    if (dictIsRehashing(d) || d->ht[0].used > size)
        return DICT_ERR;

    // 新的哈希表
    dictht n; /* the new hash table */
    // 根据size调整哈希表的大小，哈希表大小为刚好大于size的2的幂次
    unsigned long realsize = _dictNextPower(size);

    /* Rehashing to the same table size is not useful. */
    // 如果现有哈希表大小正好是需要调整到的大小，则无需做任何处理
    if (realsize == d->ht[0].size) return DICT_ERR;

    /* Allocate the new hash table and initialize all pointers to NULL */
    // 分配并初始化新的哈希表
    n.size = realsize;
    n.sizemask = realsize-1;
    n.table = zcalloc(realsize*sizeof(dictEntry*));
    n.used = 0;

    /* Is this the first initialization? If so it's not really a rehashing
     * we just set the first hash table so that it can accept keys. */
    // 此处为哈希表的首次初始化，而非rehash，直接将分配的哈希表赋值字典第一个哈希表即可，d->ht[0]
    if (d->ht[0].table == NULL) {
        d->ht[0] = n;
        return DICT_OK;
    }

    /* Prepare a second hash table for incremental rehashing */
    // 如果ht[0]非空，则将扩展的表赋予ht[1]，用于给增量哈希使用
    d->ht[1] = n;
    d->rehashidx = 0;
    return DICT_OK;
}

/* Performs N steps of incremental rehashing. Returns 1 if there are still
 * keys to move from the old to the new hash table, otherwise 0 is returned.
 *
 * Note that a rehashing step consists in moving a bucket (that may have more
 * than one key as we use chaining) from the old to the new hash table, however
 * since part of the hash table may be composed of empty spaces, it is not
 * guaranteed that this function will rehash even a single bucket, since it
 * will visit at max N*10 empty buckets in total, otherwise the amount of
 * work it does would be unbound and the function may block for a long time. */
// 使用步数n进行rehash，n指定了对哈希表bucket查找的次数。返回值为0表示对ht[0]中的所有dictEntry进行了rehash，否则返回1
// rehash的结果有两种：d->ht[0]的元素全部转移到d->ht[1]中，并最终赋值给d->ht[0]，清空d->ht[1]；d->ht[0]和d->ht[1]最终都有一部分元素
int dictRehash(dict *d, int n) {
    /* empty_visits表示每次查找哈希表时能够容忍的空的bucket的总数，如果空的bucket超过这个数目，则本次不做rehash处理，继续下次查找。
       因此，如果哈希表中的空bucket足够多，而n又比较小时，有可能仅会对部分bucket进行rehash，或不会进行任何rehash。
       (当然如果哈希表中的空bucket足够多，也就没有必要rehash) */
    int empty_visits = n*10; /* Max number of empty buckets to visit. */
    // 如果无法执行rehash(即dict->ht[1]为空)，则直接退出
    if (!dictIsRehashing(d)) return 0;

    while(n-- && d->ht[0].used != 0) {
        // 用于遍历所选的bucket中的dictEntry
        dictEntry *de, *nextde;

        /* Note that rehashidx can't overflow as we are sure there are more
         * elements because ht[0].used != 0 */
        assert(d->ht[0].size > (unsigned long)d->rehashidx);
        // 从字典的第一个哈希表d->ht[0]中根据下标查找一个非空的bucket，查找次数受限于empty_visits，当查找的空的元素等于empty_visits时，直接退出
        while(d->ht[0].table[d->rehashidx] == NULL) {
            d->rehashidx++;
            if (--empty_visits == 0) return 1;
        }
        // de为本次查找到的非空的bucket，保存了dictEntry数据的链表，de为链表首节点
        de = d->ht[0].table[d->rehashidx];
        /* Move all the keys in this bucket from the old to the new hash HT */
        // 轮询选择的bucket对应的dictEntry链表
        while(de) {
            uint64_t h;
            
            nextde = de->next;
            /* Get the index in the new hash table */
            /* 根据dictEntry的key计算哈希值，并获取新哈希表的下标。原始的dictEntry是一条链表，
               根据dictEntry->key rehash之后大概率会分散到d->ht[1]的不同bucket中。
               如果原始链表中的所有dictEntry全部落到同一个bucket，下面处理会使得链表元素倒过来
               例如，原始链表为：
                +-----+    +-----+    +-----+
                |  aa +--->+ bb  +--->+ cc  |
                +-----+    +-----+    +-----+
                新链表为：
                +-----+    +-----+    +-----+
                | cc  +--->+  bb +--->+ aa  |
                +-----+    +-----+    +-----+
            */
            h = dictHashKey(d, de->key) & d->ht[1].sizemask;
            de->next = d->ht[1].table[h];
            d->ht[1].table[h] = de;
            // 修正哈希表中已使用的dictEntry数目
            d->ht[0].used--;
            d->ht[1].used++;
            de = nextde;
        }
        // 第一个哈希表中的bucket d->ht[0].table[d->rehashidx]已经经过rehash到第二个哈希表中，清空原始bucket
        d->ht[0].table[d->rehashidx] = NULL;
        // 增加第一个哈希表中的bucket d->ht[0] rehash的索引，下一轮基于此下标继续遍历
        d->rehashidx++;
    }

    /* Check if we already rehashed the whole table... */
    /* 如果原始哈希表中的dictEntry全部rehash到的新的哈希表，则释放原始哈希表ht[0]，并将新的哈希表ht[1]变为ht[0]。返回0
       其中将rehashidx置为-1，表示rehash结束 */ 
    if (d->ht[0].used == 0) {
        zfree(d->ht[0].table);
        d->ht[0] = d->ht[1];
        _dictReset(&d->ht[1]);
        d->rehashidx = -1;
        return 0;
    }
    
    // 如果仅ht[0]中的部分(含0个)dictEntry进行了rehash，则返回1
    /* More to rehash... */
    return 1;
}

// 获取以毫秒为单位的当前时间
long long timeInMilliseconds(void) {
    struct timeval tv;

    gettimeofday(&tv,NULL);
    return (((long long)tv.tv_sec)*1000)+(tv.tv_usec/1000);
}

/* Rehash for an amount of time between ms milliseconds and ms+1 milliseconds */
// 尝试在1ms之内进行rehash，步数100
int dictRehashMilliseconds(dict *d, int ms) {
    long long start = timeInMilliseconds();
    int rehashes = 0;

    // 在1ms之内尝试对d哈希表进行rehash，步数为100。如果rehash时间超过1ms，则退出
    while(dictRehash(d,100)) {
        rehashes += 100;
        if (timeInMilliseconds()-start > ms) break;
    }
    return rehashes;
}

/* This function performs just a step of rehashing, and only if there are
 * no safe iterators bound to our hash table. When we have iterators in the
 * middle of a rehashing we can't mess with the two hash tables otherwise
 * some element can be missed or duplicated.
 *
 * This function is called by common lookup or update operations in the
 * dictionary so that the hash table automatically migrates from H1 to H2
 * while it is actively used. */
/* 单步rehash。仅当不存在绑定到哈希表的安全的迭代器时才会被调用。通常用于查找或更新字典使用。
   在执行add、delete、find操作时，都会判断dict是否正在rehash，如果是，则执行_dictRehashStep()函数，进行增量rehash。
   每次执行 _dictRehashStep, 会将ht[0]->table 哈希表第一个不为空的索引上的所有节点就会全部迁移到 ht[1]->table。
   这样将rehash过程分散到各个查找、插入和删除操作中去了，而不是集中在某一个操作中一次性做完*/
static void _dictRehashStep(dict *d) {
    // 单步rehash需要保证没有正在对该字典进行迭代的迭代器，否则可能导致迭代异常(单步rehash或改变bucket内容)
    if (d->iterators == 0) dictRehash(d,1);
}

/* Add an element to the target hash table */
// 在哈希表中添加一个新的dictEntry，设置key和value
int dictAdd(dict *d, void *key, void *val)
{
    dictEntry *entry = dictAddRaw(d,key,NULL);

    if (!entry) return DICT_ERR;
    dictSetVal(d, entry, val);
    return DICT_OK;
}

/* Low level add or find:
 * This function adds the entry but instead of setting a value returns the
 * dictEntry structure to the user, that will make sure to fill the value
 * field as he wishes.
 *
 * This function is also directly exposed to the user API to be called
 * mainly in order to store non-pointers inside the hash value, example:
 *
 * entry = dictAddRaw(dict,mykey,NULL);
 * if (entry != NULL) dictSetSignedIntegerVal(entry,1000);
 *
 * Return values:
 *
 * If key already exists NULL is returned, and "*existing" is populated
 * with the existing entry if existing is not NULL.
 *
 * If key was added, the hash entry is returned to be manipulated by the caller.
 */
// 在哈希表中添加一个新的dictEntry，此处没有设置key对应的value。最终返回新增的dictEntry的指针。existing用于出参返回已存在的dictEntry
dictEntry *dictAddRaw(dict *d, void *key, dictEntry **existing)
{
    long index;
    dictEntry *entry;
    dictht *ht;

    // 判断是否正在rehash，如果正在进行rehash，则执行一次单步rehash。目的是为了辅助完成rehash
    if (dictIsRehashing(d)) _dictRehashStep(d);

    /* Get the index of the new element, or -1 if
     * the element already exists. */
    // 校验该key对应的元素是否已经存在，如果存在则直接返回，否则返回应该插入的哈希表的索引
    if ((index = _dictKeyIndex(d, key, dictHashKey(d,key), existing)) == -1)
        return NULL;

    /* Allocate the memory and store the new entry.
     * Insert the element in top, with the assumption that in a database
     * system it is more likely that recently added entries are accessed
     * more frequently. */
    // 如果正在进行rehash，则插入到新的哈希表中，否则放到原始哈希表
    ht = dictIsRehashing(d) ? &d->ht[1] : &d->ht[0];
    entry = zmalloc(sizeof(*entry));
    entry->next = ht->table[index];
    ht->table[index] = entry;
    ht->used++;

    /* Set the hash entry fields. */
    // 设置dictEntry的key
    dictSetKey(d, entry, key);
    return entry;
}

/* Add or Overwrite:
 * Add an element, discarding the old value if the key already exists.
 * Return 1 if the key was added from scratch, 0 if there was already an
 * element with such key and dictReplace() just performed a value update
 * operation. */
// 新增或覆盖key对应的value。如果是新增则返回1，如果是覆盖，返回0
int dictReplace(dict *d, void *key, void *val)
{
    dictEntry *entry, *existing, auxentry;

    /* Try to add the element. If the key
     * does not exists dictAdd will succeed. */
    entry = dictAddRaw(d,key,&existing);
    // 如果是新增的，设置value
    if (entry) {
        dictSetVal(d, entry, val);
        return 1;
    }

    /* Set the new value and free the old one. Note that it is important
     * to do that in this order, as the value may just be exactly the same
     * as the previous one. In this context, think to reference counting,
     * you want to increment (set), and then decrement (free), and not the
     * reverse. */
    // 如果是与已有的元素的key重复，覆盖value即可。如果value占用了堆内存，则需要申请新的val并释放原有的val
    auxentry = *existing;
    dictSetVal(d, existing, val);
    dictFreeVal(d, &auxentry);
    return 0;
}

/* Add or Find:
 * dictAddOrFind() is simply a version of dictAddRaw() that always
 * returns the hash entry of the specified key, even if the key already
 * exists and can't be added (in that case the entry of the already
 * existing key is returned.)
 *
 * See dictAddRaw() for more information. */
// 返回与key对应的dictEntry的指针，该元素可能是已有的，也可能是新增的
dictEntry *dictAddOrFind(dict *d, void *key) {
    dictEntry *entry, *existing;
    entry = dictAddRaw(d,key,&existing);
    return entry ? entry : existing;
}

/* Search and remove an element. This is an helper function for
 * dictDelete() and dictUnlink(), please check the top comment
 * of those functions. */
// 删除key对应的dictEntry，如果nofree非0，表示不要释放该节点并返回该节点的指针，否则释放该dictEntry对应的内存
static dictEntry *dictGenericDelete(dict *d, const void *key, int nofree) {
    uint64_t h, idx;
    dictEntry *he, *prevHe;
    int table;
    // 如果所有哈希表中不存在任何dictEntry，则直接返回
    if (d->ht[0].used == 0 && d->ht[1].used == 0) return NULL;

    // 如果正在执行rehash，则执行一次增量rehash
    if (dictIsRehashing(d)) _dictRehashStep(d);
    // 计算key对应的哈希值
    h = dictHashKey(d, key);
    // 遍历dict的两个哈希表
    for (table = 0; table <= 1; table++) {
        // 获取哈希表bucket的索引
        idx = h & d->ht[table].sizemask;
        // 获取索引对应的dictEntry链表首节点
        he = d->ht[table].table[idx];
        prevHe = NULL;
        while(he) {
            // 如果he为需要移除的节点，则将其从链表中移除
            if (key==he->key || dictCompareKeys(d, key, he->key)) {
                /* Unlink the element from the list */
                if (prevHe)
                    prevHe->next = he->next;
                else
                    d->ht[table].table[idx] = he->next;
                // 如果需要释放内存则释放key和value以及dictEntry本身
                if (!nofree) {
                    dictFreeKey(d, he);
                    dictFreeVal(d, he);
                    zfree(he);
                }
                d->ht[table].used--;
                return he;
            }
            prevHe = he;
            he = he->next;
        }
        // 如果无法进行rehash，此时说明第二个哈希表为空，直接返回即可
        if (!dictIsRehashing(d)) break;
    }
    return NULL; /* not found */
}

/* Remove an element, returning DICT_OK on success or DICT_ERR if the
 * element was not found. */
// 删除key对应的dictExtry，(非空)返回DICT_OK表示删除成功，(NULL)DICT_ERR删除失败
int dictDelete(dict *ht, const void *key) {
    return dictGenericDelete(ht,key,0) ? DICT_OK : DICT_ERR;
}

/* Remove an element from the table, but without actually releasing
 * the key, value and dictionary entry. The dictionary entry is returned
 * if the element was found (and unlinked from the table), and the user
 * should later call `dictFreeUnlinkedEntry()` with it in order to release it.
 * Otherwise if the key is not found, NULL is returned.
 *
 * This function is useful when we want to remove something from the hash
 * table but want to use its value before actually deleting the entry.
 * Without this function the pattern would require two lookups:
 *
 *  entry = dictFind(...);
 *  // Do something with entry
 *  dictDelete(dictionary,entry);
 *
 * Thanks to this function it is possible to avoid this, and use
 * instead:
 *
 * entry = dictUnlink(dictionary,entry);
 * // Do something with entry
 * dictFreeUnlinkedEntry(entry); // <- This does not need to lookup again.
 */
// 移除一个key对应的dictEntry，不作释放，返回该dictEntry
dictEntry *dictUnlink(dict *ht, const void *key) {
    return dictGenericDelete(ht,key,1);
}

/* You need to call this function to really free the entry after a call
 * to dictUnlink(). It's safe to call this function with 'he' = NULL. */
// 释放从哈希表中移除的节点的内存，dictUnlink之后调用
void dictFreeUnlinkedEntry(dict *d, dictEntry *he) {
    if (he == NULL) return;
    dictFreeKey(d, he);
    dictFreeVal(d, he);
    zfree(he);
}

/* Destroy an entire dictionary */
// 删除指定哈希表ht中的所有元素，并重新初始化该哈希表
int _dictClear(dict *d, dictht *ht, void(callback)(void *)) {
    unsigned long i;

    /* Free all the elements */
    for (i = 0; i < ht->size && ht->used > 0; i++) {
        dictEntry *he, *nextHe;
        // 处理字典中的私有数据
        if (callback && (i & 65535) == 0) callback(d->privdata);
        
        // 如果哈希表中的bucket为空，则继续处理下一个bucket
        if ((he = ht->table[i]) == NULL) continue;
        // 遍历删除bucket对应的链表元素
        while(he) {
            nextHe = he->next;
            dictFreeKey(d, he);
            dictFreeVal(d, he);
            zfree(he);
            ht->used--;
            he = nextHe;
        }
    }
    /* Free the table and the allocated cache structure */
    // 删除哈希表
    zfree(ht->table);
    /* Re-initialize the table */
    // 重新初始化哈希表
    _dictReset(ht);
    return DICT_OK; /* never fails */
}

/* Clear & Release the hash table */
// 释放字典的所有内存
void dictRelease(dict *d)
{
    _dictClear(d,&d->ht[0],NULL);
    _dictClear(d,&d->ht[1],NULL);
    zfree(d);
}

// 根据key在字典d中查找对应的dictEntry，如果找到则返回指向该dictEntry的指针，否则返回NULL
dictEntry *dictFind(dict *d, const void *key)
{
    dictEntry *he;
    uint64_t h, idx, table;
    
    // 如果当前字典中没有任何元素，直接返回
    if (d->ht[0].used + d->ht[1].used == 0) return NULL; /* dict is empty */
    // 如果正在进行rehash，则执行一次增量rehash
    if (dictIsRehashing(d)) _dictRehashStep(d);
    // 计算key对应的hash值
    h = dictHashKey(d, key);
    
    // 从哈希表中找出一个与key相同的dictEntry，并返回执行该dictEntry的指针
    for (table = 0; table <= 1; table++) {
        idx = h & d->ht[table].sizemask;
        he = d->ht[table].table[idx];
        while(he) {
            if (key==he->key || dictCompareKeys(d, key, he->key))
                return he;
            he = he->next;
        }
        // 如果d->ht[1]为空，则直接退出
        if (!dictIsRehashing(d)) return NULL;
    }
    return NULL;
}

// 从字典中查找key对应的值
void *dictFetchValue(dict *d, const void *key) {
    dictEntry *he;

    he = dictFind(d,key);
    return he ? dictGetVal(he) : NULL;
}

/* A fingerprint is a 64 bit number that represents the state of the dictionary
 * at a given time, it's just a few dict properties xored together.
 * When an unsafe iterator is initialized, we get the dict fingerprint, and check
 * the fingerprint again when the iterator is released.
 * If the two fingerprints are different it means that the user of the iterator
 * performed forbidden operations against the dictionary while iterating. */
/* 获取给定时刻(由此时字典对应的integers[6]决定)的字典的指纹。通常用于判断迭代器的使用者是否有对字典作过变更操作
   如果执行前后的指纹发生了变化，则说明迭代器的使用者执行了非法操作。 */
long long dictFingerprint(dict *d) {
    long long integers[6], hash = 0;
    int j;

    integers[0] = (long) d->ht[0].table;
    integers[1] = d->ht[0].size;
    integers[2] = d->ht[0].used;
    integers[3] = (long) d->ht[1].table;
    integers[4] = d->ht[1].size;
    integers[5] = d->ht[1].used;

    /* We hash N integers by summing every successive integer with the integer
     * hashing of the previous sum. Basically:
     *
     * Result = hash(hash(hash(int1)+int2)+int3) ...
     *
     * This way the same set of integers in a different order will (likely) hash
     * to a different number. */
    for (j = 0; j < 6; j++) {
        hash += integers[j];
        /* For the hashing step we use Tomas Wang's 64 bit integer hash. */
        hash = (~hash) + (hash << 21); // hash = (hash << 21) - hash - 1;
        hash = hash ^ (hash >> 24);
        hash = (hash + (hash << 3)) + (hash << 8); // hash * 265
        hash = hash ^ (hash >> 14);
        hash = (hash + (hash << 2)) + (hash << 4); // hash * 21
        hash = hash ^ (hash >> 28);
        hash = hash + (hash << 31);
    }
    return hash;
}

// 初始化一个迭代器
dictIterator *dictGetIterator(dict *d)
{
    dictIterator *iter = zmalloc(sizeof(*iter));

    iter->d = d;
    iter->table = 0;
    iter->index = -1;
    iter->safe = 0;
    iter->entry = NULL;
    iter->nextEntry = NULL;
    return iter;
}

// 启用一个安全的迭代器
dictIterator *dictGetSafeIterator(dict *d) {
    dictIterator *i = dictGetIterator(d);

    i->safe = 1;
    return i;
}

// 迭代获取哈希表中的dictEntry首节点，获取失败则返回NULL
dictEntry *dictNext(dictIterator *iter)
{
    while (1) {
        // 如果前面没有对该字典执行过迭代，则iter->entry为空，需要获取第一个节点
        if (iter->entry == NULL) {
            // 获取哈希表，默认是第一个哈希表
            dictht *ht = &iter->d->ht[iter->table];
            // 如果是第一次进行迭代，则进行安全和非安全模式的相关操作
            if (iter->index == -1 && iter->table == 0) {
                // 安全模式下仅统计正在对字典d进行迭代的迭代器的数量，非安全模式记录迭代前的字典指纹，方式迭代时修改字典本身
                if (iter->safe)
                    iter->d->iterators++;
                else
                    iter->fingerprint = dictFingerprint(iter->d);
            }
            // 获取迭代的下一个哈希表索引
            iter->index++;
            // 如果索引越界，且字典正在rehash中，则使用第二和哈希表d->ht[1],将迭代索引置为0
            if (iter->index >= (long) ht->size) {
                if (dictIsRehashing(iter->d) && iter->table == 0) {
                    iter->table++;
                    iter->index = 0;
                    ht = &iter->d->ht[1];
                // 如果索引越界且索引无效，说明迭代结束，返回NULL
                } else {
                    break;
                }
            }
            // 使用迭代到的bucket中的链表首节点作为返回值，可能为NULL
            iter->entry = ht->table[iter->index];
        // 如果前面已经对该字典执行过迭代，则获取通过当前迭代到的dictEntry的next指针指向的下一个dictEntry
        } else {
            iter->entry = iter->nextEntry;
        }
        // 如果获取到的dictEntry非空，则将该dictEntry指向的下一个节点赋值到iter->nextEntry中，继续迭代查看下一个bucket的内容
        if (iter->entry) {
            /* We need to save the 'next' here, the iterator user
             * may delete the entry we are returning. */
            iter->nextEntry = iter->entry->next;
            return iter->entry;
        }
    }
    return NULL;
}

// 释放一个迭代器
void dictReleaseIterator(dictIterator *iter)
{
    // 如果迭代器进行过了迭代(注意前面的叹号)，如果是安全模式，则在释放迭代器时减少该计数；否则对比迭代前后的字典的指纹，不一致则触发断言
    if (!(iter->index == -1 && iter->table == 0)) {
        if (iter->safe)
            iter->d->iterators--;
        // 非安全模式校验迭代前后是否对字典进行了修改，如果执行了修改则产生断言
        else
            assert(iter->fingerprint == dictFingerprint(iter->d));
    }
    //释放迭代器
    zfree(iter);
}

/* Return a random entry from the hash table. Useful to
 * implement randomized algorithms */
// 从字典中随机选出一个哈希表上的bucket上的dictEntry，该dictEntry其实是位于该bucket上的链表首节点
dictEntry *dictGetRandomKey(dict *d)
{
    dictEntry *he, *orighe;
    unsigned long h;
    int listlen, listele;
    
    // 如果字典中的dictEntry为0，直接返回
    if (dictSize(d) == 0) return NULL;
    // 如果正在进行rehash，则执行一次增量rehash
    if (dictIsRehashing(d)) _dictRehashStep(d);
    // 如果正在进行rehash
    if (dictIsRehashing(d)) {
        do {
            /* We are sure there are no elements in indexes from 0
             * to rehashidx-1 */
            // rehash到的节点d->rehashidx之前是没有任何元素的，有数据的总长度为(d->ht[0].size + d->ht[1].size - d->rehashidx)
            h = d->rehashidx + (random() % (d->ht[0].size +
                                            d->ht[1].size -
                                            d->rehashidx));
            he = (h >= d->ht[0].size) ? d->ht[1].table[h - d->ht[0].size] :
                                      d->ht[0].table[h];
        } while(he == NULL);
    //如果没有进行rehash
    } else {
        do {
            // 根据随机数获取一个随机的bucket索引
            h = random() & d->ht[0].sizemask;
            // 根据随机的bucket索引获取dictExtry链表
            he = d->ht[0].table[h];
        } while(he == NULL);
    }

    /* Now we found a non empty bucket, but it is a linked
     * list and we need to get a random element from the list.
     * The only sane way to do so is counting the elements and
     * select a random index. */
    // 记录获取的dictEntry链表长度
    listlen = 0;
    // 原dictEntry链表首节点
    orighe = he;
    // 获取链表长度
    while(he) {
        he = he->next;
        listlen++;
    }
    // 根据随机数获取链表中的随机节点的位置
    listele = random() % listlen;
    // 从首部遍历，直到找到计算出的随机位置上的节点
    he = orighe;
    while(listele--) he = he->next;
    return he;
}

/* This function samples the dictionary to return a few keys from random
 * locations.
 *
 * It does not guarantee to return all the keys specified in 'count', nor
 * it does guarantee to return non-duplicated elements, however it will make
 * some effort to do both things.
 *
 * Returned pointers to hash table entries are stored into 'des' that
 * points to an array of dictEntry pointers. The array must have room for
 * at least 'count' elements, that is the argument we pass to the function
 * to tell how many random elements we need.
 *
 * The function returns the number of items stored into 'des', that may
 * be less than 'count' if the hash table has less than 'count' elements
 * inside, or if not enough elements were found in a reasonable amount of
 * steps.
 *
 * Note that this function is not suitable when you need a good distribution
 * of the returned items, but only when you need to "sample" a given number
 * of continuous elements to run some kind of algorithm or to produce
 * statistics. However the function is much faster than dictGetRandomKey()
 * at producing N elements. */
// 从字典中随机挑选dictEntry，保存到des数组中。最终返回实际获取到的元素的个数
unsigned int dictGetSomeKeys(dict *d, dictEntry **des, unsigned int count) {
    unsigned long j; /* internal hash table id, 0 or 1. */
    unsigned long tables; /* 1 or 2 tables? */
    unsigned long stored = 0, maxsizemask;
    unsigned long maxsteps;

    // 如果字典中的dictEntry为0，直接返回
    if (dictSize(d) < count) count = dictSize(d);
    // 控制了遍历的次数，防止在无法获取到足够的元素(比如哈希表中的元素本身就很少的情况)时陷入死循环
    maxsteps = count*10;

    /* Try to do a rehashing work proportional to 'count'. */
    // 如果正在rehash，则执行count次增量哈希
    for (j = 0; j < count; j++) {
        if (dictIsRehashing(d))
            _dictRehashStep(d);
        else
            break;
    }

    // 如果正在进行rehash，则使用第二个哈希表
    tables = dictIsRehashing(d) ? 2 : 1;
    maxsizemask = d->ht[0].sizemask;
    // 后续会使用该值随机获取两个哈希表中的bucket索引，因此在两个哈希表中选择最小的sizemask
    if (tables > 1 && maxsizemask < d->ht[1].sizemask)
        maxsizemask = d->ht[1].sizemask;

    /* Pick a random point inside the larger table. */
    // 获取一个随机的bucket索引
    unsigned long i = random() & maxsizemask;
    unsigned long emptylen = 0; /* Continuous empty entries so far. */
    // 遍历逻辑是通过bucket索引在两个哈希表中进行遍历，因此需要保证bucket索引不能大于任何一个哈希表的最大索引
    while(stored < count && maxsteps--) {
        // 如果正在进行rehash，则可以从两个表中挑选，否则仅从第一个表中挑选
        for (j = 0; j < tables; j++) {
            /* Invariant of the dict.c rehashing: up to the indexes already
             * visited in ht[0] during the rehashing, there are no populated
             * buckets, so we can skip ht[0] for indexes between 0 and idx-1. */
            // 如果还在进行rehash且当前正在遍历哈希表1，且当前的bucket索引小于rehashidx，需要调整bucket索引大小，防止遍历哈希表二时越界
            if (tables == 2 && j == 0 && i < (unsigned long) d->rehashidx) {
                /* Moreover, if we are currently out of range in the second
                 * table, there will be no elements in both tables up to
                 * the current rehashing index, so we jump if possible.
                 * (this happens when going from big to small table). */
                if (i >= d->ht[1].size)
                    i = d->rehashidx;
                else
                    continue;
            }
            // 如果已经遍历完一个表，则遍历下一个表，或直接退出
            if (i >= d->ht[j].size) continue; /* Out of range for this table. */
            // 获取第j个表的第i个bucket中的dictEntry链表
            dictEntry *he = d->ht[j].table[i];

            /* Count contiguous empty buckets, and jump to other
             * locations if they reach 'count' (with a minimum of 5). */
            // 如果获取到的dictEntry链表为空，且遍历过程中发现连续的空bucket(大于5个)，则随机挑选另外一个bucket索引
            if (he == NULL) {
                emptylen++;
                if (emptylen >= 5 && emptylen > count) {
                    i = random() & maxsizemask;
                    emptylen = 0;
                }
            // 如果获取到的bucket中的链表非空，则将该链表中的元素放到des数组中，当数组中保存的元素数目等于count则直接返回，否则继续遍历
            } else {
                emptylen = 0;
                while (he) {
                    /* Collect all the elements of the buckets found non
                     * empty while iterating. */
                    *des = he;
                    des++;
                    he = he->next;
                    stored++;
                    if (stored == count) return stored;
                }
            }
        }
        // 如果，无法获取到足够的元素，则获取下一个bucket 索引，并在哈希表中查找
        i = (i+1) & maxsizemask;
    }
    return stored;
}

/* This is like dictGetRandomKey() from the POV of the API, but will do more
 * work to ensure a better distribution of the returned element.
 *
 * This function improves the distribution because the dictGetRandomKey()
 * problem is that it selects a random bucket, then it selects a random
 * element from the chain in the bucket. However elements being in different
 * chain lengths will have different probabilities of being reported. With
 * this function instead what we do is to consider a "linear" range of the table
 * that may be constituted of N buckets with chains of different lengths
 * appearing one after the other. Then we report a random element in the range.
 * In this way we smooth away the problem of different chain lenghts. */
#define GETFAIR_NUM_ENTRIES 15
// 该函数相比dictGetRandomKey获取的数据更加分散。
dictEntry *dictGetFairRandomKey(dict *d) {
    dictEntry *entries[GETFAIR_NUM_ENTRIES];
    unsigned int count = dictGetSomeKeys(d,entries,GETFAIR_NUM_ENTRIES);
    /* Note that dictGetSomeKeys() may return zero elements in an unlucky
     * run() even if there are actually elements inside the hash table. So
     * when we get zero, we call the true dictGetRandomKey() that will always
     * yeld the element if the hash table has at least one. */
    // 如果调用dictGetSomeKeys没有获取到任何元素，则退化为dictGetRandomKey
    if (count == 0) return dictGetRandomKey(d);
    unsigned int idx = rand() % count;
    // 如果获取到元素，则从获取到的元素中随机返回一个
    return entries[idx];
}

/* Function to reverse bits. Algorithm from:
 * http://graphics.stanford.edu/~seander/bithacks.html#ReverseParallel */
 // 位反转算法，给dictScan函数使用
static unsigned long rev(unsigned long v) {
    unsigned long s = 8 * sizeof(v); // bit size; must be power of 2
    unsigned long mask = ~0;
    while ((s >>= 1) > 0) {
        mask ^= (mask << s);
        v = ((v >> s) & mask) | ((v << s) & ~mask);
    }
    return v;
}

/* dictScan() is used to iterate over the elements of a dictionary.
 *
 * Iterating works the following way:
 *
 * 1) Initially you call the function using a cursor (v) value of 0.
 * 2) The function performs one step of the iteration, and returns the
 *    new cursor value you must use in the next call.
 * 3) When the returned cursor is 0, the iteration is complete.
 *
 * The function guarantees all elements present in the
 * dictionary get returned between the start and end of the iteration.
 * However it is possible some elements get returned multiple times.
 *
 * For every element returned, the callback argument 'fn' is
 * called with 'privdata' as first argument and the dictionary entry
 * 'de' as second argument.
 *
 * HOW IT WORKS.
 *
 * The iteration algorithm was designed by Pieter Noordhuis.
 * The main idea is to increment a cursor starting from the higher order
 * bits. That is, instead of incrementing the cursor normally, the bits
 * of the cursor are reversed, then the cursor is incremented, and finally
 * the bits are reversed again.
 *
 * This strategy is needed because the hash table may be resized between
 * iteration calls.
 *
 * dict.c hash tables are always power of two in size, and they
 * use chaining, so the position of an element in a given table is given
 * by computing the bitwise AND between Hash(key) and SIZE-1
 * (where SIZE-1 is always the mask that is equivalent to taking the rest
 *  of the division between the Hash of the key and SIZE).
 *
 * For example if the current hash table size is 16, the mask is
 * (in binary) 1111. The position of a key in the hash table will always be
 * the last four bits of the hash output, and so forth.
 *
 * WHAT HAPPENS IF THE TABLE CHANGES IN SIZE?
 *
 * If the hash table grows, elements can go anywhere in one multiple of
 * the old bucket: for example let's say we already iterated with
 * a 4 bit cursor 1100 (the mask is 1111 because hash table size = 16).
 *
 * If the hash table will be resized to 64 elements, then the new mask will
 * be 111111. The new buckets you obtain by substituting in ??1100
 * with either 0 or 1 can be targeted only by keys we already visited
 * when scanning the bucket 1100 in the smaller hash table.
 *
 * By iterating the higher bits first, because of the inverted counter, the
 * cursor does not need to restart if the table size gets bigger. It will
 * continue iterating using cursors without '1100' at the end, and also
 * without any other combination of the final 4 bits already explored.
 *
 * Similarly when the table size shrinks over time, for example going from
 * 16 to 8, if a combination of the lower three bits (the mask for size 8
 * is 111) were already completely explored, it would not be visited again
 * because we are sure we tried, for example, both 0111 and 1111 (all the
 * variations of the higher bit) so we don't need to test it again.
 *
 * WAIT... YOU HAVE *TWO* TABLES DURING REHASHING!
 *
 * Yes, this is true, but we always iterate the smaller table first, then
 * we test all the expansions of the current cursor into the larger
 * table. For example if the current cursor is 101 and we also have a
 * larger table of size 16, we also test (0)101 and (1)101 inside the larger
 * table. This reduces the problem back to having only one table, where
 * the larger one, if it exists, is just an expansion of the smaller one.
 *
 * LIMITATIONS
 *
 * This iterator is completely stateless, and this is a huge advantage,
 * including no additional memory used.
 *
 * The disadvantages resulting from this design are:
 *
 * 1) It is possible we return elements more than once. However this is usually
 *    easy to deal with in the application level.
 * 2) The iterator must return multiple elements per call, as it needs to always
 *    return all the keys chained in a given bucket, and all the expansions, so
 *    we are sure we don't miss keys moving during rehashing.
 * 3) The reverse cursor is somewhat hard to understand at first, but this
 *    comment is supposed to help.
 */
/* 迭代遍历字典中的元素，v作为哈希表的游标(索引)。本函数算法比较复杂，建议参考：https://blog.csdn.net/gqtcgq/article/details/50533336
   本函数用于保证字典中原有的元素都可以被遍历并尽可能少重复迭代，但有可能迭代到系统的元素 */
unsigned long dictScan(dict *d,
                       unsigned long v,
                       dictScanFunction *fn,
                       dictScanBucketFunction* bucketfn,
                       void *privdata)
{
    dictht *t0, *t1;
    const dictEntry *de, *next;
    unsigned long m0, m1;
    
    // 判断字典中是否有dictEntry，如果没有直接返回
    if (dictSize(d) == 0) return 0;

    /* Having a safe iterator means no rehashing can happen, see _dictRehashStep.
     * This is needed in case the scan callback tries to do dictFind or alike. */
    // 用于防止在迭代过程中出现增量rehash(rehash会进行数据转移)
    d->iterators++;
    
    // 如果没有执行过rehash或rehash已经结束，仅使用第一个哈希表ht[0]
    if (!dictIsRehashing(d)) {
        // 获取第一个哈希表和哈希表bucket掩码
        t0 = &(d->ht[0]);
        m0 = t0->sizemask;

        /* Emit entries at cursor */
        // 调用bucketfn处理bucket
        if (bucketfn) bucketfn(privdata, &t0->table[v & m0]);
        // 获取游标处的dictEntry链表。由于没有对bucket进行判空，因此可能存在处理空的bucket
        de = t0->table[v & m0];
        // 遍历链表并调用fn函数处理每个节点
        while (de) {
            next = de->next;
            // 调用fn处理链表节点
            fn(privdata, de);
            de = next;
        }

        /* Set unmasked bits so incrementing the reversed cursor
         * operates on the masked bits */
        /* 以下的处理逻辑主要是为了在哈希表扩容缩容时保证遍历哈希表中的原有的元素并尽可能少重复迭代
         * 算法主要基于哈希表扩容缩容时，影响的主要是索引对应的二进制数值的高位。比如sizemask为111，当检索到的索引
         * 为110时发生了扩容，此时sizemask变为1111，那么原110对应新的索引为0110或1110，反之亦然。可见扩缩容时受影响的
         * 是高位部分，地位部分不受影响。下面算法中对索引的迭代不是从低位进行叠加进位，而是从高位开始反向叠加进位。最大限度
         * 减少了重复bucket的迭代 */
        /*保留v的低n位数(n为m0的位数)，其余位全置为1，例如：
            +----------------------------+  
            |0|0|0|0|...| a1 |a2 |...|an |转换为
            +----------------------------+
            +----------------------------+
            |1|1|1|1|...| a1 |a2 |...|an |
            +----------------------------+
        */
        v |= ~m0;
        /* Increment the reverse cursor */
        /* 翻转v的比特位
            +------------------------+
            |an|an-1|...|a1|1|1|...|1|
            +------------------------+
        */
        v = rev(v);
        /* 
            +------------------------+
            |an|an-1|...|a1+1|0|...|0|
            +------------------------+
        */
        v++;
        /*
            +--------------+
            |a+1|a2|...|an|
            +--------------+
        */
        v = rev(v);
    // 如果正在进行rehash，需要同时处理位于ht[0]和ht[1]中相同索引的bucket。返回的v位于ht[1]
    } else {
        t0 = &d->ht[0];
        t1 = &d->ht[1];

        /* Make sure t0 is the smaller and t1 is the bigger table */
        if (t0->size > t1->size) {
            t0 = &d->ht[1];
            t1 = &d->ht[0];
        }

        m0 = t0->sizemask;
        m1 = t1->sizemask;

        /* Emit entries at cursor */
        // 处理位于ht[0]的索引为v的bucket中的链表
        if (bucketfn) bucketfn(privdata, &t0->table[v & m0]);
        de = t0->table[v & m0];
        while (de) {
            next = de->next;
            fn(privdata, de);
            de = next;
        }

        /* Iterate over indices in larger table that are the expansion
         * of the index pointed to by the cursor in the smaller table */
        do {
            /* Emit entries at cursor */
            // 处理位于ht[1]的索引为v的bucket中的链表
            if (bucketfn) bucketfn(privdata, &t1->table[v & m1]);
            de = t1->table[v & m1];
            while (de) {
                next = de->next;
                fn(privdata, de);
                de = next;
            }

            /* Increment the reverse cursor not covered by the smaller mask.*/
            v |= ~m1;
            v = rev(v);
            v++;
            v = rev(v);

            /* Continue while bits covered by mask difference is non-zero */
        } while (v & (m0 ^ m1));
    }

    /* undo the ++ at the top */
    d->iterators--;
    // 返回当前迭代的bucket的位置
    return v;
}

/* ------------------------- private functions ------------------------------ */

/* Expand the hash table if needed */
static int _dictExpandIfNeeded(dict *d)
{
    /* Incremental rehashing already in progress. Return. */
    if (dictIsRehashing(d)) return DICT_OK;

    /* If the hash table is empty expand it to the initial size. */
    // 如果哈希表为空，则使用初始值进行初始化
    if (d->ht[0].size == 0) return dictExpand(d, DICT_HT_INITIAL_SIZE);

    /* If we reached the 1:1 ratio, and we are allowed to resize the hash
     * table (global setting) or we should avoid it but the ratio between
     * elements/buckets is over the "safe" threshold, we resize doubling
     * the number of buckets. */
    /* 如果已经有的元素(dictEntry)大于哈希表bucket数，且允许扩大哈希表或已有的dictEntry与bucket的比例大于
       dict_force_resize_ratio才能扩大哈希表 */
    if (d->ht[0].used >= d->ht[0].size &&
        (dict_can_resize ||
         d->ht[0].used/d->ht[0].size > dict_force_resize_ratio))
    {
        // 直接将哈希表扩大为已有元素的两倍
        return dictExpand(d, d->ht[0].used*2);
    }
    return DICT_OK;
}

/* Our hash table capability is a power of two */
// 哈希表大小是2的幂
static unsigned long _dictNextPower(unsigned long size)
{
    unsigned long i = DICT_HT_INITIAL_SIZE;

    if (size >= LONG_MAX) return LONG_MAX + 1LU;
    // 如果需要调整的长度小于初始长度DICT_HT_INITIAL_SIZE，则直接使用初始长度。否则不断以2的幂增加长度：DICT_HT_INITIAL_SIZE*(2^n),直到其大于入参size
    while(1) {
        if (i >= size)
            return i;
        i *= 2;
    }
}

/* Returns the index of a free slot that can be populated with
 * a hash entry for the given 'key'.
 * If the key already exists, -1 is returned
 * and the optional output parameter may be filled.
 *
 * Note that if we are in the process of rehashing the hash table, the
 * index is always returned in the context of the second (new) hash table. */
/* 返回可用bucket(或叫slot)的索引，该bucket可以填充给定“key”的哈希项。如果已经存在相同的key，返回-1，否则返回哈希索引。
   入参hash为根据key计算出的哈希值 */
static long _dictKeyIndex(dict *d, const void *key, uint64_t hash, dictEntry **existing)
{
    unsigned long idx, table;
    dictEntry *he;
    if (existing) *existing = NULL;

    /* Expand the hash table if needed */
    // 在必要时扩展哈希表
    if (_dictExpandIfNeeded(d) == DICT_ERR)
        return -1;
    // 从d->ht[0]
    for (table = 0; table <= 1; table++) {
        // 计算哈希值在哈希表中的bucket索引
        idx = hash & d->ht[table].sizemask;
        /* Search if this slot does not already contain the given key */
        // 获取bucket索引对应的dictEntry链表，并进行遍历，如果入参existing非空，则将查找到的dictEntry作为出参返回出去，并返回-1
        he = d->ht[table].table[idx];
        while(he) {
            if (key==he->key || dictCompareKeys(d, key, he->key)) {
                if (existing) *existing = he;
                return -1;
            }
            he = he->next;
        }
        // 如果无法进行rehash，直接返回
        if (!dictIsRehashing(d)) break;
    }
    // 如果索引idx对应的链表中不存在相同的key，则返回哈希索引
    return idx;
}

// 清空字典，但不释放字典本身的内存
void dictEmpty(dict *d, void(callback)(void*)) {
    _dictClear(d,&d->ht[0],callback);
    _dictClear(d,&d->ht[1],callback);
    d->rehashidx = -1;
    d->iterators = 0;
}

// 启用resize功能，可以扩大哈希表大小
void dictEnableResize(void) {
    dict_can_resize = 1;
}

// 禁用resize功能，无法修改哈希表大小
void dictDisableResize(void) {
    dict_can_resize = 0;
}

// 获取key对应的哈希值
uint64_t dictGetHash(dict *d, const void *key) {
    return dictHashKey(d, key);
}

/* Finds the dictEntry reference by using pointer and pre-calculated hash.
 * oldkey is a dead pointer and should not be accessed.
 * the hash value should be provided using dictGetHash.
 * no string / key comparison is performed.
 * return value is the reference to the dictEntry if found, or NULL if not found. */
// 根据key和哈希值从两个哈希表中找出dictEntry，哈希值用于计算bucket索引
dictEntry **dictFindEntryRefByPtrAndHash(dict *d, const void *oldptr, uint64_t hash) {
    dictEntry *he, **heref;
    unsigned long idx, table;
    // 如果哈希表没有任何dictEntry，则返回
    if (d->ht[0].used + d->ht[1].used == 0) return NULL; /* dict is empty */
    // 尝试在两个哈希表中
    for (table = 0; table <= 1; table++) {
        // 获取哈希值对应的bucket索引
        idx = hash & d->ht[table].sizemask;
        // 获取对应的dictEntry链表
        heref = &d->ht[table].table[idx];
        he = *heref;
        // 遍历链表，找到与ildptr系统的dictEntry，并返回该dictEntry
        while(he) {
            if (oldptr==he->key)
                return heref;
            heref = &he->next;
            he = *heref;
        }
        // 如果rehash结束，则ht[1]为空，直接退出
        if (!dictIsRehashing(d)) return NULL;
    }
    return NULL;
}

/* ------------------------------- Debugging ---------------------------------*/

#define DICT_STATS_VECTLEN 50
size_t _dictGetStatsHt(char *buf, size_t bufsize, dictht *ht, int tableid) {
    unsigned long i, slots = 0, chainlen, maxchainlen = 0;
    unsigned long totchainlen = 0;
    unsigned long clvector[DICT_STATS_VECTLEN];
    size_t l = 0;

    if (ht->used == 0) {
        return snprintf(buf,bufsize,
            "No stats available for empty dictionaries\n");
    }

    /* Compute stats. */
    for (i = 0; i < DICT_STATS_VECTLEN; i++) clvector[i] = 0;
    for (i = 0; i < ht->size; i++) {
        dictEntry *he;

        if (ht->table[i] == NULL) {
            clvector[0]++;
            continue;
        }
        slots++;
        /* For each hash entry on this slot... */
        chainlen = 0;
        he = ht->table[i];
        while(he) {
            chainlen++;
            he = he->next;
        }
        clvector[(chainlen < DICT_STATS_VECTLEN) ? chainlen : (DICT_STATS_VECTLEN-1)]++;
        if (chainlen > maxchainlen) maxchainlen = chainlen;
        totchainlen += chainlen;
    }

    /* Generate human readable stats. */
    l += snprintf(buf+l,bufsize-l,
        "Hash table %d stats (%s):\n"
        " table size: %ld\n"
        " number of elements: %ld\n"
        " different slots: %ld\n"
        " max chain length: %ld\n"
        " avg chain length (counted): %.02f\n"
        " avg chain length (computed): %.02f\n"
        " Chain length distribution:\n",
        tableid, (tableid == 0) ? "main hash table" : "rehashing target",
        ht->size, ht->used, slots, maxchainlen,
        (float)totchainlen/slots, (float)ht->used/slots);

    for (i = 0; i < DICT_STATS_VECTLEN-1; i++) {
        if (clvector[i] == 0) continue;
        if (l >= bufsize) break;
        l += snprintf(buf+l,bufsize-l,
            "   %s%ld: %ld (%.02f%%)\n",
            (i == DICT_STATS_VECTLEN-1)?">= ":"",
            i, clvector[i], ((float)clvector[i]/ht->size)*100);
    }

    /* Unlike snprintf(), teturn the number of characters actually written. */
    if (bufsize) buf[bufsize-1] = '\0';
    return strlen(buf);
}

void dictGetStats(char *buf, size_t bufsize, dict *d) {
    size_t l;
    char *orig_buf = buf;
    size_t orig_bufsize = bufsize;

    l = _dictGetStatsHt(buf,bufsize,&d->ht[0],0);
    buf += l;
    bufsize -= l;
    if (dictIsRehashing(d) && bufsize > 0) {
        _dictGetStatsHt(buf,bufsize,&d->ht[1],1);
    }
    /* Make sure there is a NULL term at the end. */
    if (orig_bufsize) orig_buf[orig_bufsize-1] = '\0';
}

/* ------------------------------- Benchmark ---------------------------------*/

#ifdef DICT_BENCHMARK_MAIN

#include "sds.h"

uint64_t hashCallback(const void *key) {
    return dictGenHashFunction((unsigned char*)key, sdslen((char*)key));
}

int compareCallback(void *privdata, const void *key1, const void *key2) {
    int l1,l2;
    DICT_NOTUSED(privdata);

    l1 = sdslen((sds)key1);
    l2 = sdslen((sds)key2);
    if (l1 != l2) return 0;
    return memcmp(key1, key2, l1) == 0;
}

void freeCallback(void *privdata, void *val) {
    DICT_NOTUSED(privdata);

    sdsfree(val);
}

dictType BenchmarkDictType = {
    hashCallback,
    NULL,
    NULL,
    compareCallback,
    freeCallback,
    NULL
};

#define start_benchmark() start = timeInMilliseconds()
#define end_benchmark(msg) do { \
    elapsed = timeInMilliseconds()-start; \
    printf(msg ": %ld items in %lld ms\n", count, elapsed); \
} while(0);

/* dict-benchmark [count] */
int main(int argc, char **argv) {
    long j;
    long long start, elapsed;
    dict *dict = dictCreate(&BenchmarkDictType,NULL);
    long count = 0;

    if (argc == 2) {
        count = strtol(argv[1],NULL,10);
    } else {
        count = 5000000;
    }

    start_benchmark();
    for (j = 0; j < count; j++) {
        int retval = dictAdd(dict,sdsfromlonglong(j),(void*)j);
        assert(retval == DICT_OK);
    }
    end_benchmark("Inserting");
    assert((long)dictSize(dict) == count);

    /* Wait for rehashing. */
    while (dictIsRehashing(dict)) {
        dictRehashMilliseconds(dict,100);
    }

    start_benchmark();
    for (j = 0; j < count; j++) {
        sds key = sdsfromlonglong(j);
        dictEntry *de = dictFind(dict,key);
        assert(de != NULL);
        sdsfree(key);
    }
    end_benchmark("Linear access of existing elements");

    start_benchmark();
    for (j = 0; j < count; j++) {
        sds key = sdsfromlonglong(j);
        dictEntry *de = dictFind(dict,key);
        assert(de != NULL);
        sdsfree(key);
    }
    end_benchmark("Linear access of existing elements (2nd round)");

    start_benchmark();
    for (j = 0; j < count; j++) {
        sds key = sdsfromlonglong(rand() % count);
        dictEntry *de = dictFind(dict,key);
        assert(de != NULL);
        sdsfree(key);
    }
    end_benchmark("Random access of existing elements");

    start_benchmark();
    for (j = 0; j < count; j++) {
        sds key = sdsfromlonglong(rand() % count);
        key[0] = 'X';
        dictEntry *de = dictFind(dict,key);
        assert(de == NULL);
        sdsfree(key);
    }
    end_benchmark("Accessing missing");

    start_benchmark();
    for (j = 0; j < count; j++) {
        sds key = sdsfromlonglong(j);
        int retval = dictDelete(dict,key);
        assert(retval == DICT_OK);
        key[0] += 17; /* Change first number to letter. */
        retval = dictAdd(dict,key,(void*)j);
        assert(retval == DICT_OK);
    }
    end_benchmark("Removing and adding");
}
#endif
