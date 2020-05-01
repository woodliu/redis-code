/* The ziplist is a specially encoded dually linked list that is designed
 * to be very memory efficient. It stores both strings and integer values,
 * where integers are encoded as actual integers instead of a series of
 * characters. It allows push and pop operations on either side of the list
 * in O(1) time. However, because every operation requires a reallocation of
 * the memory used by the ziplist, the actual complexity is related to the
 * amount of memory used by the ziplist.
 *
 * ----------------------------------------------------------------------------
 *
 * ZIPLIST OVERALL LAYOUT
 * ======================
 *
 * The general layout of the ziplist is as follows:
 *
 * <zlbytes> <zltail> <zllen> <entry> <entry> ... <entry> <zlend>
 *
 * NOTE: all fields are stored in little endian, if not specified otherwise.
 *
 * <uint32_t zlbytes> is an unsigned integer to hold the number of bytes that
 * the ziplist occupies, including the four bytes of the zlbytes field itself.
 * This value needs to be stored to be able to resize the entire structure
 * without the need to traverse it first.
 *
 * <uint32_t zltail> is the offset to the last entry in the list. This allows
 * a pop operation on the far side of the list without the need for full
 * traversal.
 *
 * <uint16_t zllen> is the number of entries. When there are more than
 * 2^16-2 entries, this value is set to 2^16-1 and we need to traverse the
 * entire list to know how many items it holds.
 *
 * <uint8_t zlend> is a special entry representing the end of the ziplist.
 * Is encoded as a single byte equal to 255. No other normal entry starts
 * with a byte set to the value of 255.
 *
 * ZIPLIST ENTRIES
 * ===============
 *
 * Every entry in the ziplist is prefixed by metadata that contains two pieces
 * of information. First, the length of the previous entry is stored to be
 * able to traverse the list from back to front. Second, the entry encoding is
 * provided. It represents the entry type, integer or string, and in the case
 * of strings it also represents the length of the string payload.
 * So a complete entry is stored like this:
 *
 * <prevlen> <encoding> <entry-data>
 *
 * Sometimes the encoding represents the entry itself, like for small integers
 * as we'll see later. In such a case the <entry-data> part is missing, and we
 * could have just:
 *
 * <prevlen> <encoding>
 *
 * The length of the previous entry, <prevlen>, is encoded in the following way:
 * If this length is smaller than 254 bytes, it will only consume a single
 * byte representing the length as an unsinged 8 bit integer. When the length
 * is greater than or equal to 254, it will consume 5 bytes. The first byte is
 * set to 254 (FE) to indicate a larger value is following. The remaining 4
 * bytes take the length of the previous entry as value.
 *
 * So practically an entry is encoded in the following way:
 *
 * <prevlen from 0 to 253> <encoding> <entry>
 *
 * Or alternatively if the previous entry length is greater than 253 bytes
 * the following encoding is used:
 *
 * 0xFE <4 bytes unsigned little endian prevlen> <encoding> <entry>
 *
 * The encoding field of the entry depends on the content of the
 * entry. When the entry is a string, the first 2 bits of the encoding first
 * byte will hold the type of encoding used to store the length of the string,
 * followed by the actual length of the string. When the entry is an integer
 * the first 2 bits are both set to 1. The following 2 bits are used to specify
 * what kind of integer will be stored after this header. An overview of the
 * different types and encodings is as follows. The first byte is always enough
 * to determine the kind of entry.
 *
 * |00pppppp| - 1 byte
 *      String value with length less than or equal to 63 bytes (6 bits).
 *      "pppppp" represents the unsigned 6 bit length.
 * |01pppppp|qqqqqqqq| - 2 bytes
 *      String value with length less than or equal to 16383 bytes (14 bits).
 *      IMPORTANT: The 14 bit number is stored in big endian.
 * |10000000|qqqqqqqq|rrrrrrrr|ssssssss|tttttttt| - 5 bytes
 *      String value with length greater than or equal to 16384 bytes.
 *      Only the 4 bytes following the first byte represents the length
 *      up to 32^2-1. The 6 lower bits of the first byte are not used and
 *      are set to zero.
 *      IMPORTANT: The 32 bit number is stored in big endian.
 * |11000000| - 3 bytes
 *      Integer encoded as int16_t (2 bytes).
 * |11010000| - 5 bytes
 *      Integer encoded as int32_t (4 bytes).
 * |11100000| - 9 bytes
 *      Integer encoded as int64_t (8 bytes).
 * |11110000| - 4 bytes
 *      Integer encoded as 24 bit signed (3 bytes).
 * |11111110| - 2 bytes
 *      Integer encoded as 8 bit signed (1 byte).
 * |1111xxxx| - (with xxxx between 0000 and 1101) immediate 4 bit integer.
 *      Unsigned integer from 0 to 12. The encoded value is actually from
 *      1 to 13 because 0000 and 1111 can not be used, so 1 should be
 *      subtracted from the encoded 4 bit value to obtain the right value.
 * |11111111| - End of ziplist special entry.
 *
 * Like for the ziplist header, all the integers are represented in little
 * endian byte order, even when this code is compiled in big endian systems.
 *
 * EXAMPLES OF ACTUAL ZIPLISTS
 * ===========================
 *
 * The following is a ziplist containing the two elements representing
 * the strings "2" and "5". It is composed of 15 bytes, that we visually
 * split into sections:
 *
 *  [0f 00 00 00] [0c 00 00 00] [02 00] [00 f3] [02 f6] [ff]
 *        |             |          |       |       |     |
 *     zlbytes        zltail    entries   "2"     "5"   end
 *
 * The first 4 bytes represent the number 15, that is the number of bytes
 * the whole ziplist is composed of. The second 4 bytes are the offset
 * at which the last ziplist entry is found, that is 12, in fact the
 * last entry, that is "5", is at offset 12 inside the ziplist.
 * The next 16 bit integer represents the number of elements inside the
 * ziplist, its value is 2 since there are just two elements inside.
 * Finally "00 f3" is the first entry representing the number 2. It is
 * composed of the previous entry length, which is zero because this is
 * our first entry, and the byte F3 which corresponds to the encoding
 * |1111xxxx| with xxxx between 0001 and 1101. We need to remove the "F"
 * higher order bits 1111, and subtract 1 from the "3", so the entry value
 * is "2". The next entry has a prevlen of 02, since the first entry is
 * composed of exactly two bytes. The entry itself, F6, is encoded exactly
 * like the first entry, and 6-1 = 5, so the value of the entry is 5.
 * Finally the special entry FF signals the end of the ziplist.
 *
 * Adding another element to the above string with the value "Hello World"
 * allows us to show how the ziplist encodes small strings. We'll just show
 * the hex dump of the entry itself. Imagine the bytes as following the
 * entry that stores "5" in the ziplist above:
 *
 * [02] [0b] [48 65 6c 6c 6f 20 57 6f 72 6c 64]
 *
 * The first byte, 02, is the length of the previous entry. The next
 * byte represents the encoding in the pattern |00pppppp| that means
 * that the entry is a string of length <pppppp>, so 0B means that
 * an 11 bytes string follows. From the third byte (48) to the last (64)
 * there are just the ASCII characters for "Hello World".
 *
 * ----------------------------------------------------------------------------
 *
 * Copyright (c) 2009-2012, Pieter Noordhuis <pcnoordhuis at gmail dot com>
 * Copyright (c) 2009-2017, Salvatore Sanfilippo <antirez at gmail dot com>
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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <limits.h>
#include "zmalloc.h"
#include "util.h"
#include "ziplist.h"
#include "endianconv.h"
#include "redisassert.h"

// ziplist的结束entry，即FF
#define ZIP_END 255         /* Special "end of ziplist" entry. */
// 254作为prevlen使用的一个分界线。当前一个entry的长度小于254，prevlen为1字节；否则为5字节，此时第一个字节固定为FE，真实长度
#define ZIP_BIG_PREVLEN 254 /* Max number of bytes of the previous entry, for
                               the "prevlen" field prefixing each entry, to be
                               represented with just a single byte. Otherwise
                               it is represented as FF AA BB CC DD, where
                               AA BB CC DD are a 4 bytes unsigned integer
                               representing the previous entry len. */

/* Different encoding/length possibilities */
// 获取encoding第一个字节的前2bit，字符串使用前2bit保存编码类型
#define ZIP_STR_MASK 0xc0
// 获取encoding第一个字节的第3bit和第4bit，整数使用这2bit保存编码类型
#define ZIP_INT_MASK 0x30
// 对应|00pppppp|，即字符串长度小于63字节
#define ZIP_STR_06B (0 << 6)
// |01pppppp|qqqqqqqq|，即字符串长度大于63字节且小于16383字节
#define ZIP_STR_14B (1 << 6)
// |10000000|qqqqqqqq|rrrrrrrr|ssssssss|tttttttt|，即字符串长度大于63字节且大于16383字节且小于32^2-1字节
#define ZIP_STR_32B (2 << 6)
// |11000000|，占2字节
#define ZIP_INT_16B (0xc0 | 0<<4)
// |11010000|，占4字节
#define ZIP_INT_32B (0xc0 | 1<<4)
// |11100000|，占8字节
#define ZIP_INT_64B (0xc0 | 2<<4)
// |11110000|，占3字节
#define ZIP_INT_24B (0xc0 | 3<<4)
// |11111110|，占1字节
#define ZIP_INT_8B 0xfe

/* 4 bit integer immediate encoding |1111xxxx| with xxxx between
 * 0001 and 1101. */
#define ZIP_INT_IMM_MASK 0x0f   /* Mask to extract the 4 bits value. To add
                                   one is needed to reconstruct the value. */
#define ZIP_INT_IMM_MIN 0xf1    /* 11110001，表示整数1 */
#define ZIP_INT_IMM_MAX 0xfd    /* 11111101 ，表示整数13 */

#define INT24_MAX 0x7fffff
#define INT24_MIN (-INT24_MAX - 1)

/* Macro to determine if the entry is a string. String entries never start
 * with "11" as most significant bits of the first byte. */
// 通过编码字节的前2bit判断是否是字符串，字符串的前2bit小于二进制"11"
#define ZIP_IS_STR(enc) (((enc) & ZIP_STR_MASK) < ZIP_STR_MASK)

/* Utility macros.*/

/* Return total bytes a ziplist is composed of. */
// 获取ziplist的字节长度，即获取ziplist首部的第一个元素zlbytes的值
#define ZIPLIST_BYTES(zl)       (*((uint32_t*)(zl)))

/* Return the offset of the last item inside the ziplist. */
// 偏移到zltail的地址，用于获取zltail的值
#define ZIPLIST_TAIL_OFFSET(zl) (*((uint32_t*)((zl)+sizeof(uint32_t))))

/* Return the length of a ziplist, or UINT16_MAX if the length cannot be
 * determined without scanning the whole ziplist. */
// 偏移到zllen的地址，用于获取zllen的值
#define ZIPLIST_LENGTH(zl)      (*((uint16_t*)((zl)+sizeof(uint32_t)*2)))

/* The size of a ziplist header: two 32 bit integers for the total
 * bytes count and last item offset. One 16 bit integer for the number
 * of items field. */
// ziplist首部长度。zlbytes + zltail + zllen的大小
#define ZIPLIST_HEADER_SIZE     (sizeof(uint32_t)*2+sizeof(uint16_t))

/* Size of the "end of ziplist" entry. Just one byte. */
// zlend大小，即"FF"
#define ZIPLIST_END_SIZE        (sizeof(uint8_t))

/* Return the pointer to the first entry of a ziplist. */
// 获取ziplist中的第一个entry的地址
#define ZIPLIST_ENTRY_HEAD(zl)  ((zl)+ZIPLIST_HEADER_SIZE)

/* Return the pointer to the last entry of a ziplist, using the
 * last entry offset inside the ziplist header. */
// 找出ziplist的最后一个entry的地址，非zlend
#define ZIPLIST_ENTRY_TAIL(zl)  ((zl)+intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl)))

/* Return the pointer to the last byte of a ziplist, which is, the
 * end of ziplist FF entry. */
#define ZIPLIST_ENTRY_END(zl)   ((zl)+intrev32ifbe(ZIPLIST_BYTES(zl))-1)

/* Increment the number of items field in the ziplist header. Note that this
 * macro should never overflow the unsigned 16 bit integer, since entries are
 * always pushed one at a time. When UINT16_MAX is reached we want the count
 * to stay there to signal that a full scan is needed to get the number of
 * items inside the ziplist. */
#define ZIPLIST_INCR_LENGTH(zl,incr) { \
    if (ZIPLIST_LENGTH(zl) < UINT16_MAX) \
        ZIPLIST_LENGTH(zl) = intrev16ifbe(intrev16ifbe(ZIPLIST_LENGTH(zl))+incr); \
}

/* We use this function to receive information about a ziplist entry.
 * Note that this is not how the data is actually encoded, is just what we
 * get filled by a function in order to operate more easily. */
typedef struct zlentry {
    unsigned int prevrawlensize; /* prevlen字段占用的字节数*/
    unsigned int prevrawlen;     /* 前一个entry的长度，保存在prevlen中. */
    unsigned int lensize;        /* Bytes used to encode this entry type/len.
                                    For example strings have a 1, 2 or 5 bytes
                                    header. Integers always use a single byte.表示encoding字段占用的字节数*/
    unsigned int len;            /* Bytes used to represent the actual entry.
                                    For strings this is just the string length
                                    while for integers it is 1, 2, 3, 4, 8 or
                                    0 (for 4 bit immediate) depending on the
                                    number range. entry-data的长度*/
    unsigned int headersize;     /* prevrawlensize + lensize. 即prevlen+encoding字段所占的字节数*/
    unsigned char encoding;      /* Set to ZIP_STR_* or ZIP_INT_* depending on
                                    the entry encoding. However for 4 bits
                                    immediate integers this can assume a range
                                    of values and must be range-checked.encoding字段的值 */
    unsigned char *p;            /* Pointer to the very start of the entry, that
                                    is, this points to prev-entry-len field. 指向entry，即prevlen的地址*/
} zlentry;

#define ZIPLIST_ENTRY_ZERO(zle) { \
    (zle)->prevrawlensize = (zle)->prevrawlen = 0; \
    (zle)->lensize = (zle)->len = (zle)->headersize = 0; \
    (zle)->encoding = 0; \
    (zle)->p = NULL; \
}

/* Extract the encoding from the byte pointed by 'ptr' and set it into
 * 'encoding' field of the zlentry structure. */
// 获取encoding字段值，保存到encoding中。ptr指向ziplist的encoding的起始地址，encoding变量保存了ziplist的encoding的第一个字节
#define ZIP_ENTRY_ENCODING(ptr, encoding) do {  \
    (encoding) = (ptr[0]); \
    if ((encoding) < ZIP_STR_MASK) (encoding) &= ZIP_STR_MASK; /* 字符串编码变量(如ZIP_STR_14B)进保存了低6位，因此如果是字符串编码，需要补充前2bit信息；如果encoding > ZIP_STR_MASK则表示是一个整数编码，直接返回即可 */ \
} while(0)

/* Return bytes needed to store integer encoded by 'encoding'. */
// 计算整数编码的entry占用的字节数
unsigned int zipIntSize(unsigned char encoding) {
    switch(encoding) {
    case ZIP_INT_8B:  return 1;
    case ZIP_INT_24B: return 3;
    case ZIP_INT_16B: return 2;
    case ZIP_INT_32B: return 4;
    case ZIP_INT_64B: return 8;
    }
    // 1~13的整数编码到encoding的1个字节中，因此没有单独占用字节
    if (encoding >= ZIP_INT_IMM_MIN && encoding <= ZIP_INT_IMM_MAX)
        return 0; /* 4 bit immediate */
    panic("Invalid integer encoding 0x%02X", encoding);
    return 0;
}

/* Write the encoidng header of the entry in 'p'. If p is NULL it just returns
 * the amount of bytes required to encode such a length. Arguments:
 *
 * 'encoding' is the encoding we are using for the entry. It could be
 * ZIP_INT_* or ZIP_STR_* or between ZIP_INT_IMM_MIN and ZIP_INT_IMM_MAX
 * for single-byte small immediate integers.
 *
 * 'rawlen' is only used for ZIP_STR_* encodings and is the length of the
 * srting that this entry represents.
 *
 * The function returns the number of bytes used by the encoding/length
 * header stored in 'p'. */
/* 填充entry p的encoding字段，并返回encoding的字节长度，如果p为NULL，则直接返回字符串长度。encoding的取值为ZIP_STR_06B，ZIP_INT_16B等。
   rawlen仅用于字符串编码，表示字符串的长度。
   主要用于填充字符串编码，整数编码直接赋值即可*/
unsigned int zipStoreEntryEncoding(unsigned char *p, unsigned char encoding, unsigned int rawlen) {
    unsigned char len = 1, buf[5];
    // 如果是字符串编码
    if (ZIP_IS_STR(encoding)) {
        /* Although encoding is given it may not be set for strings,
         * so we determine it here using the raw length. */
        // 如果字符串长度小于3f，即63字节，
        if (rawlen <= 0x3f) {
            // 如果p为空，直接返回字符串长度
            if (!p) return len;
            // 使用|00pppppp|编码
            buf[0] = ZIP_STR_06B | rawlen;
        // 字符串大于63字节，且小于等于16383字节
        } else if (rawlen <= 0x3fff) {
            // encoding额外占用1字节
            len += 1;
            if (!p) return len;
            // 使用|01pppppp|qqqqqqqq|编码
            buf[0] = ZIP_STR_14B | ((rawlen >> 8) & 0x3f);
            buf[1] = rawlen & 0xff;
        // 字符串大于16383字节
        } else {
             // encoding额外占用4字节
            len += 4;
            if (!p) return len;
            // 使用|10000000|qqqqqqqq|rrrrrrrr|ssssssss|tttttttt|编码
            buf[0] = ZIP_STR_32B;
            buf[1] = (rawlen >> 24) & 0xff;
            buf[2] = (rawlen >> 16) & 0xff;
            buf[3] = (rawlen >> 8) & 0xff;
            buf[4] = rawlen & 0xff;
        }
    // 如果是整数编码，直接将编码赋值即可
    } else {
        /* Implies integer encoding, so length is always 1. */
        if (!p) return len;
        buf[0] = encoding;
    }

    /* Store this length at p. */
    // 填充p的encoding字段
    memcpy(p,buf,len);
    return len;
}

/* Decode the entry encoding type and data length (string length for strings,
 * number of bytes used for the integer for integer entries) encoded in 'ptr'.
 * The 'encoding' variable will hold the entry encoding, the 'lensize'
 * variable will hold the number of bytes required to encode the entry
 * length, and the 'len' variable will hold the entry length. */
// 获取encoding字段的值，encoding所占的字节数，以及entry-data的长度，分别保存到encoding, lensize, len中，ptr指向encoding字段。
#define ZIP_DECODE_LENGTH(ptr, encoding, lensize, len) do {                    \
    ZIP_ENTRY_ENCODING((ptr), (encoding)); /* encoding保存了ziplist的encoding的第一个字节 */ \
    if ((encoding) < ZIP_STR_MASK) {    /* |00pppppp| 编码，长度保存在低6位*/    \
        if ((encoding) == ZIP_STR_06B) {                                       \
            (lensize) = 1;                                                     \
            (len) = (ptr)[0] & 0x3f;                                           \
        } else if ((encoding) == ZIP_STR_14B) {                                \
            (lensize) = 2;                                                     \
            (len) = (((ptr)[0] & 0x3f) << 8) | (ptr)[1]; /* |01pppppp|qqqqqqqq| 编码，拼接第一个字节的低6位和第二个字节作为字符串长度*/ \
        } else if ((encoding) == ZIP_STR_32B) {                                \
            (lensize) = 5;  /* 拼接后4个字节作为字符串长度 */                    \
            (len) = ((ptr)[1] << 24) |                                         \
                    ((ptr)[2] << 16) |                                         \
                    ((ptr)[3] <<  8) |                                         \
                    ((ptr)[4]);                                                \
        } else {                                                               \
            panic("Invalid string encoding 0x%02X", (encoding));               \
        }                                                                      \
    } else {    /* 整数编码 */                                                  \
        (lensize) = 1;                                                         \
        (len) = zipIntSize(encoding); /* 计算不同编码的整数占用的字节 */          \
    }                                                                          \
} while(0);

/* Encode the length of the previous entry and write it to "p". This only
 * uses the larger encoding (required in __ziplistCascadeUpdate). */
// 将前一个entry的长度写入prevlan，p指向entry的prevlen起始地址。仅用于前一个entry大于等于ZIP_BIG_PREVLEN的场景
int zipStorePrevEntryLengthLarge(unsigned char *p, unsigned int len) {
    if (p != NULL) {
        // prevlan的第一个字节固定为ZIP_BIG_PREVLEN，即0xFE
        p[0] = ZIP_BIG_PREVLEN;
        // 将实际长度放到prevlen的第二个字节开始的内存中
        memcpy(p+1,&len,sizeof(len));
        memrev32ifbe(p+1);
    }
    //返回prevlen占用的字节数
    return 1+sizeof(len);
}

/* Encode the length of the previous entry and write it to "p". Return the
 * number of bytes needed to encode this length if "p" is NULL. */
//  根据长度len设置p中prevlen的数值，并返回prevlen字段占用的字节数。如果p为NULL，则仅返回保存len的prevlen字段占用的字节数
unsigned int zipStorePrevEntryLength(unsigned char *p, unsigned int len) {
    if (p == NULL) {
        return (len < ZIP_BIG_PREVLEN) ? 1 : sizeof(len)+1;
    } else {
        // 如果前一个entry的长度小于ZIP_BIG_PREVLEN，则prevlen仅占一个字节，直接写入第一个字节即可
        if (len < ZIP_BIG_PREVLEN) {
            p[0] = len;
            // 此时prevlen占用1个字节
            return 1;
        // 如果entry大于等于ZIP_BIG_PREVLEN，则使用如下处理方式，返回prevlen占用的字节数
        } else {
            return zipStorePrevEntryLengthLarge(p,len);
        }
    }
}

/* Return the number of bytes used to encode the length of the previous
 * entry. The length is returned by setting the var 'prevlensize'. */
// 根据prevlen第一个字节的数值返回prevlen占用的字节数
#define ZIP_DECODE_PREVLENSIZE(ptr, prevlensize) do {                          \
    if ((ptr)[0] < ZIP_BIG_PREVLEN) {                                          \
        (prevlensize) = 1;                                                     \
    } else {                                                                   \
        (prevlensize) = 5;                                                     \
    }                                                                          \
} while(0);

/* Return the length of the previous element, and the number of bytes that
 * are used in order to encode the previous element length.
 * 'ptr' must point to the prevlen prefix of an entry (that encodes the
 * length of the previous entry in order to navigate the elements backward).
 * The length of the previous entry is stored in 'prevlen', the number of
 * bytes needed to encode the previous entry length are stored in
 * 'prevlensize'. */
// 返回prevlen的数值。prevlensize保存了prevlen字段占用的字节数，prevlen保存了prevlen字段中的值
#define ZIP_DECODE_PREVLEN(ptr, prevlensize, prevlen) do {                     \
    ZIP_DECODE_PREVLENSIZE(ptr, prevlensize);                                  \
    if ((prevlensize) == 1) {                                                  \
        (prevlen) = (ptr)[0];                                                  \
    } else if ((prevlensize) == 5) {                                           \
        assert(sizeof((prevlen)) == 4);                                    \
        memcpy(&(prevlen), ((char*)(ptr)) + 1, 4);                             \
        memrev32ifbe(&prevlen);                                                \
    }                                                                          \
} while(0);

/* Given a pointer 'p' to the prevlen info that prefixes an entry, this
 * function returns the difference in number of bytes needed to encode
 * the prevlen if the previous entry changes of size.
 *
 * So if A is the number of bytes used right now to encode the 'prevlen'
 * field.
 *
 * And B is the number of bytes that are needed in order to encode the
 * 'prevlen' if the previous element will be updated to one of size 'len'.
 *
 * Then the function returns B - A
 *
 * So the function returns a positive number if more space is needed,
 * a negative number if less space is needed, or zero if the same space
 * is needed. */
// 计算p的prevlen字段占用的字节数与保存len的prevlen占用的字节数的差值
int zipPrevLenByteDiff(unsigned char *p, unsigned int len) {
    unsigned int prevlensize;
    // prevlensize保存了当前p中的prevlen占用的字节数
    ZIP_DECODE_PREVLENSIZE(p, prevlensize);
    // 计算len对应的prevlen占用的字节与p中的prevlen占用的字节差值
    return zipStorePrevEntryLength(NULL, len) - prevlensize;
}

/* Return the total number of bytes used by the entry pointed to by 'p'. */
// 返回p指向的entry占用的总字节数
unsigned int zipRawEntryLength(unsigned char *p) {
    unsigned int prevlensize, encoding, lensize, len;
    // prevlensize保存prevlen占用的字节数
    ZIP_DECODE_PREVLENSIZE(p, prevlensize);
    // encoding表示encoding本身占用的字节数，lensize表示prevlen的entry-data的字节数
    ZIP_DECODE_LENGTH(p + prevlensize, encoding, lensize, len);
    // 返回整个entry占用的字节数
    return prevlensize + lensize + len;
}

/* Check if string pointed to by 'entry' can be encoded as an integer.
 * Stores the integer value in 'v' and its encoding in 'encoding'. */
/* 尝试将entry编码为整数，entrylen 为entry的长度，解析失败返回0，成功返回1。并在v中返回entry-data解码的整数，
   encoding返回编码类型。entry指向的是entry-data*/
int zipTryEncoding(unsigned char *entry, unsigned int entrylen, long long *v, unsigned char *encoding) {
    long long value;
    // entry的取值范围为(0，32)字节，32字节为字符串编码下的最大长度
    if (entrylen >= 32 || entrylen == 0) return 0;
    // 判断是否可以将entry转变为long long的数值
    if (string2ll((char*)entry,entrylen,&value)) {
        /* Great, the string can be encoded. Check what's the smallest
         * of our encoding types that can hold this value. */
        if (value >= 0 && value <= 12) {
            *encoding = ZIP_INT_IMM_MIN+value;
        } else if (value >= INT8_MIN && value <= INT8_MAX) {
            *encoding = ZIP_INT_8B;
        } else if (value >= INT16_MIN && value <= INT16_MAX) {
            *encoding = ZIP_INT_16B;
        } else if (value >= INT24_MIN && value <= INT24_MAX) {
            *encoding = ZIP_INT_24B;
        } else if (value >= INT32_MIN && value <= INT32_MAX) {
            *encoding = ZIP_INT_32B;
        } else {
            *encoding = ZIP_INT_64B;
        }
        *v = value;
        return 1;
    }
    return 0;
}

/* Store integer 'value' at 'p', encoded as 'encoding' */
// 根据encoding将整数value赋值给p中的entry-data，p指向的是entry-data
void zipSaveInteger(unsigned char *p, int64_t value, unsigned char encoding) {
    int16_t i16;
    int32_t i32;
    int64_t i64;
    // 整数占用1个字节
    if (encoding == ZIP_INT_8B) {
        ((int8_t*)p)[0] = (int8_t)value;
    // // 整数占用2个字节
    } else if (encoding == ZIP_INT_16B) {
        i16 = value;
        memcpy(p,&i16,sizeof(i16));
        memrev16ifbe(p);
    // 整数占用3个字节
    } else if (encoding == ZIP_INT_24B) {
        /* 下面的处理原因是没有如memrev24ifbe的函数进行大小端转换，因此需要先将24bit的value转换为32bit，再调用
            memrev32ifbe进行大小端转换，并保存到p的entry-data中 */
        i32 = value<<8;
        memrev32ifbe(&i32);
        memcpy(p,((uint8_t*)&i32)+1,sizeof(i32)-sizeof(uint8_t));
    // 整数占用4字节
    } else if (encoding == ZIP_INT_32B) {
        i32 = value;
        memcpy(p,&i32,sizeof(i32));
        memrev32ifbe(p);
    // 整数占用8字节
    } else if (encoding == ZIP_INT_64B) {
        i64 = value;
        memcpy(p,&i64,sizeof(i64));
        memrev64ifbe(p);
    // 与encoding同一个字节
    } else if (encoding >= ZIP_INT_IMM_MIN && encoding <= ZIP_INT_IMM_MAX) {
        /* Nothing to do, the value is stored in the encoding itself. */
    } else {
        assert(NULL);
    }
}

/* Read integer encoded as 'encoding' from 'p' */
// 根据encoding读取p的entry-data的整数数值
int64_t zipLoadInteger(unsigned char *p, unsigned char encoding) {
    int16_t i16;
    int32_t i32;
    int64_t i64, ret = 0;
    if (encoding == ZIP_INT_8B) {
        ret = ((int8_t*)p)[0];
    } else if (encoding == ZIP_INT_16B) {
        memcpy(&i16,p,sizeof(i16));
        memrev16ifbe(&i16);
        ret = i16;
    } else if (encoding == ZIP_INT_32B) {
        memcpy(&i32,p,sizeof(i32));
        memrev32ifbe(&i32);
        ret = i32;
    } else if (encoding == ZIP_INT_24B) {
        // 此处的处理与zipSaveInteger中的原因一样
        i32 = 0;
        memcpy(((uint8_t*)&i32)+1,p,sizeof(i32)-sizeof(uint8_t));
        memrev32ifbe(&i32);
        ret = i32>>8;
    } else if (encoding == ZIP_INT_64B) {
        memcpy(&i64,p,sizeof(i64));
        memrev64ifbe(&i64);
        ret = i64;
    } else if (encoding >= ZIP_INT_IMM_MIN && encoding <= ZIP_INT_IMM_MAX) {
        ret = (encoding & ZIP_INT_IMM_MASK)-1;
    } else {
        assert(NULL);
    }
    return ret;
}

/* Return a struct with all information about an entry. */
// 将entry p解析到e中
void zipEntry(unsigned char *p, zlentry *e) {
    // 解析获取p中prevlen占用的字节数以及其数值，分别初始化到e->prevrawlensize, e->prevrawlen
    ZIP_DECODE_PREVLEN(p, e->prevrawlensize, e->prevrawlen);
    // 初始化e->encoding, e->lensize, e->len
    ZIP_DECODE_LENGTH(p + e->prevrawlensize, e->encoding, e->lensize, e->len);
    // 返回p的entry首部prevlen+encoding占用的字节数
    e->headersize = e->prevrawlensize + e->lensize;
    // 指向entry的首部，即prevlen地址
    e->p = p;
}

/* Create a new empty ziplist. */
// 创建一个空的ziplist
unsigned char *ziplistNew(void) {
    // ziplist首部+zlen的大小
    unsigned int bytes = ZIPLIST_HEADER_SIZE+ZIPLIST_END_SIZE;
    unsigned char *zl = zmalloc(bytes);
    ZIPLIST_BYTES(zl) = intrev32ifbe(bytes);
    // 内存直接赋值给zltail
    ZIPLIST_TAIL_OFFSET(zl) = intrev32ifbe(ZIPLIST_HEADER_SIZE);
    // 赋值zllen为0
    ZIPLIST_LENGTH(zl) = 0;
    // ziplist的最后一个置为ZIP_END
    zl[bytes-1] = ZIP_END;
    return zl;
}

/* Resize the ziplist. */
// 修改zlist的内存大小，并将zlbytes字段的值赋值为len
unsigned char *ziplistResize(unsigned char *zl, unsigned int len) {
    zl = zrealloc(zl,len);
    // 将新的长度赋值给zlbytes
    ZIPLIST_BYTES(zl) = intrev32ifbe(len);
    zl[len-1] = ZIP_END;
    return zl;
}

/* When an entry is inserted, we need to set the prevlen field of the next
 * entry to equal the length of the inserted entry. It can occur that this
 * length cannot be encoded in 1 byte and the next entry needs to be grow
 * a bit larger to hold the 5-byte encoded prevlen. This can be done for free,
 * because this only happens when an entry is already being inserted (which
 * causes a realloc and memmove). However, encoding the prevlen may require
 * that this entry is grown as well. This effect may cascade throughout
 * the ziplist when there are consecutive entries with a size close to
 * ZIP_BIG_PREVLEN, so we need to check that the prevlen can be encoded in
 * every consecutive entry.
 *
 * Note that this effect can also happen in reverse, where the bytes required
 * to encode the prevlen field can shrink. This effect is deliberately ignored,
 * because it can cause a "flapping" effect where a chain prevlen fields is
 * first grown and then shrunk again after consecutive inserts. Rather, the
 * field is allowed to stay larger than necessary, because a large prevlen
 * field implies the ziplist is holding large entries anyway.
 *
 * The pointer "p" points to the first entry that does NOT need to be
 * updated, i.e. consecutive fields MAY need an update. */
/* 适用于当ziplist中插入一个新的entry时。新插入的entry可能导致其下一个entry的prevlen长度发生变化，以此类推可能导致多个entry的prevlen发生变化。
   该函数以级联方式修改从p开始的entry的prevlen字段长度以及字段值。如果下一个entry的prevlen字段无法保存当前的entry的长度，则需要扩展下一个entry
   的prevlen字段，如果下一个entry的prevlen足以保存当前的entry的长度，直接赋值即可，即使下一个entry的prevlen为5个字节，而当前entry的长度小
   于ZIP_BIG_PREVLEN，原因是为了防止级联引起后续entry长度的变化。 */
unsigned char *__ziplistCascadeUpdate(unsigned char *zl, unsigned char *p) {
    // curlen保存当前entry zl的整体长度
    size_t curlen = intrev32ifbe(ZIPLIST_BYTES(zl)), rawlen, rawlensize;
    size_t offset, noffset, extra;
    unsigned char *np;
    zlentry cur, next;

    // 遍历并修正以p开始的entry的prevlan
    while (p[0] != ZIP_END) {
        // cur中保存了当前entry p的信息
        zipEntry(p, &cur);
        // rawlen保存了p表示的整个entry的长度
        rawlen = cur.headersize + cur.len;
        // rawlensize保存了p占用的字节数
        rawlensize = zipStorePrevEntryLength(NULL,rawlen);

        /* Abort if there is no next entry. */
        // 如果是最后一个entry，直接跳出遍历
        if (p[rawlen] == ZIP_END) break;
        // 获取下一个entry，保存到next中
        zipEntry(p+rawlen, &next);

        /* Abort when "prevlen" has not changed. */
        /* 如果下一个的prevlen等于当前entry的长度，说明当前entry的prevlen字段长度并没有发生变化，因此也不会影响到后续
           entry的prevlen字段，终止修正过程 */
        if (next.prevrawlen == rawlen) break;

        /* 当下一个entry的prevlen字段占用的字节数小于当前prevlen字段占用的字节数，需要扩展下一个entry的prevlen字段内存。
           扩展的处理方式与__ziplistInsert相同 */
        if (next.prevrawlensize < rawlensize) {
            /* The "prevlen" field of "next" needs more bytes to hold
             * the raw length of "cur". */
            offset = p-zl;
            extra = rawlensize-next.prevrawlensize;
            zl = ziplistResize(zl,curlen+extra);
            p = zl+offset;

            /* Current pointer and offset for next element. */
            np = p+rawlen;
            noffset = np-zl;

            /* Update tail offset when next element is not the tail element. */
            if ((zl+intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl))) != np) {
                ZIPLIST_TAIL_OFFSET(zl) =
                    intrev32ifbe(intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl))+extra);
            }

            /* Move the tail to the back. */
            memmove(np+rawlensize,
                np+next.prevrawlensize,
                curlen-noffset-next.prevrawlensize-1);
            zipStorePrevEntryLength(np,rawlen);

            /* Advance the cursor */
            p += rawlen;
            curlen += extra;
        /* 当下一个entry的prevlen字段占用的字节数大于当前prevlen字段占用的字节数，此时下一个prevlen能够保存当前entry的长度。为了
           防止prevlen字段缩小导致需要调整后续所有entry的长度，此时直接将前一个entry的长度保存到现有的prevlen中即可。
        */
        } else {
            // 此时next.prevrawlensize占用5个字节
            if (next.prevrawlensize > rawlensize) {
                /* This would result in shrinking, which we want to avoid.
                 * So, set "rawlen" in the available bytes. */
                zipStorePrevEntryLengthLarge(p+rawlen,rawlen);
            // 此时next.prevrawlensize占用1个字节
            } else {
                zipStorePrevEntryLength(p+rawlen,rawlen);
            }

            /* Stop here, as the raw length of "next" has not changed. */
            break;
        }
    }
    return zl;
}

/* Delete "num" entries, starting at "p". Returns pointer to the ziplist. */
// 删除ziplist中从p开始的num个entry。如果num过大，则删除存在的entry。
unsigned char *__ziplistDelete(unsigned char *zl, unsigned char *p, unsigned int num) {
    unsigned int i, totlen, deleted = 0;
    size_t offset;
    int nextdiff = 0;
    zlentry first, tail;

    // 将p指向的entry解析到first
    zipEntry(p, &first);
    // 判断可删除的entry的数目，保存到deleted。如果num过大，则p指向最后一个entry，否则指向从p开始的第num+1个entry
    for (i = 0; p[0] != ZIP_END && i < num; i++) {
        p += zipRawEntryLength(p);
        deleted++;
    }
    
    //需要删除的entry的总字节数
    totlen = p-first.p; /* Bytes taken by the element(s) to delete. */
    // 如果需要删除
    if (totlen > 0) {
        // 如果p[0] != ZIP_END，说明需要删除num个entry，主要调整prevlen字段，思路与__ziplistInsert相同
        if (p[0] != ZIP_END) {
            /* Storing `prevrawlen` in this entry may increase or decrease the
             * number of bytes required compare to the current `prevrawlen`.
             * There always is room to store this, because it was previously
             * stored by an entry that is now being deleted. */
            nextdiff = zipPrevLenByteDiff(p,first.prevrawlen);

            /* Note that there is always space when p jumps backward: if
             * the new previous entry is large, one of the deleted elements
             * had a 5 bytes prevlen header, so there is for sure at least
             * 5 bytes free and we need just 4. */
            p -= nextdiff;
            zipStorePrevEntryLength(p,first.prevrawlen);

            /* Update offset for tail */
            ZIPLIST_TAIL_OFFSET(zl) =
                intrev32ifbe(intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl))-totlen);

            /* When the tail contains more than one entry, we need to take
             * "nextdiff" in account as well. Otherwise, a change in the
             * size of prevlen doesn't have an effect on the *tail* offset. */
            zipEntry(p, &tail);
            if (p[tail.headersize+tail.len] != ZIP_END) {
                ZIPLIST_TAIL_OFFSET(zl) =
                   intrev32ifbe(intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl))+nextdiff);
            }

            /* Move tail to the front of the ziplist */
            memmove(first.p,p,
                intrev32ifbe(ZIPLIST_BYTES(zl))-(p-zl)-1);
        // 如果p[0] = ZIP_END，说明删除了从入参p开始的所有entry
        } else {
            /* The entire tail was deleted. No need to move memory. */
            // 更新ziplist的zltail值
            ZIPLIST_TAIL_OFFSET(zl) =
                intrev32ifbe((first.p-zl)-first.prevrawlen);
        }

        /* Resize and update length */
        offset = first.p-zl;
        // 调整ziplist的大小
        zl = ziplistResize(zl, intrev32ifbe(ZIPLIST_BYTES(zl))-totlen+nextdiff);
        // 更新zllen的值
        ZIPLIST_INCR_LENGTH(zl,-deleted);
        p = zl+offset;

        /* When nextdiff != 0, the raw length of the next entry has changed, so
         * we need to cascade the update throughout the ziplist */
        // 级联调整从p开始的entry的prevlen字段
        if (nextdiff != 0)
            zl = __ziplistCascadeUpdate(zl,p);
    }
    return zl;
}

/* Insert item at "p". */
/* 将entry s插入到ziplist zl中的entry p之前，slen为entry-data 字符串的长度(如果可以解码为整数，则需要根据编码计算实际占用的字节数)
   插入一个新的entry需要更新插入的entry中的prevlen字段，插入节点后面的节点的prevlen以及ziplist首部的字段 */
unsigned char *__ziplistInsert(unsigned char *zl, unsigned char *p, unsigned char *s, unsigned int slen) {
    // curlen保存了ziplist zl的字节长度
    size_t curlen = intrev32ifbe(ZIPLIST_BYTES(zl)), reqlen;
    unsigned int prevlensize, prevlen = 0;
    size_t offset;
    int nextdiff = 0;
    unsigned char encoding = 0;
    long long value = 123456789; /* initialized to avoid warning. Using a value
                                    that is easy to see if for some reason
                                    we use it uninitialized. */
    zlentry tail;

    /* Find out prevlen for the entry that is inserted. */
    // 找出插入点的entry后续需要保存的prevlen值。如果不是在末尾插入，则prevlen等于当前要插入节点的prevlen
    if (p[0] != ZIP_END) {
        ZIP_DECODE_PREVLEN(p, prevlensize, prevlen);
    // 如果在末尾插入，则当前节点为ZIP_END，通过偏移量ZIPLIST_ENTRY_TAIL找出ziplist中的最后一个entry，并计算其长度，赋值给prevlen
    } else {
        unsigned char *ptail = ZIPLIST_ENTRY_TAIL(zl);
        if (ptail[0] != ZIP_END) {
            prevlen = zipRawEntryLength(ptail);
        }
    }

    /* See if the entry can be encoded */
    // 尝试将entry s解码为整数，value保存了解码的entry-data的数值，encoding保存整数编码类型
    if (zipTryEncoding(s,slen,&value,&encoding)) {
        /* 'encoding' is set to the appropriate integer encoding */
        // reqlen保存了整数编码对应的entry-data占用的字节数
        reqlen = zipIntSize(encoding);
    // 如果是字符串编码，其entry-data长度就等于字符串占用的字节数
    } else {
        /* 'encoding' is untouched, however zipStoreEntryEncoding will use the
         * string length to figure out how to encode it. */
        reqlen = slen;
    }
    /* We need space for both the length of the previous entry and
     * the length of the payload. */
    // reqlen + prevlen字段长度 + encoding字段长度，至此给出了entry s的总长度 reqlen ，并计算出了prevlen，encoding
    reqlen += zipStorePrevEntryLength(NULL,prevlen);
    reqlen += zipStoreEntryEncoding(NULL,encoding,slen);

    /* When the insert position is not equal to the tail, we need to
     * make sure that the next entry can hold this entry's length in
     * its prevlen field. */
    // 用于计算下一个entry是否的prevlen是否可以有足够的空间保存插入的entry的长度
    int forcelarge = 0;
    /* 如果插入点p的位置非末尾，计算reqlen对应的prevlen字段长度 - p的prevlen字段长度的差值；否则将差值设置为0。
       p的prevlen字段值后续要赋值到新插入的entry的prevlen字段中，nextdiff用于判断新entry的prevlen字段是否有足够的空间保存前一个entry的长度
       ，以及调整p的prevlen的长度，nextdiff为0表示不需要调整原始entry的prevlen字段的长度 */
    nextdiff = (p[0] != ZIP_END) ? zipPrevLenByteDiff(p,reqlen) : 0;
    /* nextdiff == -4的场景只能有一种情况：reqlen对应的prevlen仅占1个字节，而p的prevlen占5个字节，
       reqlen < 4  ？？？？原始p的prevlen已经等于5了，新插入的 prevlen 怎么可能小于5，总的更不可能小于4 */
    if (nextdiff == -4 && reqlen < 4) {
        nextdiff = 0;
        forcelarge = 1;
    }

    /* Store offset because a realloc may change the address of zl. */
    // 保存p在zl中的偏移量，因为在执行realloc之后p的原始地址发生了变化
    offset = p-zl;
    // 调整ziplist的长度，新的ziplist长度为原长度加上新entry的总长度，再加上新entry的下一个节点的prevlen字段的变化量
    zl = ziplistResize(zl,curlen+reqlen+nextdiff);
    // 重新获取p，用于设置为新插入的entry
    p = zl+offset;

    /* Apply memory move when necessary and update tail offset. */
    // 如果不在末尾插入s，此时p非zlend
    if (p[0] != ZIP_END) {
        /* Subtract one because of the ZIP_END bytes */
        /* 将p以及p后面的entry拷贝到p+reqlen地址处，[p,p+reqlen)之间的地址用于保存新插入的entry。由于p的prevlen也需要
           根据新插入的entry进行调整，因此p的起始地址。p的起始地址也需要根据其新的prevlen进行调整，如果nextdiff为-4，说明
           原来的prevlen字段过大(5个字节)，保留第5个字节即可(保留1个字节)即可；如果nextdiff为4，说明原来的prevlen字段过小，
           需要扩大p的prevlen的大小，即将p的指针向前偏移4个字节，后续会使用正确的长度填充prevlen字段 */ 
        // memmove能够保证源在被覆盖之前将重叠区域的字节拷贝到目标区域中
        memmove(p+reqlen,p-nextdiff,curlen-offset-1+nextdiff);

        /* Encode this entry's raw length in the next entry. */
        // 更新新插入entry对应的下一个entry的prevlen字段值，prevlen字段为5字节
        if (forcelarge)
            zipStorePrevEntryLengthLarge(p+reqlen,reqlen);
        // 使用reqlen更新新entry的下一个entry的prevlen字段的值，prevlen字段为1字节
        else
            zipStorePrevEntryLength(p+reqlen,reqlen);

        /* Update offset for tail */
        // 更新ziplist的zltail值，增加偏移量，下面还要对zltail的值进行验证
        ZIPLIST_TAIL_OFFSET(zl) =
            intrev32ifbe(intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl))+reqlen);

        /* When the tail contains more than one entry, we need to take
         * "nextdiff" in account as well. Otherwise, a change in the
         * size of prevlen doesn't have an effect on the *tail* offset. */
        // 将新entry的下一个entry解析到tail中
        zipEntry(p+reqlen, &tail);
        // 由于nextdiff的存在，需要判断zltail的值是否正确
        if (p[reqlen+tail.headersize+tail.len] != ZIP_END) {
            ZIPLIST_TAIL_OFFSET(zl) =
                intrev32ifbe(intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl))+nextdiff);
        }
    // 如果将s作为zl中的最后一个节点，此时p为zlend，重新设置zltail的值
    } else {
        /* This element will be the new tail. */
        ZIPLIST_TAIL_OFFSET(zl) = intrev32ifbe(p-zl);
    }

    /* When nextdiff != 0, the raw length of the next entry has changed, so
     * we need to cascade the update throughout the ziplist */
    // 当nextdiff不为0时，说明下一个entry的prevlen长度可能发生了变化，导致下一个entry的总长度发生变化，由此可能导致下下个entry的prevlen字段长度发生变化，以此类推
    if (nextdiff != 0) {
        // 保存插入的entry的偏移量
        offset = p-zl;
        // 以级联方式修正ziplist中从p+reqlen开始的entry的prevlen字段长度以及字段值
        zl = __ziplistCascadeUpdate(zl,p+reqlen);
        // 重新获取新插入的entry的地址
        p = zl+offset;
    }

    /* Write the entry */
    // 更新p中的内容，此时p表示新插入的entry
    p += zipStorePrevEntryLength(p,prevlen);
    p += zipStoreEntryEncoding(p,encoding,slen);
    // 赋值新entry的entry-data
    if (ZIP_IS_STR(encoding)) {
        memcpy(p,s,slen);
    } else {
        zipSaveInteger(p,value,encoding);
    }
    // 因为新增了一个entry，因此将zllen增加1
    ZIPLIST_INCR_LENGTH(zl,1);
    return zl;
}

/* Merge ziplists 'first' and 'second' by appending 'second' to 'first'.
 *
 * NOTE: The larger ziplist is reallocated to contain the new merged ziplist.
 * Either 'first' or 'second' can be used for the result.  The parameter not
 * used will be free'd and set to NULL.
 *
 * After calling this function, the input parameters are no longer valid since
 * they are changed and free'd in-place.
 *
 * The result ziplist is the contents of 'first' followed by 'second'.
 *
 * On failure: returns NULL if the merge is impossible.
 * On success: returns the merged ziplist (which is expanded version of either
 * 'first' or 'second', also frees the other unused input ziplist, and sets the
 * input ziplist argument equal to newly reallocated ziplist return value. */
// 合并ziplist first和second，返回将second追加到first之后的ziplist
unsigned char *ziplistMerge(unsigned char **first, unsigned char **second) {
    /* If any params are null, we can't merge, so NULL. */
    if (first == NULL || *first == NULL || second == NULL || *second == NULL)
        return NULL;

    /* Can't merge same list into itself. */
    // 如果两个ziplist的地址相同，则说明是同一个ziplist，没有必要进行合并
    if (*first == *second)
        return NULL;
    
    // 获取first ziplist的字节长度和entry数目
    size_t first_bytes = intrev32ifbe(ZIPLIST_BYTES(*first));
    size_t first_len = intrev16ifbe(ZIPLIST_LENGTH(*first));

    // 获取second ziplist的字节长度和entry数目
    size_t second_bytes = intrev32ifbe(ZIPLIST_BYTES(*second));
    size_t second_len = intrev16ifbe(ZIPLIST_LENGTH(*second));

    int append;
    unsigned char *source, *target;
    size_t target_bytes, source_bytes;
    /* Pick the largest ziplist so we can resize easily in-place.
     * We must also track if we are now appending or prepending to
     * the target ziplist. */
    // 选择一个内存比较小的进行resize，为了减少申请的内存数
    if (first_len >= second_len) {
        /* retain first, append second to first. */
        target = *first;
        target_bytes = first_bytes;
        source = *second;
        source_bytes = second_bytes;
        append = 1;  // 表示在target后面追加，即将second追加到first之后
    } else {
        /* else, retain second, prepend first to second. */
        target = *second;
        target_bytes = second_bytes;
        source = *first;
        source_bytes = first_bytes;
        append = 0; //表示在target前面追加，即将first追加到second之前
    }

    /* Calculate final bytes (subtract one pair of metadata) */
    // first和second中的entry占用的字节总和
    size_t zlbytes = first_bytes + second_bytes -
                     ZIPLIST_HEADER_SIZE - ZIPLIST_END_SIZE;
    // first和second中的entry的总和
    size_t zllength = first_len + second_len;

    /* Combined zl length should be limited within UINT16_MAX */
    // 计算zllength的最终数值
    zllength = zllength < UINT16_MAX ? zllength : UINT16_MAX;

    /* Save offset positions before we start ripping memory apart. */
    // 获取first和second ziplist中的zltail的值，即到最后一个entry的偏移量
    size_t first_offset = intrev32ifbe(ZIPLIST_TAIL_OFFSET(*first));
    size_t second_offset = intrev32ifbe(ZIPLIST_TAIL_OFFSET(*second));

    /* Extend target to new zlbytes then append or prepend source. */
    // 扩展申请内存，大小等于first和second中的entry占用的字节总和
    target = zrealloc(target, zlbytes);
    // 如果在target后面追加，此时target指向first的首地址
    if (append) {
        /* append == appending to target */
        /* Copy source after target (copying over original [END]):
         *   [TARGET - END, SOURCE - HEADER] */
        // 将second的entry追加到first的最后一个entry(非zlend)之后即可
        memcpy(target + target_bytes - ZIPLIST_END_SIZE,
               source + ZIPLIST_HEADER_SIZE,
               source_bytes - ZIPLIST_HEADER_SIZE);
    // 如果在target前面追加，此时target指向second的首地址
    } else {
        /* !append == prepending to target */
        /* Move target *contents* exactly size of (source - [END]),
         * then copy source into vacataed space (source - [END]):
         *   [SOURCE - END, TARGET - HEADER] */
        // 首先将second的entrymemmove到first的最后一个entry(非zlend)之后
        memmove(target + source_bytes - ZIPLIST_END_SIZE,
                target + ZIPLIST_HEADER_SIZE,
                target_bytes - ZIPLIST_HEADER_SIZE);
        // 然后将first的内容(含ziplist首部)拷贝过来即可
        memcpy(target, source, source_bytes - ZIPLIST_END_SIZE);
    }

    /* Update header metadata. */
    // 更新合并后的ziplist中的zlbytes和zllen字段
    ZIPLIST_BYTES(target) = intrev32ifbe(zlbytes);
    ZIPLIST_LENGTH(target) = intrev16ifbe(zllength);
    /* New tail offset is:
     *   + N bytes of first ziplist
     *   - 1 byte for [END] of first ziplist
     *   + M bytes for the offset of the original tail of the second ziplist
     *   - J bytes for HEADER because second_offset keeps no header. */
    // 更新合并后的ziplist中的zltail
    ZIPLIST_TAIL_OFFSET(target) = intrev32ifbe(
                                   (first_bytes - ZIPLIST_END_SIZE) +
                                   (second_offset - ZIPLIST_HEADER_SIZE));

    /* __ziplistCascadeUpdate just fixes the prev length values until it finds a
     * correct prev length value (then it assumes the rest of the list is okay).
     * We tell CascadeUpdate to start at the first ziplist's tail element to fix
     * the merge seam. */
    // 级联更新ziplist中的entry的prevlen， target+first_offset为first的最后一个entry
    target = __ziplistCascadeUpdate(target, target+first_offset);

    /* Now free and NULL out what we didn't realloc */
    // 释放原始没有进行realloc操作的ziplist
    if (append) {
        zfree(*second);
        *second = NULL;
        *first = target;
    } else {
        zfree(*first);
        *first = NULL;
        *second = target;
    }
    // 返回合并后的ziplist
    return target;
}

/* 在ziplist的首部或尾部插入一个新的entry s
   where为ZIPLIST_HEAD，表示push到ziplist的首部，where为ZIPLIST_TAIL，表示push到ziplist的尾部*/
unsigned char *ziplistPush(unsigned char *zl, unsigned char *s, unsigned int slen, int where) {
    unsigned char *p;
    p = (where == ZIPLIST_HEAD) ? ZIPLIST_ENTRY_HEAD(zl) : ZIPLIST_ENTRY_END(zl);
    return __ziplistInsert(zl,p,s,slen);
}

/* Returns an offset to use for iterating with ziplistNext. When the given
 * index is negative, the list is traversed back to front. When the list
 * doesn't contain an element at the provided index, NULL is returned. */
// 在ziplist中查找第index个entry，支持正向查找和逆序查找。如果index非法或最终找到的p为ZIP_END，则返回NULL 
unsigned char *ziplistIndex(unsigned char *zl, int index) {
    unsigned char *p;
    unsigned int prevlensize, prevlen = 0;
    // 逆序查找
    if (index < 0) {
        index = (-index)-1;
        p = ZIPLIST_ENTRY_TAIL(zl);
        if (p[0] != ZIP_END) {
            ZIP_DECODE_PREVLEN(p, prevlensize, prevlen);
            while (prevlen > 0 && index--) {
                p -= prevlen;
                ZIP_DECODE_PREVLEN(p, prevlensize, prevlen);
            }
        }
    // 如果index大于等于0，正向查找
    } else {
        // 获取ziplist中的第一个entry的地址
        p = ZIPLIST_ENTRY_HEAD(zl);
        // 遍历查找合适的entry
        while (p[0] != ZIP_END && index--) {
            p += zipRawEntryLength(p);
        }
    }
    // 如果index > 0说明入参index非法，此时返回NULL
    return (p[0] == ZIP_END || index > 0) ? NULL : p;
}

/* Return pointer to next entry in ziplist.
 *
 * zl is the pointer to the ziplist
 * p is the pointer to the current element
 *
 * The element after 'p' is returned, otherwise NULL if we are at the end. */
// 返回entry p的下一个entry的地址，如果p为ZIP_END或p的下一个entry为ZIP_END，则返回NULL
unsigned char *ziplistNext(unsigned char *zl, unsigned char *p) {
    ((void) zl);

    /* "p" could be equal to ZIP_END, caused by ziplistDelete,
     * and we should return NULL. Otherwise, we should return NULL
     * when the *next* element is ZIP_END (there is no next entry). */
    if (p[0] == ZIP_END) {
        return NULL;
    }

    p += zipRawEntryLength(p);
    if (p[0] == ZIP_END) {
        return NULL;
    }

    return p;
}

/* Return pointer to previous entry in ziplist. */
// 返回p的前一个entry，如果p为第一个entry或ziplist的entry为空，返回NULL
unsigned char *ziplistPrev(unsigned char *zl, unsigned char *p) {
    unsigned int prevlensize, prevlen = 0;

    /* Iterating backwards from ZIP_END should return the tail. When "p" is
     * equal to the first element of the list, we're already at the head,
     * and should return NULL. */
    // 如果p为ZIP_END，如果zl中的entry非空，则通过偏移量直接获取最后一个entry，否则返回NULL
    if (p[0] == ZIP_END) {
        p = ZIPLIST_ENTRY_TAIL(zl);
        return (p[0] == ZIP_END) ? NULL : p;
    // 如果p为第一个entry，返回NULL
    } else if (p == ZIPLIST_ENTRY_HEAD(zl)) {
        return NULL;
    // 通过指针偏移返回前一个entry的地址
    } else {
        ZIP_DECODE_PREVLEN(p, prevlensize, prevlen);
        assert(prevlen > 0);
        return p-prevlen;
    }
}

/* Get entry pointed to by 'p' and store in either '*sstr' or 'sval' depending
 * on the encoding of the entry. '*sstr' is always set to NULL to be able
 * to find out whether the string pointer or the integer value was set.
 * Return 0 if 'p' points to the end of the ziplist, 1 otherwise. */
/* 如果p的编码为字符串，将entry p的entry-data保存到sstr中，将p的entry-data的长度保存到slen中；如果是整数编码，将数值返回到sval中。
   处理成功返回1，否则返回0 */
unsigned int ziplistGet(unsigned char *p, unsigned char **sstr, unsigned int *slen, long long *sval) {
    zlentry entry;
    if (p == NULL || p[0] == ZIP_END) return 0;
    if (sstr) *sstr = NULL;

    // 将p解析到entry中
    zipEntry(p, &entry);
    if (ZIP_IS_STR(entry.encoding)) {
        if (sstr) {
            *slen = entry.len;
            *sstr = p+entry.headersize;
        }
    } else {
        if (sval) {
            *sval = zipLoadInteger(p+entry.headersize,entry.encoding);
        }
    }
    return 1;
}

/* Insert an entry at "p". */
// 将entry s插入到ziplist zl中的entry p之前，slen为entry s的entry-data字符串长度(非保存entry-data的空间大小)
unsigned char *ziplistInsert(unsigned char *zl, unsigned char *p, unsigned char *s, unsigned int slen) {
    return __ziplistInsert(zl,p,s,slen);
}

/* Delete a single entry from the ziplist, pointed to by *p.
 * Also update *p in place, to be able to iterate over the
 * ziplist, while deleting entries. */
// 删除ziplist zl中位于p的entry，并更新p保存的地址
unsigned char *ziplistDelete(unsigned char *zl, unsigned char **p) {
    // 用于更新出参p的地址值
    size_t offset = *p-zl;
    // 删除从*p开始的1个entry
    zl = __ziplistDelete(zl,*p,1);

    /* Store pointer to current element in p, because ziplistDelete will
     * do a realloc which might result in a different "zl"-pointer.
     * When the delete direction is back to front, we might delete the last
     * entry and end up with "p" pointing to ZIP_END, so check this. */
    *p = zl+offset;
    return zl;
}

/* Delete a range of entries from the ziplist. */
// 删除ziplist zl第index开始的num个entry
unsigned char *ziplistDeleteRange(unsigned char *zl, int index, unsigned int num) {
    // 首先获取ziplist中第index的entry的地址
    unsigned char *p = ziplistIndex(zl,index);
    return (p == NULL) ? zl : __ziplistDelete(zl,p,num);
}

/* Compare entry pointer to by 'p' with 'sstr' of length 'slen'. */
/* Return 1 if equal. */
// 比较p的entry-data和sstr是否相同。比较相同时返回1
unsigned int ziplistCompare(unsigned char *p, unsigned char *sstr, unsigned int slen) {
    zlentry entry;
    unsigned char sencoding;
    long long zval, sval;
    if (p[0] == ZIP_END) return 0;

    // 将p解析到entry
    zipEntry(p, &entry);
    // 比较字符串
    if (ZIP_IS_STR(entry.encoding)) {
        /* Raw compare */
        // 长度相同时进一步比较字符串，长度不相同的字符串肯定不相同
        if (entry.len == slen) {
            return memcmp(p+entry.headersize,sstr,slen) == 0;
        } else {
            return 0;
        }
    // 比较整数
    } else {
        /* Try to compare encoded values. Don't compare encoding because
         * different implementations may encoded integers differently. */
        if (zipTryEncoding(sstr,slen,&sval,&sencoding)) {
          zval = zipLoadInteger(p+entry.headersize,entry.encoding);
          return zval == sval;
        }
    }
    return 0;
}

/* Find pointer to the entry equal to the specified entry. Skip 'skip' entries
 * between every comparison. Returns NULL when the field could not be found. */
// 在ziplist中查找一个entry，该entry的entry-data与vstr相同。skip为每次查找失败跳过的entry的个数。查找失败发明会NULL
unsigned char *ziplistFind(unsigned char *p, unsigned char *vstr, unsigned int vlen, unsigned int skip) {
    int skipcnt = 0;
    unsigned char vencoding = 0;
    long long vll = 0;

    // 遍历ziplist
    while (p[0] != ZIP_END) {
        unsigned int prevlensize, encoding, lensize, len;
        unsigned char *q;

        ZIP_DECODE_PREVLENSIZE(p, prevlensize);
        // lensize为encoding字段长度，len为entry-data长度
        ZIP_DECODE_LENGTH(p + prevlensize, encoding, lensize, len);
        // q为p的entry-data的起始地址
        q = p + prevlensize + lensize;

        if (skipcnt == 0) {
            /* Compare current entry with specified entry */
            // 字符串比较
            if (ZIP_IS_STR(encoding)) {
                if (len == vlen && memcmp(q, vstr, vlen) == 0) {
                    return p;
                }
            } else {
                /* Find out if the searched field can be encoded. Note that
                 * we do it only the first time, once done vencoding is set
                 * to non-zero and vll is set to the integer value. */
                if (vencoding == 0) {
                    // 如果vstr解析失败则将vencoding置为UCHAR_MAX
                    if (!zipTryEncoding(vstr, vlen, &vll, &vencoding)) {
                        /* If the entry can't be encoded we set it to
                         * UCHAR_MAX so that we don't retry again the next
                         * time. */
                        vencoding = UCHAR_MAX;
                    }
                    /* Must be non-zero by now */
                    // vstr异常，退出
                    assert(vencoding);
                }

                /* Compare current entry with specified entry, do it only
                 * if vencoding != UCHAR_MAX because if there is no encoding
                 * possible for the field it can't be a valid integer. */
                // 如果vstr可解析为数值，则进行整数比较
                if (vencoding != UCHAR_MAX) {
                    long long ll = zipLoadInteger(q, encoding);
                    if (ll == vll) {
                        return p;
                    }
                }
            }

            /* Reset skip count */
            skipcnt = skip;
        } else {
            /* Skip entry */
            skipcnt--;
        }

        /* Move to next entry */
        // p指向下一个entry
        p = q + len;
    }

    return NULL;
}

/* Return length of ziplist. */
// 返回ziplist中的entry的数目
unsigned int ziplistLen(unsigned char *zl) {
    unsigned int len = 0;
    // 如果ziplist中entry的数目小于UINT16_MAX，则返回zllen中的值
    if (intrev16ifbe(ZIPLIST_LENGTH(zl)) < UINT16_MAX) {
        len = intrev16ifbe(ZIPLIST_LENGTH(zl));
    // 如果 如果ziplist中entry的数目等于UINT16_MAX，此时需要遍历整个ziplist获取其包含的entry的数目
    } else {
        // 偏移到第一个entry的地址，并进行遍历
        unsigned char *p = zl+ZIPLIST_HEADER_SIZE;
        while (*p != ZIP_END) {
            p += zipRawEntryLength(p);
            len++;
        }

        /* Re-store length if small enough */
        if (len < UINT16_MAX) ZIPLIST_LENGTH(zl) = intrev16ifbe(len);
    }
    return len;
}

/* Return ziplist blob size in bytes. */
// 返回ziplist的字节长度
size_t ziplistBlobLen(unsigned char *zl) {
    return intrev32ifbe(ZIPLIST_BYTES(zl));
}

// 遍历打印ziplist zl中的entry的内容
void ziplistRepr(unsigned char *zl) {
    unsigned char *p;
    int index = 0;
    zlentry entry;

    printf(
        "{total bytes %d} "
        "{num entries %u}\n"
        "{tail offset %u}\n",
        intrev32ifbe(ZIPLIST_BYTES(zl)),
        intrev16ifbe(ZIPLIST_LENGTH(zl)),
        intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl)));
    p = ZIPLIST_ENTRY_HEAD(zl);
    while(*p != ZIP_END) {
        zipEntry(p, &entry);
        printf(
            "{\n"
                "\taddr 0x%08lx,\n"
                "\tindex %2d,\n"
                "\toffset %5ld,\n"
                "\thdr+entry len: %5u,\n"
                "\thdr len%2u,\n"
                "\tprevrawlen: %5u,\n"
                "\tprevrawlensize: %2u,\n"
                "\tpayload %5u\n",
            (long unsigned)p,
            index,
            (unsigned long) (p-zl),
            entry.headersize+entry.len,
            entry.headersize,
            entry.prevrawlen,
            entry.prevrawlensize,
            entry.len);
        printf("\tbytes: ");
        for (unsigned int i = 0; i < entry.headersize+entry.len; i++) {
            printf("%02x|",p[i]);
        }
        printf("\n");
        p += entry.headersize;
        if (ZIP_IS_STR(entry.encoding)) {
            printf("\t[str]");
            if (entry.len > 40) {
                if (fwrite(p,40,1,stdout) == 0) perror("fwrite");
                printf("...");
            } else {
                if (entry.len &&
                    fwrite(p,entry.len,1,stdout) == 0) perror("fwrite");
            }
        } else {
            printf("\t[int]%lld", (long long) zipLoadInteger(p,entry.encoding));
        }
        printf("\n}\n");
        p += entry.len;
        index++;
    }
    printf("{end}\n\n");
}

#ifdef REDIS_TEST
#include <sys/time.h>
#include "adlist.h"
#include "sds.h"

#define debug(f, ...) { if (DEBUG) printf(f, __VA_ARGS__); }

static unsigned char *createList() {
    unsigned char *zl = ziplistNew();
    zl = ziplistPush(zl, (unsigned char*)"foo", 3, ZIPLIST_TAIL);
    zl = ziplistPush(zl, (unsigned char*)"quux", 4, ZIPLIST_TAIL);
    zl = ziplistPush(zl, (unsigned char*)"hello", 5, ZIPLIST_HEAD);
    zl = ziplistPush(zl, (unsigned char*)"1024", 4, ZIPLIST_TAIL);
    return zl;
}

static unsigned char *createIntList() {
    unsigned char *zl = ziplistNew();
    char buf[32];

    sprintf(buf, "100");
    zl = ziplistPush(zl, (unsigned char*)buf, strlen(buf), ZIPLIST_TAIL);
    sprintf(buf, "128000");
    zl = ziplistPush(zl, (unsigned char*)buf, strlen(buf), ZIPLIST_TAIL);
    sprintf(buf, "-100");
    zl = ziplistPush(zl, (unsigned char*)buf, strlen(buf), ZIPLIST_HEAD);
    sprintf(buf, "4294967296");
    zl = ziplistPush(zl, (unsigned char*)buf, strlen(buf), ZIPLIST_HEAD);
    sprintf(buf, "non integer");
    zl = ziplistPush(zl, (unsigned char*)buf, strlen(buf), ZIPLIST_TAIL);
    sprintf(buf, "much much longer non integer");
    zl = ziplistPush(zl, (unsigned char*)buf, strlen(buf), ZIPLIST_TAIL);
    return zl;
}

static long long usec(void) {
    struct timeval tv;
    gettimeofday(&tv,NULL);
    return (((long long)tv.tv_sec)*1000000)+tv.tv_usec;
}

static void stress(int pos, int num, int maxsize, int dnum) {
    int i,j,k;
    unsigned char *zl;
    char posstr[2][5] = { "HEAD", "TAIL" };
    long long start;
    for (i = 0; i < maxsize; i+=dnum) {
        zl = ziplistNew();
        for (j = 0; j < i; j++) {
            zl = ziplistPush(zl,(unsigned char*)"quux",4,ZIPLIST_TAIL);
        }

        /* Do num times a push+pop from pos */
        start = usec();
        for (k = 0; k < num; k++) {
            zl = ziplistPush(zl,(unsigned char*)"quux",4,pos);
            zl = ziplistDeleteRange(zl,0,1);
        }
        printf("List size: %8d, bytes: %8d, %dx push+pop (%s): %6lld usec\n",
            i,intrev32ifbe(ZIPLIST_BYTES(zl)),num,posstr[pos],usec()-start);
        zfree(zl);
    }
}

static unsigned char *pop(unsigned char *zl, int where) {
    unsigned char *p, *vstr;
    unsigned int vlen;
    long long vlong;

    p = ziplistIndex(zl,where == ZIPLIST_HEAD ? 0 : -1);
    if (ziplistGet(p,&vstr,&vlen,&vlong)) {
        if (where == ZIPLIST_HEAD)
            printf("Pop head: ");
        else
            printf("Pop tail: ");

        if (vstr) {
            if (vlen && fwrite(vstr,vlen,1,stdout) == 0) perror("fwrite");
        }
        else {
            printf("%lld", vlong);
        }

        printf("\n");
        return ziplistDelete(zl,&p);
    } else {
        printf("ERROR: Could not pop\n");
        exit(1);
    }
}

static int randstring(char *target, unsigned int min, unsigned int max) {
    int p = 0;
    int len = min+rand()%(max-min+1);
    int minval, maxval;
    switch(rand() % 3) {
    case 0:
        minval = 0;
        maxval = 255;
    break;
    case 1:
        minval = 48;
        maxval = 122;
    break;
    case 2:
        minval = 48;
        maxval = 52;
    break;
    default:
        assert(NULL);
    }

    while(p < len)
        target[p++] = minval+rand()%(maxval-minval+1);
    return len;
}

static void verify(unsigned char *zl, zlentry *e) {
    int len = ziplistLen(zl);
    zlentry _e;

    ZIPLIST_ENTRY_ZERO(&_e);

    for (int i = 0; i < len; i++) {
        memset(&e[i], 0, sizeof(zlentry));
        zipEntry(ziplistIndex(zl, i), &e[i]);

        memset(&_e, 0, sizeof(zlentry));
        zipEntry(ziplistIndex(zl, -len+i), &_e);

        assert(memcmp(&e[i], &_e, sizeof(zlentry)) == 0);
    }
}

int ziplistTest(int argc, char **argv) {
    unsigned char *zl, *p;
    unsigned char *entry;
    unsigned int elen;
    long long value;

    /* If an argument is given, use it as the random seed. */
    if (argc == 2)
        srand(atoi(argv[1]));

    zl = createIntList();
    ziplistRepr(zl);

    zfree(zl);

    zl = createList();
    ziplistRepr(zl);

    zl = pop(zl,ZIPLIST_TAIL);
    ziplistRepr(zl);

    zl = pop(zl,ZIPLIST_HEAD);
    ziplistRepr(zl);

    zl = pop(zl,ZIPLIST_TAIL);
    ziplistRepr(zl);

    zl = pop(zl,ZIPLIST_TAIL);
    ziplistRepr(zl);

    zfree(zl);

    printf("Get element at index 3:\n");
    {
        zl = createList();
        p = ziplistIndex(zl, 3);
        if (!ziplistGet(p, &entry, &elen, &value)) {
            printf("ERROR: Could not access index 3\n");
            return 1;
        }
        if (entry) {
            if (elen && fwrite(entry,elen,1,stdout) == 0) perror("fwrite");
            printf("\n");
        } else {
            printf("%lld\n", value);
        }
        printf("\n");
        zfree(zl);
    }

    printf("Get element at index 4 (out of range):\n");
    {
        zl = createList();
        p = ziplistIndex(zl, 4);
        if (p == NULL) {
            printf("No entry\n");
        } else {
            printf("ERROR: Out of range index should return NULL, returned offset: %ld\n", p-zl);
            return 1;
        }
        printf("\n");
        zfree(zl);
    }

    printf("Get element at index -1 (last element):\n");
    {
        zl = createList();
        p = ziplistIndex(zl, -1);
        if (!ziplistGet(p, &entry, &elen, &value)) {
            printf("ERROR: Could not access index -1\n");
            return 1;
        }
        if (entry) {
            if (elen && fwrite(entry,elen,1,stdout) == 0) perror("fwrite");
            printf("\n");
        } else {
            printf("%lld\n", value);
        }
        printf("\n");
        zfree(zl);
    }

    printf("Get element at index -4 (first element):\n");
    {
        zl = createList();
        p = ziplistIndex(zl, -4);
        if (!ziplistGet(p, &entry, &elen, &value)) {
            printf("ERROR: Could not access index -4\n");
            return 1;
        }
        if (entry) {
            if (elen && fwrite(entry,elen,1,stdout) == 0) perror("fwrite");
            printf("\n");
        } else {
            printf("%lld\n", value);
        }
        printf("\n");
        zfree(zl);
    }

    printf("Get element at index -5 (reverse out of range):\n");
    {
        zl = createList();
        p = ziplistIndex(zl, -5);
        if (p == NULL) {
            printf("No entry\n");
        } else {
            printf("ERROR: Out of range index should return NULL, returned offset: %ld\n", p-zl);
            return 1;
        }
        printf("\n");
        zfree(zl);
    }

    printf("Iterate list from 0 to end:\n");
    {
        zl = createList();
        p = ziplistIndex(zl, 0);
        while (ziplistGet(p, &entry, &elen, &value)) {
            printf("Entry: ");
            if (entry) {
                if (elen && fwrite(entry,elen,1,stdout) == 0) perror("fwrite");
            } else {
                printf("%lld", value);
            }
            p = ziplistNext(zl,p);
            printf("\n");
        }
        printf("\n");
        zfree(zl);
    }

    printf("Iterate list from 1 to end:\n");
    {
        zl = createList();
        p = ziplistIndex(zl, 1);
        while (ziplistGet(p, &entry, &elen, &value)) {
            printf("Entry: ");
            if (entry) {
                if (elen && fwrite(entry,elen,1,stdout) == 0) perror("fwrite");
            } else {
                printf("%lld", value);
            }
            p = ziplistNext(zl,p);
            printf("\n");
        }
        printf("\n");
        zfree(zl);
    }

    printf("Iterate list from 2 to end:\n");
    {
        zl = createList();
        p = ziplistIndex(zl, 2);
        while (ziplistGet(p, &entry, &elen, &value)) {
            printf("Entry: ");
            if (entry) {
                if (elen && fwrite(entry,elen,1,stdout) == 0) perror("fwrite");
            } else {
                printf("%lld", value);
            }
            p = ziplistNext(zl,p);
            printf("\n");
        }
        printf("\n");
        zfree(zl);
    }

    printf("Iterate starting out of range:\n");
    {
        zl = createList();
        p = ziplistIndex(zl, 4);
        if (!ziplistGet(p, &entry, &elen, &value)) {
            printf("No entry\n");
        } else {
            printf("ERROR\n");
        }
        printf("\n");
        zfree(zl);
    }

    printf("Iterate from back to front:\n");
    {
        zl = createList();
        p = ziplistIndex(zl, -1);
        while (ziplistGet(p, &entry, &elen, &value)) {
            printf("Entry: ");
            if (entry) {
                if (elen && fwrite(entry,elen,1,stdout) == 0) perror("fwrite");
            } else {
                printf("%lld", value);
            }
            p = ziplistPrev(zl,p);
            printf("\n");
        }
        printf("\n");
        zfree(zl);
    }

    printf("Iterate from back to front, deleting all items:\n");
    {
        zl = createList();
        p = ziplistIndex(zl, -1);
        while (ziplistGet(p, &entry, &elen, &value)) {
            printf("Entry: ");
            if (entry) {
                if (elen && fwrite(entry,elen,1,stdout) == 0) perror("fwrite");
            } else {
                printf("%lld", value);
            }
            zl = ziplistDelete(zl,&p);
            p = ziplistPrev(zl,p);
            printf("\n");
        }
        printf("\n");
        zfree(zl);
    }

    printf("Delete inclusive range 0,0:\n");
    {
        zl = createList();
        zl = ziplistDeleteRange(zl, 0, 1);
        ziplistRepr(zl);
        zfree(zl);
    }

    printf("Delete inclusive range 0,1:\n");
    {
        zl = createList();
        zl = ziplistDeleteRange(zl, 0, 2);
        ziplistRepr(zl);
        zfree(zl);
    }

    printf("Delete inclusive range 1,2:\n");
    {
        zl = createList();
        zl = ziplistDeleteRange(zl, 1, 2);
        ziplistRepr(zl);
        zfree(zl);
    }

    printf("Delete with start index out of range:\n");
    {
        zl = createList();
        zl = ziplistDeleteRange(zl, 5, 1);
        ziplistRepr(zl);
        zfree(zl);
    }

    printf("Delete with num overflow:\n");
    {
        zl = createList();
        zl = ziplistDeleteRange(zl, 1, 5);
        ziplistRepr(zl);
        zfree(zl);
    }

    printf("Delete foo while iterating:\n");
    {
        zl = createList();
        p = ziplistIndex(zl,0);
        while (ziplistGet(p,&entry,&elen,&value)) {
            if (entry && strncmp("foo",(char*)entry,elen) == 0) {
                printf("Delete foo\n");
                zl = ziplistDelete(zl,&p);
            } else {
                printf("Entry: ");
                if (entry) {
                    if (elen && fwrite(entry,elen,1,stdout) == 0)
                        perror("fwrite");
                } else {
                    printf("%lld",value);
                }
                p = ziplistNext(zl,p);
                printf("\n");
            }
        }
        printf("\n");
        ziplistRepr(zl);
        zfree(zl);
    }

    printf("Regression test for >255 byte strings:\n");
    {
        char v1[257] = {0}, v2[257] = {0};
        memset(v1,'x',256);
        memset(v2,'y',256);
        zl = ziplistNew();
        zl = ziplistPush(zl,(unsigned char*)v1,strlen(v1),ZIPLIST_TAIL);
        zl = ziplistPush(zl,(unsigned char*)v2,strlen(v2),ZIPLIST_TAIL);

        /* Pop values again and compare their value. */
        p = ziplistIndex(zl,0);
        assert(ziplistGet(p,&entry,&elen,&value));
        assert(strncmp(v1,(char*)entry,elen) == 0);
        p = ziplistIndex(zl,1);
        assert(ziplistGet(p,&entry,&elen,&value));
        assert(strncmp(v2,(char*)entry,elen) == 0);
        printf("SUCCESS\n\n");
        zfree(zl);
    }

    printf("Regression test deleting next to last entries:\n");
    {
        char v[3][257] = {{0}};
        zlentry e[3] = {{.prevrawlensize = 0, .prevrawlen = 0, .lensize = 0,
                         .len = 0, .headersize = 0, .encoding = 0, .p = NULL}};
        size_t i;

        for (i = 0; i < (sizeof(v)/sizeof(v[0])); i++) {
            memset(v[i], 'a' + i, sizeof(v[0]));
        }

        v[0][256] = '\0';
        v[1][  1] = '\0';
        v[2][256] = '\0';

        zl = ziplistNew();
        for (i = 0; i < (sizeof(v)/sizeof(v[0])); i++) {
            zl = ziplistPush(zl, (unsigned char *) v[i], strlen(v[i]), ZIPLIST_TAIL);
        }

        verify(zl, e);

        assert(e[0].prevrawlensize == 1);
        assert(e[1].prevrawlensize == 5);
        assert(e[2].prevrawlensize == 1);

        /* Deleting entry 1 will increase `prevrawlensize` for entry 2 */
        unsigned char *p = e[1].p;
        zl = ziplistDelete(zl, &p);

        verify(zl, e);

        assert(e[0].prevrawlensize == 1);
        assert(e[1].prevrawlensize == 5);

        printf("SUCCESS\n\n");
        zfree(zl);
    }

    printf("Create long list and check indices:\n");
    {
        zl = ziplistNew();
        char buf[32];
        int i,len;
        for (i = 0; i < 1000; i++) {
            len = sprintf(buf,"%d",i);
            zl = ziplistPush(zl,(unsigned char*)buf,len,ZIPLIST_TAIL);
        }
        for (i = 0; i < 1000; i++) {
            p = ziplistIndex(zl,i);
            assert(ziplistGet(p,NULL,NULL,&value));
            assert(i == value);

            p = ziplistIndex(zl,-i-1);
            assert(ziplistGet(p,NULL,NULL,&value));
            assert(999-i == value);
        }
        printf("SUCCESS\n\n");
        zfree(zl);
    }

    printf("Compare strings with ziplist entries:\n");
    {
        zl = createList();
        p = ziplistIndex(zl,0);
        if (!ziplistCompare(p,(unsigned char*)"hello",5)) {
            printf("ERROR: not \"hello\"\n");
            return 1;
        }
        if (ziplistCompare(p,(unsigned char*)"hella",5)) {
            printf("ERROR: \"hella\"\n");
            return 1;
        }

        p = ziplistIndex(zl,3);
        if (!ziplistCompare(p,(unsigned char*)"1024",4)) {
            printf("ERROR: not \"1024\"\n");
            return 1;
        }
        if (ziplistCompare(p,(unsigned char*)"1025",4)) {
            printf("ERROR: \"1025\"\n");
            return 1;
        }
        printf("SUCCESS\n\n");
        zfree(zl);
    }

    printf("Merge test:\n");
    {
        /* create list gives us: [hello, foo, quux, 1024] */
        zl = createList();
        unsigned char *zl2 = createList();

        unsigned char *zl3 = ziplistNew();
        unsigned char *zl4 = ziplistNew();

        if (ziplistMerge(&zl4, &zl4)) {
            printf("ERROR: Allowed merging of one ziplist into itself.\n");
            return 1;
        }

        /* Merge two empty ziplists, get empty result back. */
        zl4 = ziplistMerge(&zl3, &zl4);
        ziplistRepr(zl4);
        if (ziplistLen(zl4)) {
            printf("ERROR: Merging two empty ziplists created entries.\n");
            return 1;
        }
        zfree(zl4);

        zl2 = ziplistMerge(&zl, &zl2);
        /* merge gives us: [hello, foo, quux, 1024, hello, foo, quux, 1024] */
        ziplistRepr(zl2);

        if (ziplistLen(zl2) != 8) {
            printf("ERROR: Merged length not 8, but: %u\n", ziplistLen(zl2));
            return 1;
        }

        p = ziplistIndex(zl2,0);
        if (!ziplistCompare(p,(unsigned char*)"hello",5)) {
            printf("ERROR: not \"hello\"\n");
            return 1;
        }
        if (ziplistCompare(p,(unsigned char*)"hella",5)) {
            printf("ERROR: \"hella\"\n");
            return 1;
        }

        p = ziplistIndex(zl2,3);
        if (!ziplistCompare(p,(unsigned char*)"1024",4)) {
            printf("ERROR: not \"1024\"\n");
            return 1;
        }
        if (ziplistCompare(p,(unsigned char*)"1025",4)) {
            printf("ERROR: \"1025\"\n");
            return 1;
        }

        p = ziplistIndex(zl2,4);
        if (!ziplistCompare(p,(unsigned char*)"hello",5)) {
            printf("ERROR: not \"hello\"\n");
            return 1;
        }
        if (ziplistCompare(p,(unsigned char*)"hella",5)) {
            printf("ERROR: \"hella\"\n");
            return 1;
        }

        p = ziplistIndex(zl2,7);
        if (!ziplistCompare(p,(unsigned char*)"1024",4)) {
            printf("ERROR: not \"1024\"\n");
            return 1;
        }
        if (ziplistCompare(p,(unsigned char*)"1025",4)) {
            printf("ERROR: \"1025\"\n");
            return 1;
        }
        printf("SUCCESS\n\n");
        zfree(zl);
    }

    printf("Stress with random payloads of different encoding:\n");
    {
        int i,j,len,where;
        unsigned char *p;
        char buf[1024];
        int buflen;
        list *ref;
        listNode *refnode;

        /* Hold temp vars from ziplist */
        unsigned char *sstr;
        unsigned int slen;
        long long sval;

        for (i = 0; i < 20000; i++) {
            zl = ziplistNew();
            ref = listCreate();
            listSetFreeMethod(ref,(void (*)(void*))sdsfree);
            len = rand() % 256;

            /* Create lists */
            for (j = 0; j < len; j++) {
                where = (rand() & 1) ? ZIPLIST_HEAD : ZIPLIST_TAIL;
                if (rand() % 2) {
                    buflen = randstring(buf,1,sizeof(buf)-1);
                } else {
                    switch(rand() % 3) {
                    case 0:
                        buflen = sprintf(buf,"%lld",(0LL + rand()) >> 20);
                        break;
                    case 1:
                        buflen = sprintf(buf,"%lld",(0LL + rand()));
                        break;
                    case 2:
                        buflen = sprintf(buf,"%lld",(0LL + rand()) << 20);
                        break;
                    default:
                        assert(NULL);
                    }
                }

                /* Add to ziplist */
                zl = ziplistPush(zl, (unsigned char*)buf, buflen, where);

                /* Add to reference list */
                if (where == ZIPLIST_HEAD) {
                    listAddNodeHead(ref,sdsnewlen(buf, buflen));
                } else if (where == ZIPLIST_TAIL) {
                    listAddNodeTail(ref,sdsnewlen(buf, buflen));
                } else {
                    assert(NULL);
                }
            }

            assert(listLength(ref) == ziplistLen(zl));
            for (j = 0; j < len; j++) {
                /* Naive way to get elements, but similar to the stresser
                 * executed from the Tcl test suite. */
                p = ziplistIndex(zl,j);
                refnode = listIndex(ref,j);

                assert(ziplistGet(p,&sstr,&slen,&sval));
                if (sstr == NULL) {
                    buflen = sprintf(buf,"%lld",sval);
                } else {
                    buflen = slen;
                    memcpy(buf,sstr,buflen);
                    buf[buflen] = '\0';
                }
                assert(memcmp(buf,listNodeValue(refnode),buflen) == 0);
            }
            zfree(zl);
            listRelease(ref);
        }
        printf("SUCCESS\n\n");
    }

    printf("Stress with variable ziplist size:\n");
    {
        stress(ZIPLIST_HEAD,100000,16384,256);
        stress(ZIPLIST_TAIL,100000,16384,256);
    }

    return 0;
}
#endif
