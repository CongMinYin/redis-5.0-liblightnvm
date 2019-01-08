/*
 * Copyright (c) 2009-2012, Pieter Noordhuis <pcnoordhuis at gmail dot com>
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
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


#ifndef __REDIS_RIO_H
#define __REDIS_RIO_H

#include <stdio.h>
#include <stdint.h>
#include <liblightnvm.h>
#include <liblightnvm_spec.h>
#include "sds.h"

#define RIO_NVME_WRITE 1
#define RIO_NVME_READ 2

//1048576=1024x1024  1M
#define INDEX_CHUNKN 256 

struct _chunk_record {
    uint64_t erase_cnt	: 32;	///chunk erase count
	uint64_t chunk	: 16;	///< Chunk in PU
	uint64_t punit	: 8;	///< Parallel Unit (PU) in PUG
	uint64_t pugrp	: 8;	///< Parallel Unit Group (PUG) 
};

typedef struct _chunk_record chunk_record;

struct _chunk_list {
    chunk_record chunk;
    struct _chunk_list *next;
};

typedef struct _chunk_list chunk_list;

struct _file_nvme {
    char filename[16];
    size_t len;                     
    struct nvm_addr index[INDEX_CHUNKN];     //先用数组试试，不行就用malloc,记录每个chunk的地址就可以了，不需要记录sector，由长度控制
    struct nvm_addr aof_chunk_head;         //RDB持久化之后进入aof增量式保存形式，保存的是首个chunk地址，后续在每个sector记录信息
    uint64_t crc;
};

typedef struct _file_nvme file_nvme;

struct _aof_io {
    struct nvm_dev *dev;        // nvme设备
    char *buf;                  // 缓存区，读写共用（因为不可能同时读写，仅在程序启动时读取），大小为最佳写入扇区大小
    off_t pos;                  // 缓冲区目前写入偏移
    struct nvm_addr chunk;      // 正在被写入的chunk
    size_t sectr;               // 下一个扇区
};

typedef struct _aof_io aof_io;

// 不能手算这个结构长度，一定要用sizeof
struct _aof_sec_head {  
    struct nvm_addr next_read_chunk;    //下一个read chunk
    uint64_t crc;                       //CRC校验主要用来判断当前sector是否有效
};

typedef struct _aof_sec_head aof_sec_head;

struct _rio {
    /* Backend functions.
     * Since this functions do not tolerate short writes or reads the return
     * value is simplified to: zero on error, non zero on complete success. */
    size_t (*read)(struct _rio *, void *buf, size_t len);
    size_t (*write)(struct _rio *, const void *buf, size_t len);
    off_t (*tell)(struct _rio *);
    int (*flush)(struct _rio *);
    /* The update_cksum method if not NULL is used to compute the checksum of
     * all the data that was read or written so far. The method should be
     * designed so that can be called with the current checksum, and the buf
     * and len fields pointing to the new block of data to add to the checksum
     * computation. */
    void (*update_cksum)(struct _rio *, const void *buf, size_t len);

    /* The current checksum */
    uint64_t cksum;

    /* number of bytes read or written */
    size_t processed_bytes;

    /* maximum single read or write chunk size */
    size_t max_processing_chunk;

    /* Backend-specific vars. */
    union {
        /* In-memory buffer target. */
        struct {
            sds ptr;
            off_t pos;
        } buffer;
        /* Stdio file pointer target. */
        struct {
            FILE *fp;
            off_t buffered; /* Bytes written since last fsync. */
            off_t autosync; /* fsync after 'autosync' bytes written. */
        } file;
        /* Multiple FDs target (used to write to N sockets). */
        struct {
            int *fds;       /* File descriptors. */
            int *state;     /* Error state of each fd. 0 (if ok) or errno. */
            int numfds;
            off_t pos;
            sds buf;
        } fdset;
        /* liblightnvm读写对象 */
        struct {
            struct nvm_dev *dev;        // nvme设备
            char *buf;                  // 缓存区，读写共用（因为不可能同时读写，仅在程序启动时读取），大小为最佳写入扇区大小
            off_t pos;                  // 缓冲区目前写入偏移
            struct nvm_addr chunk;      // 正在被写入的chunk
            size_t sectr;               // 下一个写的扇区
            file_nvme *file;            // 指向正在写的文件
        }nvme;
    } io;
};

typedef struct _rio rio;

/* The following functions are our interface with the stream. They'll call the
 * actual implementation of read / write / tell, and will update the checksum
 * if needed. */

static inline size_t rioWrite(rio *r, const void *buf, size_t len) {
    while (len) {
        //写的字节长度，不能超过每次写的最大字节长度
        size_t bytes_to_write = (r->max_processing_chunk && r->max_processing_chunk < len) ? r->max_processing_chunk : len;
        if (r->update_cksum) r->update_cksum(r,buf,bytes_to_write);
        if (r->write(r,buf,bytes_to_write) == 0)    // 调用自身write方法
            return 0;
        buf = (char*)buf + bytes_to_write;          // 更新偏移量，指向下一个读的位置
        len -= bytes_to_write;                      // 计算剩余写入长度
        r->processed_bytes += bytes_to_write;       // 更新写入长度
    }
    return 1;
}

static inline size_t rioRead(rio *r, void *buf, size_t len) {
    while (len) {
        //读的字节长度，不能超过每次读的最大字节长度
        size_t bytes_to_read = (r->max_processing_chunk && r->max_processing_chunk < len) ? r->max_processing_chunk : len;
        if (r->read(r,buf,bytes_to_read) == 0)     // 调用自身read方法
            return 0;
        if (r->update_cksum) r->update_cksum(r,buf,bytes_to_read);
        buf = (char*)buf + bytes_to_read;           // 更新偏移量，指向下一个读的位置
        len -= bytes_to_read;                       // 计算剩余读入长度
        r->processed_bytes += bytes_to_read;        // 更新读取长度
    }
    return 1;
}

static inline off_t rioTell(rio *r) {
    return r->tell(r);
}

static inline int rioFlush(rio *r) {
    return r->flush(r);
}

void rioInitWithFile(rio *r, FILE *fp);
void rioInitWithBuffer(rio *r, sds s);
void rioInitWithFdset(rio *r, int *fds, int numfds);
void rioInitWithNvme(rio *r, int rw);

int rdbLoadFileMeta(rio *r); 
ssize_t aofWriteNvme(const char *buf, size_t len);
ssize_t aofNvmeRead(char *buf, size_t len);
int rdbPreamble(void);
void rdbChunkRecycle(void);
void eraseChunk(void);

void rioFreeFdset(rio *r);

size_t rioWriteBulkCount(rio *r, char prefix, long count);
size_t rioWriteBulkString(rio *r, const char *buf, size_t len);
size_t rioWriteBulkLongLong(rio *r, long long l);
size_t rioWriteBulkDouble(rio *r, double d);

struct redisObject;
int rioWriteBulkObject(rio *r, struct redisObject *obj);

void rioGenericUpdateChecksum(rio *r, const void *buf, size_t len);
void rioSetAutoSync(rio *r, off_t bytes);

#endif
