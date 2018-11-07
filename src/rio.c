/* rio.c is a simple stream-oriented I/O abstraction that provides an interface
 * to write code that can consume/produce data using different concrete input
 * and output devices. For instance the same rdb.c code using the rio
 * abstraction can be used to read and write the RDB format using in-memory
 * buffers or files.
 *
 * A rio object provides the following methods:
 *  read: read from stream.
 *  write: write to stream.
 *  tell: get the current offset.
 *
 * It is also possible to set a 'checksum' method that is used by rio.c in order
 * to compute a checksum of the data written or read, or to query the rio object
 * for the current checksum.
 *
 * ----------------------------------------------------------------------------
 *
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


#include "fmacros.h"
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include "rio.h"
#include "util.h"
#include "crc64.h"
#include "config.h"
#include "server.h"
#include "memory.h"

static struct nvm_dev *dev = NULL;
static const struct nvm_geo *geo = NULL;
static int be_id = NVM_BE_ANY;
static char nvm_dev_path[NVM_DEV_PATH_LEN] = "/dev/nvme0n1";

/* ------------------------- Buffer I/O implementation ----------------------- */

/* Returns 1 or 0 for success/failure. */
static size_t rioBufferWrite(rio *r, const void *buf, size_t len) {
    r->io.buffer.ptr = sdscatlen(r->io.buffer.ptr,(char*)buf,len);
    r->io.buffer.pos += len;
    return 1;
}

/* Returns 1 or 0 for success/failure. */
static size_t rioBufferRead(rio *r, void *buf, size_t len) {
    if (sdslen(r->io.buffer.ptr)-r->io.buffer.pos < len)
        return 0; /* not enough buffer to return len bytes. */
    memcpy(buf,r->io.buffer.ptr+r->io.buffer.pos,len);
    r->io.buffer.pos += len;
    return 1;
}

/* Returns read/write position in buffer. */
static off_t rioBufferTell(rio *r) {
    return r->io.buffer.pos;
}

/* Flushes any buffer to target device if applicable. Returns 1 on success
 * and 0 on failures. */
static int rioBufferFlush(rio *r) {
    UNUSED(r);
    return 1; /* Nothing to do, our write just appends to the buffer. */
}

static const rio rioBufferIO = {
    rioBufferRead,
    rioBufferWrite,
    rioBufferTell,
    rioBufferFlush,
    NULL,           /* update_checksum */
    0,              /* current checksum */
    0,              /* bytes read or written */
    0,              /* read/write chunk size */
    { { NULL, 0 } } /* union for io-specific vars */
};

void rioInitWithBuffer(rio *r, sds s) {
    *r = rioBufferIO;
    r->io.buffer.ptr = s;
    r->io.buffer.pos = 0;
}

/* --------------------- Stdio file pointer implementation ------------------- */

/* Returns 1 or 0 for success/failure. */
static size_t rioFileWrite(rio *r, const void *buf, size_t len) {
    size_t retval;

    retval = fwrite(buf,len,1,r->io.file.fp);
    r->io.file.buffered += len;

    if (r->io.file.autosync &&
        r->io.file.buffered >= r->io.file.autosync)
    {
        fflush(r->io.file.fp);
        redis_fsync(fileno(r->io.file.fp));
        r->io.file.buffered = 0;
    }
    return retval;
}

/* Returns 1 or 0 for success/failure. */
static size_t rioFileRead(rio *r, void *buf, size_t len) {
    return fread(buf,len,1,r->io.file.fp);
}

/* Returns read/write position in file. */
static off_t rioFileTell(rio *r) {
    return ftello(r->io.file.fp);
}

/* Flushes any buffer to target device if applicable. Returns 1 on success
 * and 0 on failures. */
static int rioFileFlush(rio *r) {
    return (fflush(r->io.file.fp) == 0) ? 1 : 0;
}

static const rio rioFileIO = {
    rioFileRead,
    rioFileWrite,
    rioFileTell,
    rioFileFlush,
    NULL,           /* update_checksum */
    0,              /* current checksum */
    0,              /* bytes read or written */
    0,              /* read/write chunk size */
    { { NULL, 0 } } /* union for io-specific vars */
};

void rioInitWithFile(rio *r, FILE *fp) {
    *r = rioFileIO;
    r->io.file.fp = fp;
    r->io.file.buffered = 0;
    r->io.file.autosync = 0;
}
/* --------------------- open-channel SSDs I/O implementation ------------------- */

/* Returns 1 or 0 for success/failure. */
static size_t rioNvmeWrite(rio *r, const void *buff, size_t len) {
    if(dev == NULL){
        serverLog(LL_NOTICE, "nvme dev is not opened.");
        return 0;
    }
    const char *buf = buff;
	const size_t io_nsectr = nvm_dev_get_ws_opt(dev);       // 获取最佳写入扇区数
    const size_t io_nbyte = io_nsectr * geo->l.nbytes;      // 最佳写入字节数
	//size_t bufs_nbytes = geo->l.nsectr * geo->l.nbytes;     // 一个chunk中的字节数
    //int alignment = dev->geo.sector_nbytes;                 //对齐字节数，如果不对其不知道会怎么样。
	struct nvm_addr chunk;	                    //1个chunk地址,只有在写一个chunk，重新申请chunk时用到
	int res = 0;
    
    //serverLog(LL_NOTICE,"rionvmewrite start write");
    // 循环总长为总的sector数，步长是最佳写入扇区数,充分发挥并行性，这种写入方式，同一个chunk内的地址会连续记录在文件中
    int flag = 0;   // 判断是否恰好一个整的写入数量，不是整的除法+1
    if((len % geo->l.nbytes) != 0){
        flag = 1; 
    }
    // 一般情况是只循环一次，写入缓冲区，写满后else切割写入chunk，基本不存在循环两次及以上的情况，因为都是小写
	for (size_t sectr = 0; sectr < (len / geo->l.nbytes + flag); sectr += io_nsectr) {
		// sec步长与sector的字节相乘，得到最佳写入字节数起始地址偏移，一般情况下是只循环一次是0,主要作用是控制多次循环的情况，否则没有用
		const size_t buf_ofz = sectr * geo->l.nbytes;
		struct nvm_addr src[io_nsectr];	//扇区地址数组

        // 这里逻辑有问题，必须先经过缓存，避免顺序混乱
        //if(len - buf_ofz < io_nbyte ){    // 比它大的也要经过缓存，会走数据切割那一条路，不能直接去写一整个，不然会跳过缓存的内容，导致顺序错误
            // 这是多数情况，放在if里，不足最佳写入扇区的先存入buf
            if(r->io.nvme.pos + len - buf_ofz < io_nbyte){
                memcpy(r->io.nvme.buf + r->io.nvme.pos, buf + buf_ofz, len - buf_ofz);
                r->io.nvme.pos += len - buf_ofz;
                //serverLog(LL_NOTICE,"rionvmewrite to buf");
            }
            // 数据切割，也可能是不切割刚刚好，反正是要写下去的
            else{
                // 写入满的buf到chunk
                memcpy(r->io.nvme.buf + r->io.nvme.pos, buf + buf_ofz, io_nbyte - r->io.nvme.pos);
                // 填入这次要写的地址信息，chunk号和扇区号,如果只是写入缓冲区
		        for (size_t idx = 0; idx < io_nsectr; ++idx) {
			        src[idx] = r->io.nvme.chunk;	// 0
			        src[idx].l.sectr = r->io.nvme.sectr + idx;
		        }
                res = nvm_cmd_write(dev, src, io_nsectr, r->io.nvme.buf, NULL, 0x0, NULL);

                // 更新文件信息，记录追加的地址信息和长度，先判断是否超过文件最大长度
                if((r->io.nvme.file->len + io_nbyte) / 2 > ~0UL / 2){
                    serverLog(LL_NOTICE, "rdb file bigger than 4G, error.");
                    return 0;
                }
                r->io.nvme.file->len += io_nbyte; // 记录的是文件实际长度
                r->io.nvme.file->index[r->io.nvme.file->len / io_nbyte -1] = src[0];   // 整写，结构赋值，将一个步长为最佳写入扇区的首地址结构赋值给索引
                serverLog(LL_NOTICE,"rionvmewrite write down chunk：file_index=%lu, file_len=%lu, chunk_head_sec_addr:%u %u %u %u", r->io.nvme.file->len / io_nbyte -1, \
                    r->io.nvme.file->len, src[0].l.pugrp, src[0].l.punit, src[0].l.chunk, src[0].l.sectr);         // 输出信息，后期注释掉

                // 重置buf区
                memset(r->io.nvme.buf, 0, io_nbyte);
                // 此处存在恰好写完，0拷贝情况，memcpy不出错，只是不执行
                memcpy(r->io.nvme.buf, buf + buf_ofz + io_nbyte - r->io.nvme.pos, len - (buf_ofz + io_nbyte - r->io.nvme.pos));
                r->io.nvme.pos = len - (buf_ofz + io_nbyte - r->io.nvme.pos);
                r->io.nvme.sectr += io_nsectr;

                // chunk已被写满,申请空闲chunk
                if(r->io.nvme.sectr == geo->l.nsectr){
                    if (nvm_cmd_rprt_arbs(dev, NVM_CHUNK_STATE_FREE, 1, &chunk)) {
		                serverLog(LL_NOTICE, "nvm_cmd_rprt_arbs error, get chunks error.");
		                return 0;
	                }
                    r->io.nvme.chunk = chunk;
                    r->io.nvme.sectr = 0;
                }
            }
       /* }
        // 这个if else不对，如果加了这个，如果大写入，直接没经过缓存会跳乱顺序，但是正常情况下也不会走到这个else里
        else{
            // 填入这次要写的地址信息，chunk号和扇区号,如果只是写入缓冲区
		    for (size_t idx = 0; idx < io_nsectr; ++idx) {
			    src[idx] = r->io.nvme.chunk;	// 0
			    src[idx].l.sectr = r->io.nvme.sectr + idx;
		    }
		    res = nvm_cmd_write(dev, src, io_nsectr, buf + buf_ofz, NULL, 0x0, NULL);
            r->io.nvme.sectr += io_nsectr;

            // chunk已被写满,申请空闲chunk
            if(r->io.nvme.sectr == geo->l.nsectr){
                if (nvm_cmd_rprt_arbs(dev, NVM_CHUNK_STATE_FREE, 1, &chunk)) {
		            serverLog(LL_NOTICE, "nvm_cmd_rprt_arbs error, get chunks error.");
		            return 0;
	            }
                r->io.nvme.chunk = chunk;
                r->io.nvme.sectr = 0;
            }
            serverLog(LL_NOTICE,"rionvmewrite write down chunk");
        }*/

		if (res < 0) {
			serverLog(LL_NOTICE, "nvm_cmd_write error.");
			return 0;
		}
	}
    //serverLog(LL_NOTICE, "run out of rionvmewrite.");
    return 1;
}

/* Returns 1 or 0 for success/failure. */
static size_t rioNvmeRead(rio *r, void *buf, size_t len) {
    //return fread(buf,len,1,r->io.file.fp);
    return 1;
}

 /* 返回读写位置，在dev中应该是一个chunk的位置？？？这个函数先放着*/
static off_t rioNvmeTell(rio *r) {
    //return ftello(r->io.file.fp);
    return r->io.nvme.sectr;
}

/* Flushes any buffer to target device if applicable. Returns 1 on success
 * and 0 on failures. */
static int rioNvmeFlush(rio *r) {
    // 缓冲区没有数据，不需要写回，如果有，也肯定不是整的最佳写入扇区的字节数，不然会在写入就被写下去
    if (r->io.nvme.pos == 0){
        return 1;
    }
    const size_t io_nsectr = nvm_dev_get_ws_opt(dev);       // 获取最佳写入扇区数
    const size_t io_nbyte = io_nsectr * geo->l.nbytes;      // 最佳写入字节数
    struct nvm_addr src[io_nsectr];	//扇区地址数组
    int res =0;

    for (size_t idx = 0; idx < io_nsectr; ++idx) {
			src[idx] = r->io.nvme.chunk;	// 0
			src[idx].l.sectr = r->io.nvme.sectr + idx;
	}
	res = nvm_cmd_write(dev, src, io_nsectr, r->io.nvme.buf, NULL, 0x0, NULL);
    r->io.nvme.sectr += io_nsectr;
    if (res < 0) {
		serverLog(LL_NOTICE, "nvm_cmd_write error.");
		return 0;
	}

    // 更新文件信息，记录追加的地址信息和长度，先判断是否超过文件最大长度
    if((r->io.nvme.file->len + io_nbyte) / 2 > ~0UL / 2){
        serverLog(LL_NOTICE, "rdb file bigger than 4G, error.");
        return 0;
    }
    r->io.nvme.file->len += r->io.nvme.pos; // 记录的是文件实际长度
    r->io.nvme.file->index[r->io.nvme.file->len / io_nbyte] = src[0];   // 文件末尾，数据不是整的，不需要再次判断是否有余数情况，结构赋值，将一个步长为最佳写入扇区的首地址结构赋值给索引

    // chunk已被写满,申请空闲chunk
    if(r->io.nvme.sectr == geo->l.nsectr){
        if (nvm_cmd_rprt_arbs(dev, NVM_CHUNK_STATE_FREE, 1, &r->io.nvme.chunk)) {
		    serverLog(LL_NOTICE, "nvm_cmd_rprt_arbs error, get chunks error.");
		    return 0;
	    }
        r->io.nvme.sectr = 0;
    }
    return 1;
}

static const rio rioNvmeIO = {
    rioNvmeRead,
    rioNvmeWrite,
    rioNvmeTell,
    rioNvmeFlush,
    NULL,           /* update_checksum */
    0,              /* current checksum */
    0,              /* bytes read or written */
    0,              /* read/write chunk size */
    { {NULL, 0} } /* union for io-specific vars */
};

/* 全局变量表示rdb文件，生存周期持续整个redis运行期间
 * 掉电丢失，目前尚未做持久化，后续考虑
 * 仅此一份，可以看成单例，但没做特殊处理
 */
static file_nvme rdb_file_nvme = {
    "rdb_file_nvme",    // 文件名字
    0,                  // 文件长度
};

/* 全局变量表示rdb文件，生存周期持续整个redis运行期间
 * 掉电丢失，目前尚未做持久化，后续考虑
 * 仅此一份，可以看成单例，但没做特殊处理
 */
/*
static file_nvme aof_file_nvme = {
    "aof_file_nvme",    // 文件名字
    0,                  // 文件长度
};*/

void rioInitWithNvme(rio *r) {
    *r = rioNvmeIO;     // 赋值操作，前面也是，都是声明为const类型，然后进行赋值操作
    if(dev == NULL){
        dev = nvm_dev_openf(nvm_dev_path, be_id);
	    if (!dev) {
            serverLog(LL_NOTICE, "open open-channel SSDs nvm_dev fail.");
	    }
        else{
            serverLog(LL_NOTICE, "open open-channel SSDs nvm_dev success.");
        }
    }
    geo = nvm_dev_get_geo(dev);
    r->io.nvme.dev = dev;    //我觉得还是应该使用全局变量，dev字段是为了多设备考虑。
  
    //这样做优点一是为了性能考虑，二是为了对齐，三是为了不用多次申请，访问，记录chunk偏移，简单。
    //buf初始化为最佳写入扇区大小
    //每次先往buf写，如果写满buf，则刷下去，直至一次rdb结束，则强制刷回buf
    const size_t io_nsectr = nvm_dev_get_ws_opt(dev);       // 获取最佳写入扇区数
    const size_t io_nbyte = io_nsectr * geo->l.nbytes;
    r->io.nvme.buf = nvm_buf_alloc(dev, io_nbyte, NULL);  
    memset(r->io.nvme.buf, 0, io_nbyte);
    r->io.nvme.pos = 0;
    //memset(&(r->io.nvme.chunk), 0, sizeof(struct nvm_addr));
    if (nvm_cmd_rprt_arbs(dev, NVM_CHUNK_STATE_FREE, 1, &r->io.nvme.chunk)){
		serverLog(LL_NOTICE, "nvm_cmd_rprt_arbs error, get chunks error.");
	}
    r->io.nvme.sectr = 0;
    r->io.nvme.file = &rdb_file_nvme; // 文件指向

    // 初始化文件相关
    rdb_file_nvme.len = 0;
    memset(rdb_file_nvme.index, 0, sizeof(rdb_file_nvme.index));    //地址数组置全部0
    serverLog(LL_NOTICE, "sizeof(rdb_file_nvme.index) = %lu.", sizeof(rdb_file_nvme.index));    //看是否正确，正确删

    // 输出基本信息
    serverLog(LL_NOTICE, "nvme ocssd optimum sector:%lu",io_nsectr);
    serverLog(LL_NOTICE, "nvme ocssd max file len:%lu",~0UL);
}

/* ------------------- File descriptors set implementation ------------------- */

/* Returns 1 or 0 for success/failure.
 * The function returns success as long as we are able to correctly write
 * to at least one file descriptor.
 *
 * When buf is NULL and len is 0, the function performs a flush operation
 * if there is some pending buffer, so this function is also used in order
 * to implement rioFdsetFlush(). */
static size_t rioFdsetWrite(rio *r, const void *buf, size_t len) {
    ssize_t retval;
    int j;
    unsigned char *p = (unsigned char*) buf;
    int doflush = (buf == NULL && len == 0);

    /* To start we always append to our buffer. If it gets larger than
     * a given size, we actually write to the sockets. */
    if (len) {
        r->io.fdset.buf = sdscatlen(r->io.fdset.buf,buf,len);
        len = 0; /* Prevent entering the while below if we don't flush. */
        if (sdslen(r->io.fdset.buf) > PROTO_IOBUF_LEN) doflush = 1;
    }

    if (doflush) {
        p = (unsigned char*) r->io.fdset.buf;
        len = sdslen(r->io.fdset.buf);
    }

    /* Write in little chunchs so that when there are big writes we
     * parallelize while the kernel is sending data in background to
     * the TCP socket. */
    while(len) {
        size_t count = len < 1024 ? len : 1024;
        int broken = 0;
        for (j = 0; j < r->io.fdset.numfds; j++) {
            if (r->io.fdset.state[j] != 0) {
                /* Skip FDs alraedy in error. */
                broken++;
                continue;
            }

            /* Make sure to write 'count' bytes to the socket regardless
             * of short writes. */
            size_t nwritten = 0;
            while(nwritten != count) {
                retval = write(r->io.fdset.fds[j],p+nwritten,count-nwritten);
                if (retval <= 0) {
                    /* With blocking sockets, which is the sole user of this
                     * rio target, EWOULDBLOCK is returned only because of
                     * the SO_SNDTIMEO socket option, so we translate the error
                     * into one more recognizable by the user. */
                    if (retval == -1 && errno == EWOULDBLOCK) errno = ETIMEDOUT;
                    break;
                }
                nwritten += retval;
            }

            if (nwritten != count) {
                /* Mark this FD as broken. */
                r->io.fdset.state[j] = errno;
                if (r->io.fdset.state[j] == 0) r->io.fdset.state[j] = EIO;
            }
        }
        if (broken == r->io.fdset.numfds) return 0; /* All the FDs in error. */
        p += count;
        len -= count;
        r->io.fdset.pos += count;
    }

    if (doflush) sdsclear(r->io.fdset.buf);
    return 1;
}

/* Returns 1 or 0 for success/failure. */
static size_t rioFdsetRead(rio *r, void *buf, size_t len) {
    UNUSED(r);
    UNUSED(buf);
    UNUSED(len);
    return 0; /* Error, this target does not support reading. */
}

/* Returns read/write position in file. */
static off_t rioFdsetTell(rio *r) {
    return r->io.fdset.pos;
}

/* Flushes any buffer to target device if applicable. Returns 1 on success
 * and 0 on failures. */
static int rioFdsetFlush(rio *r) {
    /* Our flush is implemented by the write method, that recognizes a
     * buffer set to NULL with a count of zero as a flush request. */
    return rioFdsetWrite(r,NULL,0);
}

static const rio rioFdsetIO = {
    rioFdsetRead,
    rioFdsetWrite,
    rioFdsetTell,
    rioFdsetFlush,
    NULL,           /* update_checksum */
    0,              /* current checksum */
    0,              /* bytes read or written */
    0,              /* read/write chunk size */
    { { NULL, 0 } } /* union for io-specific vars */
};

void rioInitWithFdset(rio *r, int *fds, int numfds) {
    int j;

    *r = rioFdsetIO;
    r->io.fdset.fds = zmalloc(sizeof(int)*numfds);
    r->io.fdset.state = zmalloc(sizeof(int)*numfds);
    memcpy(r->io.fdset.fds,fds,sizeof(int)*numfds);
    for (j = 0; j < numfds; j++) r->io.fdset.state[j] = 0;
    r->io.fdset.numfds = numfds;
    r->io.fdset.pos = 0;
    r->io.fdset.buf = sdsempty();
}

/* release the rio stream. */
void rioFreeFdset(rio *r) {
    zfree(r->io.fdset.fds);
    zfree(r->io.fdset.state);
    sdsfree(r->io.fdset.buf);
}

/* ---------------------------- Generic functions ---------------------------- */

/* This function can be installed both in memory and file streams when checksum
 * computation is needed. */
void rioGenericUpdateChecksum(rio *r, const void *buf, size_t len) {
    r->cksum = crc64(r->cksum,buf,len);
}

/* Set the file-based rio object to auto-fsync every 'bytes' file written.
 * By default this is set to zero that means no automatic file sync is
 * performed.
 *
 * This feature is useful in a few contexts since when we rely on OS write
 * buffers sometimes the OS buffers way too much, resulting in too many
 * disk I/O concentrated in very little time. When we fsync in an explicit
 * way instead the I/O pressure is more distributed across time. */
void rioSetAutoSync(rio *r, off_t bytes) {
    serverAssert(r->read == rioFileIO.read);
    r->io.file.autosync = bytes;
}

/* --------------------------- Higher level interface --------------------------
 *
 * The following higher level functions use lower level rio.c functions to help
 * generating the Redis protocol for the Append Only File. */

/* Write multi bulk count in the format: "*<count>\r\n". */
size_t rioWriteBulkCount(rio *r, char prefix, long count) {
    char cbuf[128];
    int clen;

    cbuf[0] = prefix;
    clen = 1+ll2string(cbuf+1,sizeof(cbuf)-1,count);
    cbuf[clen++] = '\r';
    cbuf[clen++] = '\n';
    if (rioWrite(r,cbuf,clen) == 0) return 0;
    return clen;
}

/* Write binary-safe string in the format: "$<count>\r\n<payload>\r\n". */
size_t rioWriteBulkString(rio *r, const char *buf, size_t len) {
    size_t nwritten;

    if ((nwritten = rioWriteBulkCount(r,'$',len)) == 0) return 0;
    if (len > 0 && rioWrite(r,buf,len) == 0) return 0;
    if (rioWrite(r,"\r\n",2) == 0) return 0;
    return nwritten+len+2;
}

/* Write a long long value in format: "$<count>\r\n<payload>\r\n". */
size_t rioWriteBulkLongLong(rio *r, long long l) {
    char lbuf[32];
    unsigned int llen;

    llen = ll2string(lbuf,sizeof(lbuf),l);
    return rioWriteBulkString(r,lbuf,llen);
}

/* Write a double value in the format: "$<count>\r\n<payload>\r\n" */
size_t rioWriteBulkDouble(rio *r, double d) {
    char dbuf[128];
    unsigned int dlen;

    dlen = snprintf(dbuf,sizeof(dbuf),"%.17g",d);
    return rioWriteBulkString(r,dbuf,dlen);
}
