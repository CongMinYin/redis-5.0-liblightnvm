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
static char *buf_global = NULL;

static int chunk_use_cnt = 0; 
static int chunk_erase_cnt = 0;

/* ------------------------- open-channel SSDs data management implementation ----------------------- */
/* ------------------------- 设计地正常些，精巧写，不想写垃圾代码，不想制造学术垃圾 ----------------------- */

/* 全局变量表示rdb文件，生存周期持续整个redis运行期间
 * 因为是redis启动的时候加载rdb文件，所以必须持久化，之前只需要一个文件名就可以，现在需要把数据结构持久化下去
 * 仅此一份，可以看成单例，但没做特殊处理
 */
static file_nvme rdb_file_nvme = {
    "rdb_file_nvme",    // 文件名字
    0                   // 文件长度
};

// AOF状态结构
static aof_io aofNvmeIo = {
    NULL,
    NULL,
    0
};

//chunk_list rdb_chunk_list = NULL;         // 每次bgsave的时候加入rdb list 有个元数据文件index记录这个了，不需要另外记录
chunk_list *aof_chunk_list_head = NULL;           // 这个是需要的每次获取chunk的时候加入链表，bgsave的时候将之前的chunk都失效，加入擦除链表
chunk_list *erase_chunk_list_head = NULL;        // 每次bgsave的时候将上一次的rdb list全部加入erase list，在loop外的那个函数去搞擦除操作，aof加载的时候加入
int *wear_level_cnt = NULL;        // 数组，malloc申请，大小为多少个chunk，pugrp和puint和

/* 全局变量表示aof文件，生存周期持续整个redis运行期间
 * 因为aof操作都在aof.c，所以所有操作都用都过rio.h的接口实现
 * 仅此一份，可以看成单例，但没做特殊处理
 */
/* 
static file_nvme aof_file_nvme = {
    "aof_file_nvme",    // 文件名字
    0,                  // 文件长度
};
*/
/* ------------------------- open-channel SSDs AOF interface  implementation ----------------------- */

/* aof不需要像rdb一样管理缓冲区，每次检查前一个chunk是否写完，写完就申请新的，没写完，就顺着前一个sector写下一个sector 
 * 现在有个问题，考虑宕机，aof元数据不能在关机推出系统再写，而是每次aof追加的时候都写入元数据到硬盘，数据写到ocssd，这样并行，但是还是两次IO
 * 如果是元数据写到数据尾部，采用类似结构的链式指针效果，可以减少1次IO，只需要存头地址就可以，这样可以作为可以说的一个点，数据安全和一致性
 * RDBFlush结束的时候，末尾写入AOF起始chunk地址，后面每次AOF的sector写入，在末尾8个字节，记录下一个sector的地址
 * 这样会加快AOF的持久化速率，可能会降低AOF的灾难恢复速率，利大于弊
 * 改了一下设计方案，每次aof持久化都持久化一次元数据太垃圾了设计了，两次IO
 * 这样设计，每个chunk的的每个sector起始8字节记录当前sector是否是AOF文件内容，chunk最后一个sector的起始8个字节记录下一个chunk的地址
 * 
 * 再修改一下，这个为最终版，改为每次像RDB一样写入最佳写入扇区，允许丢失这么多数据，1个扇区虽然丢失率低，但是性能低很多
 * 假装一下只会丢4k，假装每次写入是1～最佳写入扇区的范围，实际上就是最佳写入扇区
 * 每个最佳写入扇区的头部16个字节 记录当前的chunk信息和crc，主要用crc判断当前chunk是否有效，chunk的最后一个最佳写入扇区记录下一个chunk的信息和crc
 */
int rdbPreamble(void){
    if(rdb_file_nvme.len == 0){
        serverLog(LL_NOTICE,"start rdb preamble saving.");
        /*
        if (server.rdb_child_pid != -1) {
            serverLog(LL_NOTICE,"Background save already in progress");
            return 0;
        }*/
        rdbSaveInfo rsi, *rsiptr;
        rsiptr = rdbPopulateSaveInfo(&rsi);
        if (rdbSave(server.rdb_filename,rsiptr) != C_OK) {
            serverLog(LL_NOTICE, "bgsave preamble fail");
        }
        return 1; 
    }
    return 0;
}

//return -1 read的字数
ssize_t aofNvmeRead(char *buf, size_t len) {
    if (dev == NULL) {
        serverLog(LL_NOTICE, "aofNvmeRead:nvme dev is not opened.");
        return -1;
    }
    //检测数据是否有效
    uint64_t crc = 0;
    crc =  crc64(crc, (unsigned char *)&(((aof_sec_head *)buf)->next_read_chunk), sizeof(((aof_sec_head *)buf)->next_read_chunk));
    if (crc != ((aof_sec_head *)buf)->crc){
        serverLog(LL_NOTICE, "aof read crc is different, stop aof load");
        return 0;
    }

    ssize_t totread = 0;
	const size_t io_nsectr = nvm_dev_get_ws_opt(dev);       // 获取最佳写入扇区数
    const size_t io_nbyte = io_nsectr * geo->l.nbytes;      // 最佳写入字节数
	//struct nvm_addr chunk;                                  // 这里必须要临时变量保存一下了，因为需要先申请下一个chunk才能填入信息，再写下去
    int res = 0;

    while (len > 0) {
        // 这是多数情况，放在if里，不足最佳写入扇区的先存入buf,不足这个
        // 这里不取等号是因为让这种情况进入else，将缓冲区持久化
        if(aofNvmeIo.pos + len < io_nbyte){
            memcpy(buf, aofNvmeIo.buf + aofNvmeIo.pos, len);
            aofNvmeIo.pos += len;
            //serverLog(LL_NOTICE,"rionvmewrite to buf");
            totread += len;
            //serverLog(LL_NOTICE, "pos:%lu len:%lu", aofNvmeIo.pos, len);
            len = 0;     
        }
        // 数据切割，也可能是不切割刚刚好，反正都是要写下去的
        else {
            struct nvm_addr src[io_nsectr];	//扇区地址数组

            // 读取下一个chunk
            memcpy(buf, aofNvmeIo.buf + aofNvmeIo.pos, io_nbyte - aofNvmeIo.pos);
            len -= io_nbyte - aofNvmeIo.pos;
            buf += io_nbyte - aofNvmeIo.pos;
            totread += io_nbyte - aofNvmeIo.pos;
            // 填入这次要写的地址信息，chunk号和扇区号,如果只是写入缓冲区
		    for (size_t idx = 0; idx < io_nsectr; ++idx) {
			    src[idx] =aofNvmeIo.chunk;	
			    src[idx].l.sectr = aofNvmeIo.sectr + idx;
		    }
            res = nvm_cmd_read(dev, src, io_nsectr, aofNvmeIo.buf, NULL, NVM_CMD_SCALAR, NULL);
            serverLog(LL_NOTICE, "buf len:%lu head len:%lu aofNvmeRead:nvm_cmd_read chunk %u sec %u", io_nbyte, sizeof(aof_sec_head), src[0].l.chunk, src[0].l.sectr);
            // 重置pos
            aofNvmeIo.pos = sizeof(aof_sec_head);
            aofNvmeIo.sectr += io_nsectr;

            //检测数据是否有效
            uint64_t crc = 0;
            crc =  crc64(crc, (unsigned char *)&(((aof_sec_head *)buf)->next_read_chunk), sizeof(((aof_sec_head *)buf)->next_read_chunk));
            if (crc != ((aof_sec_head *)buf)->crc){
                serverLog(LL_NOTICE, "aof read crc is different, stop aof load");
                return 0;     //这里是否应该return 0还是totread？？？
            }
            // chunk已被读取完,申请空闲chunk
            if(aofNvmeIo.sectr == geo->l.nsectr){
                aofNvmeIo.chunk = ((aof_sec_head *)buf)->next_read_chunk;
                aofNvmeIo.sectr = 0;
	        }
        }

		if (res < 0) {
			serverLog(LL_NOTICE, "rioNvmeWrite:nvm_cmd_write error.");
			return 0;
		}
	}

    return totread;
}

ssize_t aofWriteNvme(const char *buf, size_t len) {
    if(dev == NULL){
        serverLog(LL_NOTICE, "rioNvmeWrite:nvme dev is not opened.");
        return 0;
    }

    ssize_t totwritten = 0;
	const size_t io_nsectr = nvm_dev_get_ws_opt(dev);       // 获取最佳写入扇区数
    const size_t io_nbyte = io_nsectr * geo->l.nbytes;      // 最佳写入字节数
	struct nvm_addr chunk;                                  // 这里必须要临时变量保存一下了，因为需要先申请下一个chunk才能填入信息，再写下去
    int res = 0;

    while (len > 0) { 
        // 这是多数情况，放在if里，不足最佳写入扇区的先存入buf,不足这个
        // 这里不取等号是因为让这种情况进入else，将缓冲区持久化
        if(aofNvmeIo.pos + len < io_nbyte){
            memcpy(aofNvmeIo.buf + aofNvmeIo.pos, buf, len);
            aofNvmeIo.pos += len;
            //serverLog(LL_NOTICE,"rionvmewrite to buf");
            totwritten += len;
            //serverLog(LL_NOTICE, "pos:%lu len:%lu", aofNvmeIo.pos, len);
            len = 0;     
        }
        // 数据切割，也可能是不切割刚刚好，反正都是要写下去的
        else{
            struct nvm_addr src[io_nsectr];	//扇区地址数组
            // chunk已被写满,申请空闲chunk
            if(aofNvmeIo.sectr + io_nsectr == geo->l.nsectr){
                if (nvm_cmd_rprt_arbs(dev, NVM_CHUNK_STATE_FREE, 1, &chunk)) {
		            serverLog(LL_NOTICE, "aofNvmeWrite:nvm_cmd_rprt_arbs error, get chunks error.");
		            return 0;
                }
                chunk_use_cnt++;
                aof_sec_head t;
                t.next_read_chunk = chunk;
                t.crc =  crc64(t.crc, (unsigned char *)&t.next_read_chunk, sizeof(t.next_read_chunk));
                memcpy(aofNvmeIo.buf, &t, sizeof(t));   // 替换掉原本的头部信息

                chunk_list *tmp = (chunk_list *)malloc(sizeof(chunk_list));
                tmp->chunk.pugrp = chunk.l.pugrp;
                tmp->chunk.punit = chunk.l.punit;
                tmp->chunk.chunk = chunk.l.chunk;
                tmp->chunk.erase_cnt = wear_level_cnt[tmp->chunk.pugrp*geo->l.npunit*geo->l.nchunk +  tmp->chunk.punit*geo->l.nchunk + tmp->chunk.chunk];
                tmp->next = aof_chunk_list_head ;
                aof_chunk_list_head = tmp;
	        }

            // 写入满的buf到chunk
            memcpy(aofNvmeIo.buf + aofNvmeIo.pos, buf, io_nbyte - aofNvmeIo.pos);
            len -= io_nbyte - aofNvmeIo.pos;
            buf += io_nbyte - aofNvmeIo.pos;
            totwritten += io_nbyte - aofNvmeIo.pos;
            // 填入这次要写的地址信息，chunk号和扇区号,如果只是写入缓冲区
		    for (size_t idx = 0; idx < io_nsectr; ++idx) {
			    src[idx] =aofNvmeIo.chunk;	
			    src[idx].l.sectr = aofNvmeIo.sectr + idx;
		    }
            res = nvm_cmd_write(dev, src, io_nsectr, aofNvmeIo.buf, NULL, NVM_CMD_SCALAR, NULL);
            serverLog(LL_NOTICE, "buf len:%lu head len:%lu rioNvmeWrite:nvm_cmd_write chunk %u sec %u", io_nbyte, sizeof(aof_sec_head), src[0].l.chunk, src[0].l.sectr);

            // 重置buf区
            memset(aofNvmeIo.buf + sizeof(aof_sec_head), 0, io_nbyte - sizeof(aof_sec_head));
            aofNvmeIo.pos = sizeof(aof_sec_head);
            aofNvmeIo.sectr += io_nsectr;

            // chunk已被写满,申请空闲chunk
            if(aofNvmeIo.sectr == geo->l.nsectr){
                aofNvmeIo.chunk = chunk;
                aofNvmeIo.sectr = 0;
	        }
        }

		if (res < 0) {
			serverLog(LL_NOTICE, "rioNvmeWrite:nvm_cmd_write error.");
			return 0;
		}
	}

    return totwritten;
}

void rdbChunkRecycle(void) {
    if (rdb_file_nvme.len == 0) {
        return ; 
    }
    int rdb_chunk_cnt = rdb_file_nvme.len / (geo->l.nsectr * geo->l.nbytes);
    if (rdb_file_nvme.len % (geo->l.nsectr * geo->l.nbytes)){
        rdb_chunk_cnt++;
    }

    // 将chunk收集到erase队列，一般测试只有少量chunk，1个chunk的大小16M， 4K x 4k，头部插入法
    for (int i = 0; i < rdb_chunk_cnt++; ++i) {
        chunk_list *tmp = (chunk_list *)malloc(sizeof(chunk_list));
        tmp->chunk.pugrp = rdb_file_nvme.index[i].l.pugrp;
        tmp->chunk.punit = rdb_file_nvme.index[i].l.punit;
        tmp->chunk.chunk = rdb_file_nvme.index[i].l.chunk;
        tmp->chunk.erase_cnt = wear_level_cnt[tmp->chunk.pugrp*geo->l.npunit*geo->l.nchunk +  tmp->chunk.punit*geo->l.nchunk + tmp->chunk.chunk];
        tmp->next = erase_chunk_list_head ;
        erase_chunk_list_head = tmp;
        chunk_erase_cnt++;
    }

    chunk_use_cnt -= rdb_chunk_cnt;
    chunk_erase_cnt += rdb_chunk_cnt;

    if (aof_chunk_list_head == NULL) {
        return ;
    }

    int aof_chunk_cnt = 1;
    chunk_list *t = aof_chunk_list_head;
    while(t->next != NULL) {
        t = t->next;
        aof_chunk_cnt++;
    }
    t->next = erase_chunk_list_head;
    erase_chunk_list_head = aof_chunk_list_head;
    chunk_use_cnt -= aof_chunk_cnt;
    chunk_erase_cnt += aof_chunk_cnt;
}

void eraseChunk(void) {
    if (chunk_erase_cnt == 0) {
        return ;
    }
    static int high_erase_cnt = 5;
    serverLog(LL_NOTICE, "erase chunk cnt:%d", chunk_erase_cnt);
    struct nvm_ret ret;
    int res = -1;

    // 如果待擦除个数超过20%，无差别从头到尾擦除,1s/次擦除5个chunk
    if (chunk_erase_cnt > geo->l.npugrp * geo->l.npunit * geo->l.nchunk * 0.2){
        struct nvm_addr *chunk_addrs = (struct nvm_addr *)malloc(sizeof(struct nvm_addr) * high_erase_cnt);
        chunk_list *tmp = erase_chunk_list_head;
        for (int i  = 0; i < high_erase_cnt; ++i) {
            chunk_addrs[i].val = 0;
            chunk_addrs[i].l.pugrp = tmp->chunk.pugrp;
            chunk_addrs[i].l.punit = tmp->chunk.punit;
            chunk_addrs[i].l.chunk = tmp->chunk.chunk;
            erase_chunk_list_head = erase_chunk_list_head->next;
            wear_level_cnt[tmp->chunk.pugrp*geo->l.npunit*geo->l.nchunk +  tmp->chunk.punit*geo->l.nchunk + tmp->chunk.chunk]++;
            free(tmp);
        }
  
        res = nvm_cmd_erase(dev, chunk_addrs, high_erase_cnt, NULL, NVM_CMD_SCALAR, &ret);
	    if (res < 0) {
		    perror("Erase failure");
		    return;
	    }

        free(chunk_addrs);
        chunk_erase_cnt -= high_erase_cnt;
        high_erase_cnt += 5;    //每次递增擦除chunk个数
    } else {
        // 无效未擦除块较少，每次擦除1个,找到磨损次数最低的chunk，如果有相同的，链表头部优先擦除
        chunk_list *tmp = erase_chunk_list_head;
        chunk_list *tmp_pre = erase_chunk_list_head;   
        chunk_list *min_erase_chunk = erase_chunk_list_head;
        chunk_list *min_erase_chunk_pre = erase_chunk_list_head;
        while (tmp != NULL) {
            if (min_erase_chunk->chunk.erase_cnt > tmp->chunk.erase_cnt) {
                min_erase_chunk = tmp;
                min_erase_chunk_pre = tmp_pre;
            } 
            tmp_pre = tmp;
            tmp = tmp->next;
        }

        struct nvm_addr chunk_addr;
        chunk_addr.val = 0;
        chunk_addr.l.pugrp = min_erase_chunk->chunk.pugrp;
        chunk_addr.l.punit = min_erase_chunk->chunk.punit;
        chunk_addr.l.chunk = min_erase_chunk->chunk.chunk;
        res = nvm_cmd_erase(dev, &chunk_addr, 1, NULL, NVM_CMD_SCALAR, &ret);
	    if (res < 0) {
		    perror("Erase failure");
		    return;
	    }
        min_erase_chunk_pre->next = min_erase_chunk->next;
        wear_level_cnt[min_erase_chunk->chunk.pugrp*geo->l.npunit*geo->l.nchunk +  min_erase_chunk->chunk.punit*geo->l.nchunk + min_erase_chunk->chunk.chunk]++;
        free(min_erase_chunk);

        chunk_erase_cnt--;
        high_erase_cnt = 5;     //重置高压下的擦除次数
    }

}

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
        serverLog(LL_NOTICE, "rioNvmeWrite:nvme dev is not opened.");
        return 0;
    }
    //uint64_t crc = 0;
    const char *buf = buff;
	const size_t io_nsectr = nvm_dev_get_ws_opt(dev);       // 获取最佳写入扇区数
    const size_t io_nbyte = io_nsectr * geo->l.nbytes;      // 最佳写入字节数
	int res = 0;
    
    //serverLog(LL_NOTICE,"rionvmewrite start write");
    // 这是多数情况，放在if里，不足最佳写入扇区的先存入buf
    // 这里不取等号是因为让这种情况进入else，将缓冲区持久化
    //static int test_write_num = 0;
    if(r->io.nvme.pos + len < io_nbyte){
        memcpy(r->io.nvme.buf + r->io.nvme.pos, buf, len);
        r->io.nvme.pos += len;
        //crc = crc64(crc, (unsigned char *)(r->io.nvme.buf), io_nbyte);
        //serverLog(LL_NOTICE,"rionvmewrite to buf, test_write_num = %d, pos = %lu crc = %lu", test_write_num++, r->io.nvme.pos, crc);
    }
    // 数据切割，也可能是不切割刚刚好，反正都是要写下去的
    else{
        struct nvm_addr src[io_nsectr];	//扇区地址数组
        // 写入满的buf到chunk
        memcpy(r->io.nvme.buf + r->io.nvme.pos, buf, io_nbyte - r->io.nvme.pos);
        // 填入这次要写的地址信息，chunk号和扇区号,如果只是写入缓冲区
		for (size_t idx = 0; idx < io_nsectr; ++idx) {
			src[idx] = r->io.nvme.chunk;	
			src[idx].l.sectr = r->io.nvme.sectr + idx;
		}
        res = nvm_cmd_write(dev, src, io_nsectr, r->io.nvme.buf, NULL, NVM_CMD_SCALAR, NULL);
        //r->io.nvme.file->index[r->io.nvme.file->len / io_nbyte] = src[0];   // 整写，结构赋值，将一个步长为最佳写入扇区的首地址结构赋值给索引
        
        //crc = crc64(crc, (unsigned char *)(r->io.nvme.buf), io_nbyte);
        //serverLog(LL_NOTICE, "write crc = %lu, r-buf = %lu", crc, (uint64_t)(r->io.nvme.buf));
        r->io.nvme.file->len += io_nbyte; // 记录的是文件实际长度
        // 重置buf区
        memset(r->io.nvme.buf, 0, io_nbyte);
        // 此处存在恰好写完，0拷贝情况，memcpy不出错，只是不执行
        memcpy(r->io.nvme.buf, buf + io_nbyte - r->io.nvme.pos, len - (io_nbyte - r->io.nvme.pos));
        r->io.nvme.pos = len - (io_nbyte - r->io.nvme.pos);
        r->io.nvme.sectr += io_nsectr;

        // chunk已被写满,申请空闲chunk
        if(r->io.nvme.sectr == geo->l.nsectr){
            if (nvm_cmd_rprt_arbs(dev, NVM_CHUNK_STATE_FREE, 1, &r->io.nvme.chunk)) {
		        serverLog(LL_NOTICE, "rioNvmeWrite:nvm_cmd_rprt_arbs error, get chunks error.");
		        return 0;
	        } 
            chunk_use_cnt++;
            serverLog(LL_NOTICE,"rioNvmeWrite:rioNvmeWrite down next chunk:%u %u %u %u", r->io.nvme.chunk.l.pugrp, r->io.nvme.chunk.l.punit, r->io.nvme.chunk.l.chunk, r->io.nvme.chunk.l.sectr);         // 输出信息，后期注释掉
            r->io.nvme.sectr = 0;
            r->io.nvme.file->index[(r->io.nvme.file->len+len) / geo->l.nsectr / io_nbyte] =  r->io.nvme.chunk; 
        }

		if (res < 0) {
			serverLog(LL_NOTICE, "rioNvmeWrite:nvm_cmd_write error.");
			return 0;
		}
	}
    //serverLog(LL_NOTICE, "run out of rionvmewrite.");
    return 1;
}

/* Returns 1 or 0 for success/failure. */
static size_t rioNvmeRead(rio *r, void *buff, size_t len) {
    //return fread(buf,len,1,r->io.file.fp);
    if(dev == NULL){
        serverLog(LL_NOTICE, "rioNvmeRead:nvme dev is not opened.");
        return 0;
    }
    char * buf = (char *)buff;
	const size_t io_nsectr = nvm_dev_get_ws_opt(dev);       // 获取最佳读取扇区数
    const size_t io_nbyte = io_nsectr * geo->l.nbytes;      // 最佳读取字节数
	int res = 0;

    //serverLog(LL_NOTICE,"rionvmewrite start write");
    // 这是多数情况，放在if里，不足最佳写入扇区的先存入buf
    // 这里不取等号是因为让这种情况进入else，从OCSSD读取缓冲区大小的数据
    //static int test_read_num = 0;
    if(r->io.nvme.pos + len < io_nbyte){
        memcpy(buf, r->io.nvme.buf + r->io.nvme.pos, len);
        r->io.nvme.pos += len;
        //serverLog(LL_NOTICE,"rionvmeread from buf, test_read_num = %d, pos = %lu", test_read_num++, r->io.nvme.pos);
    }
    // 数据切割，也可能是不切割刚刚好,就是if的=号情况，反正是要写下去的
    else{
        struct nvm_addr src[io_nsectr];	//扇区地址数组
        // 剩余部分先拷贝到buf
        memcpy(buf, r->io.nvme.buf + r->io.nvme.pos, io_nbyte - r->io.nvme.pos);

        // 填入这次要写的地址信息，chunk号和扇区号,如果只是写入缓冲区
		for (size_t idx = 0; idx < io_nsectr; ++idx) {
			src[idx] = r->io.nvme.chunk;	
			src[idx].l.sectr = r->io.nvme.sectr + idx;
		}
        res = nvm_cmd_read(dev, src, io_nsectr, r->io.nvme.buf, NULL, NVM_CMD_SCALAR, NULL);
        /*int i;
        for( i = 0; i < io_nbyte; i++){
            if(buf_global[i] != r->io.nvme.buf[i]){
                serverLog(LL_NOTICE, "i = %d diff out", i);
                break;
            }
        }
        if(i == io_nbyte){
            serverLog(LL_NOTICE, "data equal");
        }*/

        //serverLog(LL_NOTICE,"rioNvmeRead:rioNvmeRead read from chunk：chunk:%u %u %u %u",  src[0].l.pugrp, src[0].l.punit, src[0].l.chunk, src[0].l.sectr);         // 输出信息，后期注释掉
        //uint64_t crc = 0;
        //crc = crc64(crc, (unsigned char *)(r->io.nvme.buf), io_nbyte);
        //serverLog(LL_NOTICE, "read crc = %lu r-buf = %lu", crc, (uint64_t)(r->io.nvme.buf));

        // 此处存在恰好读完，不继续拷贝情况，memcpy不出错，只是不执行
        memcpy(buf + io_nbyte - r->io.nvme.pos, r->io.nvme.buf, len - (io_nbyte - r->io.nvme.pos));
        r->io.nvme.pos = len - (io_nbyte - r->io.nvme.pos);
        r->io.nvme.sectr += io_nsectr;

        // chunk已被读取结束，准备下一个chunk的读取工作
        if(r->io.nvme.sectr == geo->l.nsectr){
            r->io.nvme.chunk = rdb_file_nvme.index[r->processed_bytes+len / io_nbyte];  //从这里看，没必要记录每断sector步长的首地址，记录chunk首地址就可以了
            r->io.nvme.sectr = 0; 
            serverLog(LL_NOTICE,"rioNvmeRead:rioNvmeRead read from chunk：next index = %lu chunk:%u %u %u %u",  r->processed_bytes+len / io_nbyte, src[0].l.pugrp, src[0].l.punit, src[0].l.chunk, src[0].l.sectr);         // 输出信息，后期注释掉
        }
        if (res < 0) {
			serverLog(LL_NOTICE, "rioNvmeRead:nvm_cmd_read error.");
			return 0;
		}
    }
    return 1;
}

 /* 返回读写位置，在dev中应该是一个chunk的位置？？？这个函数先放着*/
static off_t rioNvmeTell(rio *r) {
    //return ftello(r->io.file.fp);
    return r->processed_bytes;
}

/* Flushes any buffer to target device if applicable. Returns 1 on success
 * and 0 on failures. */
static int rioNvmeFlush(rio *r) {
    if(dev == NULL){
        serverLog(LL_NOTICE, "rioNvmeWrite:nvme dev is not opened.");
        return 0;
    }
    const size_t io_nsectr = nvm_dev_get_ws_opt(dev);       // 获取最佳写入扇区数
    //const size_t io_nbyte = io_nsectr * geo->l.nbytes;      // 最佳写入字节数
    struct nvm_addr src[io_nsectr];	//扇区地址数组
    aof_sec_head t;
    uint64_t crc = 0;
    int res =0;

    // 缓冲区没有数据，不需要写回，如果有，也肯定不是整的最佳写入扇区的字节数，不然会在写入就被写下去
    // 不能直接返回，要将文件元数据持久化下去，改为跳转语句
    if (r->io.nvme.pos == 0){
        serverLog(LL_NOTICE, "flush buf no data, go out");
        goto OUT;
    }
    
    for (size_t idx = 0; idx < io_nsectr; ++idx) {
            src[idx] = r->io.nvme.chunk;	// 0
			src[idx].l.sectr = r->io.nvme.sectr + idx;
	}
	res = nvm_cmd_write(dev, src, io_nsectr, r->io.nvme.buf, NULL, NVM_CMD_SCALAR, NULL);
    /*uint64_t crcc = 0;
    crcc = crc64(crcc, (unsigned char *)(r->io.nvme.buf), io_nbyte);
    serverLog(LL_NOTICE, "flush write crc = %lu r-buf = %lu", crcc, (uint64_t)(r->io.nvme.buf));
    */
    r->io.nvme.sectr += io_nsectr;
    if (res < 0) {
		serverLog(LL_NOTICE, "rioNvmeFlush:nvm_cmd_write error.");
		return 0;
	}

    r->io.nvme.file->len += r->io.nvme.pos; // 记录的是文件实际长度

OUT:
    // 在init rdb的时候已经申请好了
    t.next_read_chunk = aofNvmeIo.chunk;
    t.crc =  crc64(t.crc, (unsigned char *)&(t.next_read_chunk), sizeof(t.next_read_chunk));
    memcpy(aofNvmeIo.buf, &t, sizeof(t));
    aofNvmeIo.pos += sizeof(t);

    /* 将rdbfile元数据写到磁盘上，暂时先写到普通硬盘文件上，采用crc校验是否正确
     */
    FILE *rdb_file_fp;
    rdb_file_nvme.crc =  crc64(crc, (unsigned char *)&rdb_file_nvme, sizeof(rdb_file_nvme) - 8);
    if((rdb_file_fp = fopen("rdb_meta_file", "w")) == NULL) { //w+表示可读可写，后续可以改成r，b表示二进制，之前都没有加b，就不加了，因为不是一行一行读写，所以好像都一样
        serverLog(LL_NOTICE, "rioNvmeFlush:can not open to write rdb_meta_file.");
    }
    if(fwrite(&rdb_file_nvme, sizeof(rdb_file_nvme), 1, rdb_file_fp) == 0){
        serverLog(LL_NOTICE, "rioNvmeFlush:can not write down rdb_meta_file.");
    }
    fclose(rdb_file_fp);

    // 将磨损均衡信息写到磁盘上
    /*
    FILE *wear_level_file_fp;
    rdb_file_nvme.crc =  crc64(crc, (unsigned char *)&rdb_file_nvme, sizeof(rdb_file_nvme) - 8);
    if((rdb_file_fp = fopen("rdb_meta_file", "w")) == NULL) { //w+表示可读可写，后续可以改成r，b表示二进制，之前都没有加b，就不加了，因为不是一行一行读写，所以好像都一样
        serverLog(LL_NOTICE, "rioNvmeFlush:can not open to write rdb_meta_file.");
    }
    if(fwrite(&rdb_file_nvme, sizeof(rdb_file_nvme), 1, wear_level_file_fp) == 0){
        serverLog(LL_NOTICE, "rioNvmeFlush:can not write down rdb_meta_file.");
    }
    fclose(wear_level_file_fp);
    */ 
    serverLog(LL_NOTICE, "RDB save process byte = %lu", r->processed_bytes);
       
    return 1;
}

int rdbLoadFileMeta(rio *r){
    r->io.nvme.file = &rdb_file_nvme;  //feihua 
    FILE *rdb_file_fp;
    if((rdb_file_fp = fopen("rdb_meta_file", "r")) == NULL){ 
        serverLog(LL_NOTICE, "rdbLoadFileMeta:can not open rdb_meta_file.");
        return -1;
    }
    if(fread(&rdb_file_nvme, sizeof(rdb_file_nvme), 1, rdb_file_fp) == 0){
        serverLog(LL_NOTICE, "rdbLoadFileMeta:can not read rdb_meta_file.");
        return -1;
    }

    uint64_t crc = 0;
    crc =  crc64(crc, (unsigned char *)&rdb_file_nvme, sizeof(rdb_file_nvme) - 8);
    if(crc != rdb_file_nvme.crc){
        serverLog(LL_NOTICE, "rdbLoadFileMeta:crc check is unequal.crc = %lu r.crc = %lu", crc, rdb_file_nvme.crc);
        return -1;
    }
    serverLog(LL_NOTICE,"rdbLoadFileMeta:load chunk:%u %u %u %u", rdb_file_nvme.index[0].l.pugrp, rdb_file_nvme.index[0].l.punit, rdb_file_nvme.index[0].l.chunk, rdb_file_nvme.index[0].l.sectr);         // 输出信息，后期注释掉
    fclose(rdb_file_fp);
    return 0;
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
    { {NULL, 0} }   /* union for io-specific vars */
};

void rioInitWithNvme(rio *r, int rw) {
    *r = rioNvmeIO;     // 赋值操作，前面也是，都是声明为const类型，然后进行赋值操作
    if(dev == NULL){
        dev = nvm_dev_openf(nvm_dev_path, be_id);
	    if (!dev) {
            serverLog(LL_NOTICE, "open open-channel SSDs nvm_dev fail.");
	    }
        /*else{
            serverLog(LL_NOTICE, "open open-channel SSDs nvm_dev success.");
        }*/
        geo = nvm_dev_get_geo(dev);

        if (wear_level_cnt == NULL){
            wear_level_cnt = (chunk_record *)malloc(sizeof(int) * geo->l.npugrp * geo->l.npunit * geo->l.nchunk);
            memset(wear_level_cnt, 0, sizeof(int) * geo->l.npugrp * geo->l.npunit * geo->l.nchunk);
        }
    }  
    r->io.nvme.dev = dev;    //我觉得还是应该使用全局变量，dev字段是为了多设备考虑。
  
    //这样做优点一是为了性能考虑，二是为了对齐，三是为了不用多次申请，访问，记录chunk偏移，简单。
    //buf初始化为最佳写入扇区大小
    //每次先往buf写，如果写满buf，则刷下去，直至一次rdb结束，则强制刷回buf
    const size_t io_nsectr = nvm_dev_get_ws_opt(dev);       // 获取最佳写入扇区数
    const size_t io_nbyte = io_nsectr * geo->l.nbytes;
    r->max_processing_chunk = io_nbyte;

    if(r->io.nvme.buf != NULL){
        nvm_buf_free(dev, r->io.nvme.buf);   // 不知道能不能保证结构体初始化为空或者后续有无篡改，释放后重新申请比较安全。
    }
    r->io.nvme.buf = nvm_buf_alloc(dev, io_nbyte, NULL); 
    if (!r->io.nvme.buf) {
		NVM_DEBUG("FAILED: allocating bufs");
		return ;
	}
    memset(r->io.nvme.buf, 0, io_nbyte);
    
    if(aofNvmeIo.buf != NULL){
        nvm_buf_free(dev, aofNvmeIo.buf);   // 不知道能不能保证结构体初始化为空或者后续有无篡改，释放后重新申请比较安全。
    }
    aofNvmeIo.buf = nvm_buf_alloc(dev, io_nbyte, NULL); 
    if (!aofNvmeIo.buf) {
		NVM_DEBUG("FAILED: allocating bufs");
		return ;
	}
    memset(aofNvmeIo.buf, 0, io_nbyte);
    r->io.nvme.file = &rdb_file_nvme; // 文件指向
    
    if(rw == RIO_NVME_WRITE){
        r->io.nvme.pos = 0;
        //memset(&(r->io.nvme.chunk), 0, sizeof(struct nvm_addr));
        if (nvm_cmd_rprt_arbs(dev, NVM_CHUNK_STATE_FREE, 1, &r->io.nvme.chunk)){
		    serverLog(LL_NOTICE, "nvm_cmd_rprt_arbs error, get chunks error.");
	    }
        chunk_use_cnt++;
        serverLog(LL_NOTICE,"init:init get chunk:%u %u %u %u", r->io.nvme.chunk.l.pugrp, r->io.nvme.chunk.l.punit, r->io.nvme.chunk.l.chunk, r->io.nvme.chunk.l.sectr);         // 输出信息，后期注释掉
        r->io.nvme.sectr = 0;
         memset(&rdb_file_nvme.index, 0, sizeof(rdb_file_nvme.index));    //地址数组置全部0
        rdb_file_nvme.index[0] = r->io.nvme.chunk;  // 已经申请了第一个chunk地址，赋值
        rdb_file_nvme.len = 0;
        rdb_file_nvme.crc = 0;
        if (nvm_cmd_rprt_arbs(dev, NVM_CHUNK_STATE_FREE, 1, &rdb_file_nvme.aof_chunk_head)){
		    serverLog(LL_NOTICE, "nvm_cmd_rprt_arbs error, get chunks error.");
	    }
        chunk_use_cnt++;
        chunk_list *tmp = (chunk_list *)malloc(sizeof(chunk_list));
        tmp->chunk.pugrp = rdb_file_nvme.aof_chunk_head.l.pugrp;
        tmp->chunk.punit = rdb_file_nvme.aof_chunk_head.l.punit;
        tmp->chunk.chunk = rdb_file_nvme.aof_chunk_head.l.chunk;
        tmp->chunk.erase_cnt = wear_level_cnt[tmp->chunk.pugrp*geo->l.npunit*geo->l.nchunk +  tmp->chunk.punit*geo->l.nchunk + tmp->chunk.chunk];
        tmp->next = aof_chunk_list_head ;
        aof_chunk_list_head = tmp;
    
        // 初始化AOF写状态结构，AOF接续在RDB之后
        aofNvmeIo.dev = dev;
        aofNvmeIo.pos = 0;
        aofNvmeIo.chunk = rdb_file_nvme.aof_chunk_head;
        aofNvmeIo.sectr = 0;
    }
    else if (rw == RIO_NVME_READ){
        r->io.nvme.pos = io_nbyte;
        r->io.nvme.chunk = rdb_file_nvme.index[0];
        r->io.nvme.sectr = 0;  

        // 初始化AOF读状态结构，AOF接续在RDB之后
        aofNvmeIo.pos = io_nbyte;
        aofNvmeIo.chunk = rdb_file_nvme.aof_chunk_head;
        aofNvmeIo.dev = dev;
        aofNvmeIo.sectr = 0; 
    }

    if (!buf_global){
        buf_global = nvm_buf_alloc(dev, io_nbyte, NULL);
    }
    /* 这里还应该有删除之前的rdb文件信息，擦除chunk等操作
     * 不过鉴于系统重启失效，暂时不写，看看需不需要
     */

    // 输出基本信息
    //serverLog(LL_NOTICE, "nvme ocssd optimum sector:%lu",io_nsectr);
    //serverLog(LL_NOTICE, "nvme ocssd max file len:%lu",~0UL);
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
