#include "inode_manager.h"

// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));
}

void disk::read_block(blockid_t id, char *buf)
{
  memcpy(buf, blocks[id], BLOCK_SIZE);
  return;
}

void disk::write_block(blockid_t id, const char *buf)
{
  memcpy(blocks[id], buf, BLOCK_SIZE);
  return;
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
  /*
   * your code goes here.
   * note: you should mark the corresponding bit in block bitmap when alloc.
   * you need to think about which block you can start to be allocated.
   */
  int start = IBLOCK(INODE_NUM, sb.nblocks);
  for (int i = start; i < BLOCK_NUM; ++i)
  {
    if (!using_blocks[i])
    {
      using_blocks[i] = 1;
      return i;
    }
  }
  return 0;
}

void block_manager::free_block(uint32_t id)
{
  /*
   * your code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */
  using_blocks[id] = 0;
  return;
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();

  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;
}

void block_manager::read_block(uint32_t id, char *buf)
{
  d->read_block(id, buf);
}

void block_manager::write_block(uint32_t id, const char *buf)
{
  d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
  bm = new block_manager();
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1)
  {
    // printf("hello!");
    printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
}

/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type)
{
  /*
   * your code goes here.
   * note: the normal inode block should begin from the 2nd inode block.
   * the 1st is used for root_dir, see inode_manager::inode_manager().
   */
  static int inum = 0;

  for (int i = 0; i < INODE_NUM; i++)
  {
    inum = (inum + 1) % INODE_NUM;
    inode_t *ino = get_inode(inum);
    if (ino == NULL)
    {
      ino = (inode_t *)malloc(sizeof(inode_t));
      bzero(ino, sizeof(inode_t));
      ino->type = type;
      ino->atime = (unsigned int)time(NULL);
      ino->mtime = (unsigned int)time(NULL);
      ino->ctime = (unsigned int)time(NULL);
      put_inode(inum, ino);
      free(ino);
      break;
    }
    else
      free(ino);
  }

  printf("alloc_inode ===> inum=%d\n", inum);
  return (inum);
}

void inode_manager::free_inode(uint32_t inum)
{
  /*
   * your code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   */
  inode_t *_ino = get_inode(inum);
  if (_ino != NULL)
  {
    _ino->type = 0;

    put_inode(inum, _ino);
    free(_ino);
  }
  return;
}

/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode *
inode_manager::get_inode(uint32_t inum)
{
  inode_t *ino;
  /*
   * your code goes here.
   */
  if (inum < 0 || inum >= INODE_NUM)
  {
    return NULL;
  }
  char buf[BLOCK_SIZE];
  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  inode_t *_inode;
  _inode = (inode_t *)buf + inum % IPB;
  if (_inode->type == 0)
  {
    return NULL;
  }
  ino = (inode_t *)malloc(sizeof(inode_t));
  *ino = *_inode;
  return ino;
}

void inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;

  printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode *)buf + inum % IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

#define MIN(a, b) ((a) < (b) ? (a) : (b))

blockid_t inode_manager::getBlk(inode_t *_ino, uint32_t n)
{
  blockid_t ID{0};
  // printf("getBlk === > n = %d\n", n);
  if (n < NDIRECT)
  {
    ID = _ino->blocks[n];
  }
  if (n >= NDIRECT && n < MAXFILE)
  {
    // INDIRECT BLK
    // printf("in");
    char buf[BLOCK_SIZE];
    bm->read_block(_ino->blocks[NDIRECT], buf);
    ID = ((blockid_t *)buf)[n - NDIRECT];

  }
  if (ID == 0)
  {
    printf("ID=%d", ID);
    exit(0);
  }
  // printf("out");
  return ID;
}
/* Get all the data of a file by inum.
 * Return alloced data, should be freed by caller. */
void inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  /*
   * your code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_out
   */
  // printf("in Read File");
  char buf[BLOCK_SIZE];
  inode_t *_ino = get_inode(inum);
  if (_ino != NULL)
  {
    *size = _ino->size;
    *buf_out = (char *)malloc(_ino->size);
    int BlkNum = *size / BLOCK_SIZE;
    for (int i = 0; i < BlkNum; ++i)
    {
      bm->read_block(getBlk(_ino, i), buf);
      memcpy(*buf_out + i * BLOCK_SIZE, buf, BLOCK_SIZE);
    }
    int remain = *size % BLOCK_SIZE;
    if (remain)
    {
      bm->read_block(getBlk(_ino, BlkNum), buf);
      memcpy(*buf_out + BlkNum * BLOCK_SIZE, buf, remain);
    }
    free(_ino);
  }
  return;
}

void inode_manager::writeBlk(inode_t *_ino, uint32_t n)
{
  if (n < NDIRECT)
  {
    _ino->blocks[n] = bm->alloc_block();
    // printf("WriteBlk ===> n = %d,%d", n, _ino->blocks[n]);
  }
  if (n >= NDIRECT && n < MAXFILE)
  {
    // create INdirect Blk
    char buf[BLOCK_SIZE];
    if (_ino->blocks[NDIRECT] == 0)
    {
      _ino->blocks[NDIRECT] = bm->alloc_block();
    }
    bm->read_block(_ino->blocks[NDIRECT], buf);
    ((blockid_t *)buf)[n - NDIRECT] = bm->alloc_block();
    bm->write_block(_ino->blocks[NDIRECT],buf);
    // printf("WriteBlk ===> n = %d,%d", n, ((blockid_t *)buf)[n - NDIRECT]);
  }
  return;
}
/* alloc/free blocks if needed */
void inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  /*
   * your code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf
   * is larger or smaller than the size of original inode
   */
  printf("in Write File\n");
  inode_t *_ino = get_inode(inum);
  if (_ino != NULL)
  {
    printf("size = %d\n", size);
    int OldBlkNum = _ino->size == 0 ? 0 : (_ino->size - 1) / BLOCK_SIZE + 1;
    int NewBlkNum = size == 0 ? 0 : (size - 1) / BLOCK_SIZE + 1;
    printf("%d\n", NewBlkNum);
    if (NewBlkNum > OldBlkNum)
    {
      for (int i = OldBlkNum; i < NewBlkNum; ++i)
      {
        writeBlk(_ino, i);
      }
    }

    else
    {
      for (int i = NewBlkNum; i < OldBlkNum; ++i)
      {
        bm->free_block(getBlk(_ino, i));
      }
    }

    int BlkNum = size / BLOCK_SIZE;
    for (int i = 0; i < BlkNum; ++i)
    {
      bm->write_block(getBlk(_ino, i), buf + i * BLOCK_SIZE);
    }
    int remain = size % BLOCK_SIZE;
    char remainbuf[BLOCK_SIZE]{0};
    if (remain)
    {
      memcpy(remainbuf, buf + BlkNum * BLOCK_SIZE, remain);
      bm->write_block(getBlk(_ino, BlkNum), remainbuf);
    }

    _ino->size = size;
    _ino->mtime = (unsigned int)time(NULL);
    _ino->ctime = (unsigned int)time(NULL);
    _ino->atime = (unsigned int)time(NULL);
    put_inode(inum, _ino);
    free(_ino);
  }

  return;
}

void inode_manager::get_attr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
  inode *_ino = get_inode(inum);
  if (_ino != NULL)
  {
    a.type = _ino->type;
    a.atime = _ino->atime;
    a.ctime = _ino->ctime;
    a.mtime = _ino->mtime;
    a.size = _ino->size;
    free(_ino);
  }
  return;
}

void inode_manager::remove_file(uint32_t inum)
{
  /*
   * your code goes here
   * note: you need to consider about both the data block and inode of the file
   */
  printf("in Remove File\n");
  inode_t *_ino = get_inode(inum);
  if (_ino != NULL)
  {
    int BlkNum = _ino->size == 0 ? 0 : (_ino->size - 1) / BLOCK_SIZE + 1;
    for (int i = 0; i < BlkNum; ++i)
    {
      bm->free_block(getBlk(_ino, i));
    }
    if (BlkNum > NDIRECT)
      bm->free_block(_ino->blocks[NDIRECT]);
    free_inode(inum);
    free(_ino);
  }

  return;
}
