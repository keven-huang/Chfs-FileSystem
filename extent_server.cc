// the extent server implementation

#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "extent_server.h"
#include "persister.h"

chfs_command::txid_t extent_server::tid = 0;

extent_server::extent_server()
{
  im = new inode_manager();
  _persister = new chfs_persister("log"); // DO NOT change the dir name here
  // Your code here for Lab2A: recover data on startup
  // checkpoint
  _persister->restore_checkpoint();
  std::vector<chfs_command> logs = _persister->log_entries;
  for (auto cmd : logs)
  {
    switch (cmd.type)
    {
    case chfs_command::CMD_COMMIT:
      tid = std::max(tid, cmd.id);
      committed.emplace(cmd.id, true);
      break;
    default:
      break;
    }
  }

  // redo
  for (auto cmd : logs)
  {
    if (committed[cmd.id])
    {
      switch (cmd.type)
      {
      case chfs_command::cmd_type::CMD_CREATE:
      {
        // unsigned long long id = im->alloc_inode(cmd.inumtype);
        inode_t *ino = im->get_inode(cmd.inum);
        if (ino == NULL)
        {
          ino = (inode_t *)malloc(sizeof(inode_t));
          bzero(ino, sizeof(inode_t));
          ino->type = cmd.inumtype;
          ino->atime = (unsigned int)time(NULL);
          ino->mtime = (unsigned int)time(NULL);
          ino->ctime = (unsigned int)time(NULL);
          im->put_inode(cmd.inum, ino);
          free(ino);
          break;
        }
        else
          free(ino);
      }
      break;
      case chfs_command::cmd_type::CMD_PUT:
      {
        im->write_file(cmd.inum, cmd.New.c_str(), cmd.NewSize);
        // printf("REDO WRITE\n");
        assert(cmd.inum > 0);
      }
      break;
      case chfs_command::cmd_type::CMD_REMOVE:
        im->remove_file(cmd.inum);
        // printf("REDO REMOVE\n");
        break;
      default:
        break;
      }
    }
  }

  _persister->restore_logdata();
  printf("OUT_RESTORE\n");
  logs = _persister->log_entries;
  for (auto cmd : logs)
  {
    switch (cmd.type)
    {
    case chfs_command::CMD_COMMIT:
      tid = std::max(tid, cmd.id);
      committed.emplace(cmd.id, true);
      break;
    default:
      break;
    }
  }

  // redo
  for (auto cmd : logs)
  {
    if (committed[cmd.id])
    {
      switch (cmd.type)
      {
      case chfs_command::cmd_type::CMD_CREATE:
      {
        inode_t *ino = im->get_inode(cmd.inum);
        if (ino == NULL)
        {
          ino = (inode_t *)malloc(sizeof(inode_t));
          bzero(ino, sizeof(inode_t));
          ino->type = cmd.inumtype;
          ino->atime = (unsigned int)time(NULL);
          ino->mtime = (unsigned int)time(NULL);
          ino->ctime = (unsigned int)time(NULL);
          im->put_inode(cmd.inum, ino);
          free(ino);
          break;
        }
        else
          free(ino);
        // printf("REDO ALLOC %lld,%lld\n",cmd.inum,id);
      }
      break;
      case chfs_command::cmd_type::CMD_PUT:
      {
        im->write_file(cmd.inum, cmd.New.c_str(), cmd.NewSize);
        // printf("REDO WRITE\n");
        assert(cmd.inum > 0);
      }
      break;
      case chfs_command::cmd_type::CMD_REMOVE:
        im->remove_file(cmd.inum);
        // printf("REDO REMOVE\n");
        break;
      default:
        break;
      }
    }
  }
}

int extent_server::create(uint32_t type, extent_protocol::extentid_t &id)
{
  // alloc a new inode and return inum
  printf("extent_server: create inode\n");
  id = im->alloc_inode(type);
  chfs_command cmd(tid, chfs_command::CMD_CREATE);
  cmd.setinum(id);
  cmd.setinumtype(type);
  _persister->append_log(cmd);

  return extent_protocol::OK;
}

int extent_server::put(extent_protocol::extentid_t id, std::string buf, int &)
{
  id &= 0x7fffffff;

  const char *cbuf = buf.c_str();
  int size = buf.size();
  chfs_command cmd(tid, chfs_command::CMD_PUT);
  cmd.setinum(id);
  // Origin
  std::string old;
  int oldsize = 0;
  char *oldbuf = NULL;
  im->read_file(id, &oldbuf, &oldsize);
  if (oldsize == 0)
    old = "";
  else
  {
    old.assign(oldbuf, oldsize);
    free(oldbuf);
  }
  cmd.setOrigin(old, oldsize);
  // New
  cmd.setNew(buf, size);
  _persister->append_log(cmd);

  im->write_file(id, cbuf, size);
  return extent_protocol::OK;
}

int extent_server::get(extent_protocol::extentid_t id, std::string &buf)
{
  printf("extent_server: get %lld\n", id);

  id &= 0x7fffffff;

  int size = 0;
  char *cbuf = NULL;

  im->read_file(id, &cbuf, &size);
  if (size == 0)
    buf = "";
  else
  {
    buf.assign(cbuf, size);
    free(cbuf);
  }
  printf("extent_server_get_buf: %s", buf.c_str());
  return extent_protocol::OK;
}

int extent_server::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a)
{
  printf("extent_server: getattr %lld\n", id);

  id &= 0x7fffffff;

  extent_protocol::attr attr;
  memset(&attr, 0, sizeof(attr));
  im->get_attr(id, attr);
  a = attr;

  return extent_protocol::OK;
}

int extent_server::remove(extent_protocol::extentid_t id, int &)
{
  printf("extent_server: write %lld\n", id);

  id &= 0x7fffffff;
  chfs_command cmd(tid, chfs_command::CMD_REMOVE);
  cmd.setinum(id);
  _persister->append_log(cmd);
  im->remove_file(id);

  return extent_protocol::OK;
}

int extent_server::LOG_BEGIN()
{
  ++tid;
  _persister->append_log(chfs_command(tid, chfs_command::CMD_BEGIN));
  return extent_protocol::OK;
}

int extent_server::LOG_COMMIT()
{
  _persister->append_log(chfs_command(tid, chfs_command::CMD_COMMIT));
  return extent_protocol::OK;
}

int extent_server::restore()
{
  _persister->restore_checkpoint();
  std::vector<chfs_command> logs = _persister->log_entries;
  // redo
  for (auto cmd : logs)
  {
    switch (cmd.type)
    {
    case chfs_command::cmd_type::CMD_CREATE:
    {
      unsigned long long id = im->alloc_inode(cmd.inumtype);
    }
    break;
    case chfs_command::cmd_type::CMD_PUT:
    {
      im->write_file(cmd.id, cmd.New.c_str(), cmd.NewSize);
    }
    break;
    case chfs_command::cmd_type::CMD_REMOVE:
      im->remove_file(cmd.inum);
      break;
    default:
      break;
    }
  }
}