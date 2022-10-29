// chfs client.  implements FS operations using extent and lock server
#include "chfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

chfs_client::chfs_client()
{
    ec = new extent_client();
}

chfs_client::chfs_client(std::string extent_dst, std::string lock_dst)
{
    ec = new extent_client();
    if (ec->put(1, "") != extent_protocol::OK)
        printf("error init root dir\n"); // XYB: init root dir
}

chfs_client::inum
chfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
chfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool chfs_client::isfile(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK)
    {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_FILE)
    {
        printf("isfile: %lld is a file\n", inum);
        return true;
    }
    printf("isfile: %lld is a dir\n", inum);
    return false;
}
/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 *
 * */

bool chfs_client::isdir(inum inum)
{
    // Oops! is this still correct when you implement symlink?
    printf("=============in isdir========");
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK)
    {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_DIR)
    {
        printf("isfile: %lld is a file\n", inum);
        return true;
    }
    return false;
}

bool chfs_client::issymlink(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK)
    {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_SYMLINK)
    {
        printf("issymlink: %lld is a symlink\n", inum);
        return true;
    }
    return false;
}

int chfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK)
    {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
    return r;
}

int chfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK)
    {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
    return r;
}

#define EXT_RPC(xx)                                                \
    do                                                             \
    {                                                              \
        if ((xx) != extent_protocol::OK)                           \
        {                                                          \
            printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
            r = IOERR;                                             \
            goto release;                                          \
        }                                                          \
    } while (0)

// Only support set size of attr
int chfs_client::setattr(inum ino, size_t size)
{
    int r = OK;

    /*
     * your code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */

    std::string buf;
    extent_protocol::attr a;
    extent_protocol::status ret;
    if ((ret = ec->getattr(ino, a)) != extent_protocol::OK)
    {
        return ret;
    }
    ec->get(ino, buf);
    if (a.size < size)
        buf += std::string(size - a.size, '\0');
    else if (a.size > size)
        buf = buf.substr(0, size);
    ec->put(ino, buf);
    return r;
}

int chfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;
    printf("=============in Create========");
    /*
     * your code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    std::string buf;
    inum _inum;
    bool found;
    lookup(parent, name, found, _inum);
    if (found)
        return EXIST;
    ec->create(extent_protocol::T_FILE, ino_out);
    ec->get(parent, buf);
    buf += ("(" + std::string(name)) + "," + filename(ino_out) + ")" + "/";
    ec->put(parent, buf);
    return r;
}

int chfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    std::string buf;
    inum _inum;
    bool found;
    lookup(parent, name, found, _inum);
    if (found)
        return EXIST;
    ec->create(extent_protocol::T_DIR, ino_out);
    ec->get(parent, buf);
    buf += ("(" + std::string(name)) + "," + filename(ino_out) + ")" + "/";
    ec->put(parent, buf);
    return r;
}

int chfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    printf("=============in LoopUp========");
    int r = OK;

    /*
     * your code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */
    std::list<dirent> list;

    readdir(parent, list);

    if (list.empty())
    {
        found = false;
        return r;
    }

    for (std::list<dirent>::iterator it = list.begin(); it != list.end(); it++)
    {
        if (!strcmp(it->name.c_str(), name))
        {
            found = true;
            ino_out = it->inum;
            return r;
        }
    }
    found = false;
    return r;
}

// (name,inum)/(name,inum)
int chfs_client::readdir(inum dir, std::list<dirent> &list)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */
    std::string buf;
    if (!isdir(dir))
    {
        return EXIST;
    }

    ec->get(dir, buf);

    printf("directory_name:%s", buf.c_str());
    int start = 0;
    int end = buf.find('/');
    while (end != std::string::npos)
    {
        std::string directory = buf.substr(start, end - start); // (name,inum)/
        int npos = buf.find(',', start);
        std::string name = buf.substr(start + 1, npos - start - 1);
        std::string inum = buf.substr(npos + 1, end - npos - 2);
        struct dirent entry;
        entry.name = name;
        entry.inum = n2i(inum);
        list.emplace_back(entry);
        start = end + 1;
        end = buf.find('/', start);
    }

    return r;
}

int chfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;

    /*
     * your code goes here.
     * note: read using ec->get().
     */

    ec->get(ino, data);

    if (off <= (int)data.size())
    {
        if (off + size <= data.size())
            data = data.substr(off, size);
        else
            data = data.substr(off, data.size() - off);
    }

    return r;
}

int chfs_client::write(inum ino, size_t size, off_t off, const char *data,
                       size_t &bytes_written)
{
    printf("=============in Write========");
    int r = OK;

    /*
     * your code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */
    std::string buf;
    std::string data_buf = std::string(data, size);
    ec->get(ino, buf);
    if (size + off > buf.size())
        buf.resize(off + size, '\0');
    bytes_written = size;
    buf.replace(off, size, data_buf);
    ec->put(ino, buf);
    return r;
}

int chfs_client::unlink(inum parent, const char *name)
{

    int r = OK;

    /*
     * your code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */
    inum _inum;
    bool found;
    lookup(parent, name, found, _inum);
    ec->remove(_inum);

    std::string buf;
    ec->get(parent, buf);
    printf("======origin directory=====:%s", buf.c_str());

    int start = buf.find(name);
    int end = buf.find('/', start);
    buf.erase(start - 1, end - start + 2);
    printf("======after directory=====:%s", buf.c_str());
    ec->put(parent, buf);

    return r;
}

int chfs_client::symlink(inum parent, const char *symbol, const char *links, inum &ino_out)
{
    printf("=====in symlink function===\n");
    int r = OK;
    bool found = false;
    inum ino;
    lookup(parent,symbol,found,ino);
    if(found)
        return EXIST;

    ec->create(extent_protocol::T_SYMLINK, ino_out);
    printf("ino = %d\n",parent);
    ec->put(ino_out, std::string(links));
    std::string buf;
    ec->get(parent, buf);

    printf("buf = %s",buf);

    buf += "(" + std::string(symbol) + "," + filename(ino_out) + ")" + "/";
    ec->put(parent,buf);

    return r;
}

int chfs_client::readlink(inum ino,std::string &links){
    int r = OK;
    std::string buf;
    ec->get(ino,buf);
    links = buf;
    return r;
}