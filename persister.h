#ifndef persister_h
#define persister_h

#include <sys/stat.h>
#include <fcntl.h>
#include <mutex>
#include <iostream>
#include <fstream>
#include "rpc.h"

#define MAX_LOG_SZ 131072

/*
 * Your code here for Lab2A:
 * Implement class chfs_command, you may need to add command types such as
 * 'create', 'put' here to represent different commands a transaction requires.
 *
 * Here are some tips:
 * 1. each transaction in ChFS consists of several chfs_commands.
 * 2. each transaction in ChFS MUST contain a BEGIN command and a COMMIT command.
 * 3. each chfs_commands contains transaction ID, command type, and other information.
 * 4. you can treat a chfs_command as a log entry.
 */
class chfs_command
{
public:
    typedef unsigned long long txid_t;
    enum cmd_type
    {
        CMD_BEGIN = 0,
        CMD_COMMIT,
        CMD_CREATE,
        CMD_PUT,
        CMD_REMOVE
    };

    txid_t id;
    cmd_type type;
    std::string Origin;
    size_t OriginSize;
    std::string New;
    size_t NewSize;
    unsigned long long inum;
    uint32_t inumtype;

    // constructor
    chfs_command() {}
    chfs_command(txid_t _id, cmd_type _type) : id(_id), type(_type) {}
    void setOrigin(std::string _ori, size_t _old)
    {
        Origin = _ori;
        OriginSize = _old;
    }
    void setNew(std::string _new, size_t _newsize)
    {
        New = _new;
        NewSize = _newsize;
    }
    void setinum(unsigned long long _inum)
    {
        inum = _inum;
    }
    void setinumtype(uint32_t _type)
    {
        inumtype = _type;
    }
    std::string transfer() const
    {
        std::string entry;
        switch (type)
        {
        case CMD_BEGIN:
            entry = std::to_string(id) + ":" + std::to_string(CMD_BEGIN) + ";";
            break;
        case CMD_COMMIT:
            entry = std::to_string(id) + ":" + std::to_string(CMD_COMMIT) + ";";
            break;
        case CMD_CREATE:
            entry = std::to_string(id) + ":" + std::to_string(CMD_CREATE) + ":" + std::to_string(inum) + ":" + std::to_string(inumtype) + ";";
            break;
        case CMD_PUT:
            entry = std::to_string(id) + ":" + std::to_string(CMD_PUT) + ":" + std::to_string(inum) + ":" +
                    std::to_string(NewSize) + ":" + New + ":" + std::to_string(OriginSize) + ":" + Origin + ";";
            break;
        case CMD_REMOVE:
            entry = std::to_string(id) + ":" + std::to_string(CMD_REMOVE) + ":" + std::to_string(inum) + ";";
            break;
        default:
            break;
        }
        return entry;
    }

    uint64_t size() const
    {
        uint64_t s = sizeof(cmd_type) + sizeof(txid_t);
        return s;
    }
};

/*
 * Your code here for Lab2A:
 * Implement class persister. A persister directly interacts with log files.
 * Remember it should not contain any transaction logic, its only job is to
 * persist and recover data.
 *
 * P.S. When and how to do checkpoint is up to you. Just keep your logfile size
 *      under MAX_LOG_SZ and checkpoint file size under DISK_SIZE.
 */
template <typename command>
class persister
{

public:
    persister(const std::string &file_dir);
    ~persister();

    // persist data into solid binary file
    // You may modify parameters in these functions
    void append_log(const command &log);
    void checkpoint();
    void clear();
    // restore data from solid binary file
    // You may modify parameters in these functions
    void restore_logdata();
    void restore_checkpoint();

private:
    std::mutex mtx;
    std::string file_dir;
    std::string file_path_checkpoint;
    std::string file_path_logfile;

public:
    // restored log data
    std::vector<command> log_entries;
};

template <typename command>
persister<command>::persister(const std::string &dir)
{
    // DO NOT change the file names here
    file_dir = dir;
    file_path_checkpoint = file_dir + "/checkpoint.bin";
    file_path_logfile = file_dir + "/logdata.bin";
}

template <typename command>
persister<command>::~persister()
{
    log_entries.clear();
}

template <typename command>
void persister<command>::append_log(const command &log)
{
    std::fstream fs(file_path_logfile, std::ios::binary | std::ios::out | std::ios::app);
    const std::string buf = log.transfer();
    const char *cbuf = buf.c_str();
    int size = buf.size();
    fs.write(cbuf, size);
    fs.close();
    struct stat statbuf;
    stat(file_path_logfile.c_str(), &statbuf);
    if (log.type == chfs_command::CMD_COMMIT)
    {
        checkpoint();
    }
}

template <typename command>
void persister<command>::checkpoint()
{
    std::fstream checkpoint_file(file_path_checkpoint, std::ios::binary | std::ios::out | std::ios::app);
    std::fstream log_file(file_path_logfile, std::ios::binary | std::ios::in);
    char ch;
    while (log_file.read(&ch, 1))
    {
        checkpoint_file.write(&ch, 1);
    }
    checkpoint_file.close();
    log_file.close();
    // clear
    std::fstream fsm;
    fsm.open(file_path_logfile, std::fstream::out | std::ios::trunc);
    fsm.close();
}

template <typename command>
void persister<command>::restore_logdata()
{
    // Your code here for lab2A
    std::fstream fs;
    fs.open(file_path_logfile, std::ios::in | std::ios::binary);

    if (fs.is_open())
    {
        while (!fs.eof())
        {
            bool complete = true;
            char buf[1024];
            char ch;
            std::string txid;
            while (!fs.eof() && fs.read(&ch, 1))
            {
                if (ch == ':')
                    break;
                txid += ch;
            }
            if (fs.eof())
            {
                if (ch == ':')
                    fs.write(":0;", 3);
                else
                    fs.write("0;", 2);
                break;
            }
            chfs_command::txid_t tid = atoll(txid.c_str());
            if (tid <= 0)
                complete = false;
            // printf("LOG_TXID = %s\n", txid.c_str());
            fs.read(&ch, 1);
            chfs_command::cmd_type type;
            int _type = atoi(&ch);
            type = (chfs_command::cmd_type)(atoi(&ch));
            // printf("CMD_TYPE = %d\n", type);
            chfs_command cmd(tid, type);
            switch (type)
            {
            case chfs_command::CMD_BEGIN:
            case chfs_command::CMD_COMMIT:
            {
                if (!fs.read(&ch, 1))
                {
                    fs.close();
                    fs.open(file_path_logfile, std::ios::out | std::ios::app | std::ios::binary);
                    fs.write("$", 1);
                    break;
                }
                if (ch != ';')
                {
                    complete = false;
                    break;
                }
            }
            break;
            case chfs_command::CMD_CREATE:
            {
                if (!fs.read(&ch, 1))
                {
                    complete = false;
                    fs.close();
                    fs.open(file_path_logfile, std::ios::out | std::ios::app | std::ios::binary);
                    fs.write(":1:1;", 5);
                    break;
                }
                fs.getline(buf, sizeof(buf), ':');
                if (fs.eof())
                {
                    complete = false;
                    fs.close();
                    fs.open(file_path_logfile, std::ios::out | std::ios::app | std::ios::binary);
                    fs.write("1:1;", 4);
                    break;
                }
                unsigned long long inum = (unsigned long long)(atoll(buf));
                // printf("INUM = %lld\n", inum);
                fs.getline(buf, sizeof(buf), ';');
                if (fs.eof())
                {
                    fs.close();
                    fs.open(file_path_logfile, std::ios::out | std::ios::app | std::ios::binary);
                    fs.write(";", 1);
                    break;
                }
                uint32_t _type = atoi(buf);
                // printf("inumtype = %d\n", _type);
                cmd.setinum(inum);
                cmd.setinumtype(_type);
            }
            break;
            case chfs_command::CMD_PUT:
            {
                int cnt;
                if (!fs.read(&ch, 1))
                {
                    fs.close();
                    fs.open(file_path_logfile, std::ios::out | std::ios::app | std::ios::binary);
                    fs.write("$", 1);
                    break;
                }
                if (ch != ':')
                {
                    complete = false;
                    break;
                }
                fs.getline(buf, sizeof(buf), ':');
                if (fs.eof())
                {
                    fs.close();
                    fs.open(file_path_logfile, std::ios::out | std::ios::app | std::ios::binary);
                    fs.write("0:0::0:;", 8);
                    complete = false;
                    break;
                }
                unsigned long long inum = (unsigned long long)(atoll(buf));
                // printf("INUM = %lld\n", inum);
                cmd.setinum(inum);
                // new
                cnt = fs.getline(buf, sizeof(buf), ':').gcount();
                if (fs.eof())
                {
                    fs.close();
                    fs.open(file_path_logfile, std::ios::out | std::ios::app | std::ios::binary);
                    fs.write("$:", 2);
                    complete = false;
                    break;
                }
                if (buf[cnt - 2] == '$')
                {
                    printf("count = %d\n", cnt);
                    printf("FALSE\n");
                    complete = false;
                    break;
                }
                size_t newsize = atol(buf);
                printf("NEWSIZE = %ld\n", newsize);
                char *newbuf = new char[newsize + 1];
                cnt = fs.read(newbuf, newsize).gcount();
                if (fs.eof())
                {
                    printf("IN\n");
                    std::string bufstr = std::string(newsize - cnt, '\0');
                    complete = false;
                    fs.close();
                    fs.open(file_path_logfile, std::ios::out | std::ios::app | std::ios::binary);
                    fs.write(bufstr.c_str(), newsize - cnt);
                    fs.write(":0:;", 4);
                    break;
                }
                newbuf[newsize] = '\0';
                fs.read(&ch, 1);
                if (ch != ':')
                {
                    complete = false;
                    break;
                }
                std::string newstr = std::string(newbuf, newsize);
                cmd.setNew(newstr, newsize);
                // printf("NEWBUF = %s\n", newstr.c_str());
                delete newbuf;
                // origin
                cnt = fs.getline(buf, sizeof(buf), ':').gcount();
                if (fs.eof())
                {
                    fs.close();
                    fs.open(file_path_logfile, std::ios::out | std::ios::app | std::ios::binary);
                    fs.write("$:", 2);
                    complete = false;
                    break;
                }
                if (buf[cnt - 2] == '$')
                {
                    printf("count = %d", cnt);
                    printf("FALSE\n");
                    complete = false;
                    break;
                }
                size_t oldsize = atol(buf);
                // printf("OLDSIZE = %ld\n", oldsize);
                if (oldsize != 0)
                {
                    char *oldbuf = new char[oldsize + 1];
                    cnt = fs.read(oldbuf, oldsize).gcount();
                    if (fs.eof())
                    {
                        printf("IN\n");
                        std::string bufstr = std::string(oldsize - cnt, 'A');
                        fs.close();
                        fs.open(file_path_logfile, std::ios::out | std::ios::app | std::ios::binary);
                        fs.write(bufstr.c_str(), oldsize - cnt);
                        fs.write(";", 1);
                        complete = false;
                        break;
                    }
                    oldbuf[oldsize] = '\0';
                    std::string oldstr = std::string(oldbuf, oldsize);
                    cmd.setOrigin(oldstr, oldsize);
                    // printf("OLDBUF = %s\n", oldbuf);
                    delete oldbuf;
                }
                else
                    cmd.setOrigin("", oldsize);
                if (!fs.read(&ch, 1))
                {
                    fs.close();
                    fs.open(file_path_logfile, std::ios::out | std::ios::app | std::ios::binary);
                    fs.write("$", 1);
                    break;
                }
                if (ch != ';')
                {
                    printf("%c\n", ch);
                    complete = false;
                }
            }
            break;
            case chfs_command::CMD_REMOVE:
            {
                if (!fs.read(&ch, 1))
                {
                    fs.close();
                    fs.open(file_path_logfile, std::ios::out | std::ios::app | std::ios::binary);
                    fs.write("$", 1);
                    break;
                }
                if (ch != ':')
                {
                    break;
                }
                fs.getline(buf, sizeof(buf), ';');
                if (fs.eof())
                {
                    fs.close();
                    fs.open(file_path_logfile, std::ios::out | std::ios::app | std::ios::binary);
                    fs.write(";", 1);
                    break;
                }
                unsigned long long inum = (unsigned long long)atoll(buf);
                // printf("INUM = %lld\n", inum);
                cmd.setinum(inum);
            }
            break;
            default:
                break;
            }

            if (complete)
            {
                printf("ADD COMMAND\n");
                log_entries.emplace_back(cmd);
            }
            else
            {
                printf("ADD FAILED\n");
                continue;
            }
        }
    }

    fs.close();
};

template <typename command>
void persister<command>::clear()
{
    std::fstream fsm;
    fsm.open(file_path_logfile, std::fstream::out | std::ios::trunc);
    fsm.close();
}

template <typename command>
void persister<command>::restore_checkpoint()
{
    std::fstream fs;
    fs.open(file_path_checkpoint, std::ios::in | std::ios::binary);

    if (fs.is_open())
    {
        while (!fs.eof())
        {
            bool complete = true;
            char buf[1024];
            char ch;
            std::string txid;
            while (!fs.eof() && fs.read(&ch, 1))
            {
                if (ch == ':')
                    break;
                txid += ch;
            }
            if (fs.eof())
            {
                if (ch == ':')
                    fs.write(":0;", 3);
                else
                    fs.write("0;", 2);
                break;
            }
            chfs_command::txid_t tid = atoll(txid.c_str());
            if (tid <= 0)
                complete = false;
            // printf("LOG_TXID = %s\n", txid.c_str());
            fs.read(&ch, 1);
            chfs_command::cmd_type type;
            int _type = atoi(&ch);
            type = (chfs_command::cmd_type)(atoi(&ch));
            // printf("CMD_TYPE = %d\n", type);
            chfs_command cmd(tid, type);
            switch (type)
            {
            case chfs_command::CMD_BEGIN:
            case chfs_command::CMD_COMMIT:
            {
                if (!fs.read(&ch, 1))
                {
                    fs.close();
                    fs.open(file_path_checkpoint, std::ios::out | std::ios::app | std::ios::binary);
                    fs.write("$", 1);
                    break;
                }
                if (ch != ';')
                {
                    complete = false;
                    break;
                }
            }
            break;
            case chfs_command::CMD_CREATE:
            {
                if (!fs.read(&ch, 1))
                {
                    complete = false;
                    fs.close();
                    fs.open(file_path_checkpoint, std::ios::out | std::ios::app | std::ios::binary);
                    fs.write(":1:1;", 5);
                    break;
                }
                fs.getline(buf, sizeof(buf), ':');
                if (fs.eof())
                {
                    complete = false;
                    fs.close();
                    fs.open(file_path_checkpoint, std::ios::out | std::ios::app | std::ios::binary);
                    fs.write("1:1;", 4);
                    break;
                }
                unsigned long long inum = (unsigned long long)(atoll(buf));
                // printf("INUM = %lld\n", inum);
                fs.getline(buf, sizeof(buf), ';');
                if (fs.eof())
                {
                    fs.close();
                    fs.open(file_path_checkpoint, std::ios::out | std::ios::app | std::ios::binary);
                    fs.write(";", 1);
                    break;
                }
                uint32_t _type = atoi(buf);
                // printf("inumtype = %d\n", _type);
                cmd.setinum(inum);
                cmd.setinumtype(_type);
            }
            break;
            case chfs_command::CMD_PUT:
            {
                int cnt;
                if (!fs.read(&ch, 1))
                {
                    fs.close();
                    fs.open(file_path_checkpoint, std::ios::out | std::ios::app | std::ios::binary);
                    fs.write("$", 1);
                    break;
                }
                if (ch != ':')
                {
                    complete = false;
                    break;
                }
                fs.getline(buf, sizeof(buf), ':');
                if (fs.eof())
                {
                    fs.close();
                    fs.open(file_path_checkpoint, std::ios::out | std::ios::app | std::ios::binary);
                    fs.write("0:0::0:;", 8);
                    complete = false;
                    break;
                }
                unsigned long long inum = (unsigned long long)(atoll(buf));
                // printf("INUM = %lld\n", inum);
                cmd.setinum(inum);
                // new
                cnt = fs.getline(buf, sizeof(buf), ':').gcount();
                if (fs.eof())
                {
                    fs.close();
                    fs.open(file_path_checkpoint, std::ios::out | std::ios::app | std::ios::binary);
                    fs.write("$:", 2);
                    complete = false;
                    break;
                }
                if (buf[cnt - 2] == '$')
                {
                    printf("count = %d\n", cnt);
                    printf("FALSE\n");
                    complete = false;
                    break;
                }
                size_t newsize = atol(buf);
                printf("NEWSIZE = %ld\n", newsize);
                char *newbuf = new char[newsize + 1];
                cnt = fs.read(newbuf, newsize).gcount();
                if (fs.eof())
                {
                    printf("IN\n");
                    std::string bufstr = std::string(newsize - cnt, '\0');
                    complete = false;
                    fs.close();
                    fs.open(file_path_checkpoint, std::ios::out | std::ios::app | std::ios::binary);
                    fs.write(bufstr.c_str(), newsize - cnt);
                    fs.write(":0:;", 4);
                    break;
                }
                newbuf[newsize] = '\0';
                fs.read(&ch, 1);
                if (ch != ':')
                {
                    complete = false;
                    break;
                }
                std::string newstr = std::string(newbuf, newsize);
                cmd.setNew(newstr, newsize);
                // printf("NEWBUF = %s\n", newstr.c_str());
                delete newbuf;
                // origin
                cnt = fs.getline(buf, sizeof(buf), ':').gcount();
                if (fs.eof())
                {
                    fs.close();
                    fs.open(file_path_checkpoint, std::ios::out | std::ios::app | std::ios::binary);
                    fs.write("$:", 2);
                    complete = false;
                    break;
                }
                if (buf[cnt - 2] == '$')
                {
                    printf("count = %d", cnt);
                    printf("FALSE\n");
                    complete = false;
                    break;
                }
                size_t oldsize = atol(buf);
                // printf("OLDSIZE = %ld\n", oldsize);
                if (oldsize != 0)
                {
                    char *oldbuf = new char[oldsize + 1];
                    cnt = fs.read(oldbuf, oldsize).gcount();
                    if (fs.eof())
                    {
                        printf("IN\n");
                        std::string bufstr = std::string(oldsize - cnt, 'A');
                        fs.close();
                        fs.open(file_path_checkpoint, std::ios::out | std::ios::app | std::ios::binary);
                        fs.write(bufstr.c_str(), oldsize - cnt);
                        fs.write(";", 1);
                        complete = false;
                        break;
                    }
                    oldbuf[oldsize] = '\0';
                    std::string oldstr = std::string(oldbuf, oldsize);
                    cmd.setOrigin(oldstr, oldsize);
                    // printf("OLDBUF = %s\n", oldbuf);
                    delete oldbuf;
                }
                else
                    cmd.setOrigin("", oldsize);
                if (!fs.read(&ch, 1))
                {
                    fs.close();
                    fs.open(file_path_checkpoint, std::ios::out | std::ios::app | std::ios::binary);
                    fs.write("$", 1);
                    break;
                }
                if (ch != ';')
                {
                    printf("%c\n", ch);
                    complete = false;
                }
            }
            break;
            case chfs_command::CMD_REMOVE:
            {
                if (!fs.read(&ch, 1))
                {
                    fs.close();
                    fs.open(file_path_checkpoint, std::ios::out | std::ios::app | std::ios::binary);
                    fs.write("$", 1);
                    break;
                }
                if (ch != ':')
                {
                    break;
                }
                fs.getline(buf, sizeof(buf), ';');
                if (fs.eof())
                {
                    fs.close();
                    fs.open(file_path_checkpoint, std::ios::out | std::ios::app | std::ios::binary);
                    fs.write(";", 1);
                    break;
                }
                unsigned long long inum = (unsigned long long)atoll(buf);
                // printf("INUM = %lld\n", inum);
                cmd.setinum(inum);
            }
            break;
            default:
                break;
            }

            if (complete)
            {
                printf("ADD COMMAND\n");
                log_entries.emplace_back(cmd);
            }
            else
            {
                printf("ADD FAILED\n");
                continue;
            }
        }
    }

    fs.close();
};

using chfs_persister = persister<chfs_command>;

#endif // persister_h