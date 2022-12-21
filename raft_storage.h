#ifndef raft_storage_h
#define raft_storage_h

#include "raft_protocol.h"
#include <fcntl.h>
#include <mutex>
#include <fstream>

template <typename command>
class raft_storage
{
public:
    raft_storage(const std::string &file_dir);
    ~raft_storage();
    // Lab3: Your code here,
    // presistence data
    int current_term;
    int vote_for;
    std::vector<log_entry<command>> log;

    void restore(std::vector<char> &snapshot);
    void updateLog();
    void updateMetaData();
    void updateSnapshot(const std::vector<char> &snapshot);

private:
    std::mutex mtx;
    std::string raft_path;
    std::string meta_data_path;
    std::string log_path;
    std::string snapshot_path;
};

template <typename command>
raft_storage<command>::raft_storage(const std::string &dir)
{
    raft_path = std::move(dir);
    meta_data_path = raft_path + "/metadata";
    log_path = raft_path + "/log";
    snapshot_path = raft_path + "/snapshot";
    // Lab3: Your code here
    current_term = 0;
    vote_for = -1;
}

template <typename command>
void raft_storage<command>::restore(std::vector<char> &snapshot)
{
    std::unique_lock<std::mutex> lck(mtx);
    // read meta data
    std::fstream meta_in(meta_data_path, std::ios::in | std::ios::binary);
    if (meta_in.is_open()|| meta_in.eof())
    {
        meta_in.read((char *)&current_term, sizeof(int));
        meta_in.read((char *)&vote_for, sizeof(int));
        meta_in.close();
    }
    else
    {
        return;
    }
    // read log (term,cmdsize,cmd)
    std::fstream log_in(log_path, std::ios::in | std::ios::binary);
    if (log_in.is_open() || log_in.eof())
    {
        int size;
        log_in.read((char *)&size, sizeof(int));
        int index, term, cmdsize;
        for (int i = 0; i < size; i++)
        {
            log_entry<command> entry;
            // index
            log_in.read((char *)&index, sizeof(int));
            // term
            log_in.read((char *)&term, sizeof(int));
            // cmdsize
            log_in.read((char *)&cmdsize, sizeof(int));
            // cmd
            char *buf = new char[cmdsize];
            log_in.read(buf, cmdsize);
            entry.index = index;
            entry.term = term;
            entry.cmd.deserialize(buf, cmdsize);
            log.emplace_back(entry);
            delete[] buf;
        }
        log_in.close();
    }
    else
    {
        log.clear();
    }
    // snapshot
    // std::fstream snapshot_in(snapshot_path, std::ios::in | std::ios::binary);
    // if (snapshot_in.is_open()|| snapshot_in.eof())
    // {
    //     int size;
    //     snapshot_in.read((char *)&size, sizeof(int));
    //     snapshot.resize(size);
    //     for (char &_char : snapshot)
    //     {
    //         snapshot_in.read(&_char, sizeof(char));
    //     }
    //     snapshot_in.close();
    // }
    // else{
    //     snapshot.clear();
    // }
    return;
}

template <typename command>
raft_storage<command>::~raft_storage()
{
    // Lab3: Your code here
}

template <typename command>
void raft_storage<command>::updateLog()
{
    std::unique_lock<std::mutex> lck(mtx);
    std::fstream fs(log_path, std::ios::trunc | std::ios::out | std::ios::binary);
    if (fs.is_open())
    {
        int size = log.size();
        fs.write((char *)&size, sizeof(int));
        int term, cmdsize, index;
        for (auto entry : log)
        {
            index = entry.index;
            term = entry.term;
            cmdsize = entry.cmd.size();
            char *buf = new char[cmdsize];
            entry.cmd.serialize(buf, cmdsize);
            // write term,cmdsize,cmd
            fs.write((char *)&index, sizeof(int));
            fs.write((char *)&term, sizeof(int));
            fs.write((char *)&cmdsize, sizeof(int));
            fs.write(buf, cmdsize);
            delete[] buf;
        }
        fs.close();
    }
}

template <typename command>
void raft_storage<command>::updateMetaData()
{
    std::unique_lock<std::mutex> lck(mtx);
    std::fstream fs(meta_data_path, std::ios::trunc | std::ios::out | std::ios::binary);
    if (fs.is_open())
    {
        fs.write((char *)&current_term, sizeof(int));
        fs.write((char *)&vote_for, sizeof(int));
        fs.close();
    }
}

template <typename command>
void raft_storage<command>::updateSnapshot(const std::vector<char> &snapshot)
{
    std::unique_lock<std::mutex> lck(mtx);
    std::fstream fs(snapshot_path, std::ios::trunc | std::ios::out | std::ios::binary);
    if (fs.is_open())
    {
        int size = snapshot.size();
        fs.write((char *)&size, sizeof(int));
        fs.write(snapshot.data(), size);

        fs.close();
    }
}
#endif // raft_storage_h