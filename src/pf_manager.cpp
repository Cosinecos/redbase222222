#include "pf_manager.h"
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <io.h>  // 包含_chsize_s需要的头文件
PF_Manager::PF_Manager()
{
    // pBufferMgr = new PF_BufferMgr(PF_BUFFER_SIZE);
}
PF_Manager::~PF_Manager()
{
}
RC PF_Manager::SetBufferSize(int length, ALGORITHM algor)
{
    pBufferMgr = new PF_BufferMgr(length, algor);
    return (0);
}
RC PF_Manager::CreateFile(const char *fileName, int length)
{
    int fd;
    int numBytes;
    // 计算文件总大小（1 << length 是原逻辑，确保单位正确）
    const __int64 fileSize = static_cast<__int64>(1 << length); // 用__int64适配Windows大文件

    // 1. 跨平台打开文件（Windows用_open，Linux用open）
#ifdef _WIN32
    // Windows：使用CRT的_open函数（替代POSIX的open）
    // 权限掩码：_S_IREAD | _S_IWRITE 表示读写权限
    fd = _open(fileName, _O_CREAT | _O_EXCL | _O_WRONLY, _S_IREAD | _S_IWRITE);
#else
    // Linux/Unix：使用标准open函数
    // 权限掩码：0644 表示所有者读写，其他只读
    fd = open(fileName, O_CREAT | O_EXCL | O_WRONLY, 0644);
#endif

    if (fd < 0)
        return (PF_UNIX); // 打开失败

    // 2. 跨平台调整文件大小（替换fallocate）
    int reback = 0;
#ifdef _WIN32
    // Windows：使用_chsize_s调整文件大小（替代fallocate）
    // 函数原型：errno_t _chsize_s(int fd, __int64 new_size)
    // 成功返回0，失败返回非0
    reback = _chsize_s(fd, fileSize) != 0 ? -1 : 0;
#else
    // Linux/Unix：保留fallocate
    reback = fallocate(fd, 0, 0, fileSize);
#endif

    if (reback < 0)
    {
        // 调整大小失败：关闭文件并删除
#ifdef _WIN32
        _close(fd);       // Windows关闭文件
        _unlink(fileName); // Windows删除文件
#else
        close(fd);        // Linux关闭文件
        unlink(fileName); // Linux删除文件
#endif
        return (PF_UNIX);
    }

    // 3. 写入文件头（与原逻辑一致，仅替换write为跨平台版本）
    char hdrBuf[PF_FILE_HDR_SIZE];
    memset(hdrBuf, 0, PF_FILE_HDR_SIZE);
    PF_FileHdr *hdr = (PF_FileHdr *)hdrBuf;
    hdr->firstFree = 0;
    hdr->FreeSlotPage = -1;
    hdr->numPages = (fileSize) / 4096 - 1; // 用计算好的fileSize避免重复移位

#ifdef _WIN32
    numBytes = _write(fd, hdrBuf, PF_FILE_HDR_SIZE); // Windows写入
#else
    numBytes = write(fd, hdrBuf, PF_FILE_HDR_SIZE);  // Linux写入
#endif

    if (numBytes != PF_FILE_HDR_SIZE)
    {
        // 写入头失败：关闭并删除文件
#ifdef _WIN32
        _close(fd);
        _unlink(fileName);
#else
        close(fd);
        unlink(fileName);
#endif
        return (numBytes < 0) ? PF_UNIX : PF_HDRWRITE;
    }

    // 4. 初始化块（原逻辑不变，仅确保pBufferMgr已初始化）
    char *pbuffer = nullptr;
    RC rc;
    for (long i = 0; i < hdr->numPages; i++) // 修正循环条件（原i <= numPages-1 等价于 i < numPages）
    {
        if ((rc = pBufferMgr->AllocatePage(fd, i, &pbuffer)))
        {
#ifdef _WIN32
            _close(fd);
            _unlink(fileName);
#else
            close(fd);
            unlink(fileName);
#endif
            return rc;
        }
        memset(pbuffer, 0, PF_PAGE_SIZE); // 修正：用实际页大小（原sizeof(pbuffer)是指针大小，错误）
        PF_PageHdr *pf_pageHdr = (PF_PageHdr *)pbuffer;
        pf_pageHdr->full = FALSE;
        pf_pageHdr->nextFree = (i < hdr->numPages - 1) ? (i + 1) : PF_PAGE_LIST_END; // 修正边界判断
        pf_pageHdr->slotNum = 0;
        pf_pageHdr->freeCnt = PF_PAGE_SIZE;
        pf_pageHdr->nextSlotPage = -1;
        pf_pageHdr->emptySlotNum = 0;
        pf_pageHdr->PageID = i;

        pBufferMgr->UnpinPage(fd, i);
        pBufferMgr->MarkDirty(fd, i);
    }

    // 5. 刷新并关闭文件
    pBufferMgr->FlushPages(fd);
#ifdef _WIN32
    if (_close(fd) < 0) // Windows关闭文件
#else
        if (close(fd) < 0)  // Linux关闭文件
#endif
        return (PF_UNIX);

    return (0);
}
RC PF_Manager::DestroyFile(const char *fileName)
{
    if (unlink(fileName) < 0)
        return (PF_UNIX);
    // Return ok
    return (0);
}
RC PF_Manager::OpenFile(const char *fileName, PF_FileHandle &fileHandle)
{
    int rc;
    if (fileHandle.bFileOpen)
        return (PF_FILEOPEN);
    if ((fileHandle.unixfd = open(fileName, O_RDWR)) < 0)
        return (PF_UNIX);
    int numBytes = read(fileHandle.unixfd, (char *)&fileHandle.hdr,
                        sizeof(PF_FileHdr));
    if (numBytes != sizeof(PF_FileHdr))
    {
        rc = (numBytes < 0) ? PF_UNIX : PF_HDRREAD;
        close(fileHandle.unixfd);
        fileHandle.bFileOpen = TRUE;
        return (rc);
    }
    fileHandle.bHdrChanged = FALSE;
    fileHandle.pBufferMgr = pBufferMgr;
    fileHandle.bFileOpen = TRUE;
    return (0);
}
RC PF_Manager::CloseFile(PF_FileHandle &fileHandle)
{
    RC rc;

    // Ensure fileHandle refers to open file
    if (!fileHandle.bFileOpen)
        return (PF_CLOSEDFILE);

    // Flush all buffers for this file and write out the header
    if ((rc = fileHandle.FlushPages()))
        return (rc);

    // Close the file
    if (close(fileHandle.unixfd) < 0)
        return (PF_UNIX);
    fileHandle.bFileOpen = FALSE;

    // Reset the buffer manager pointer in the file handle
    fileHandle.pBufferMgr = NULL;

    // Return ok
    return 0;
}
RC PF_Manager::ClearBuffer()
{
    return pBufferMgr->ClearBuffer();
}
RC PF_Manager::AllocateBlock(char *&buffer)
{
    return pBufferMgr->AllocateBlock(buffer);
}
RC PF_Manager::DisposeBlock(char *buffer)
{
    return pBufferMgr->DisposeBlock(buffer);
}