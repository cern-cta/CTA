/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "MockRemoteFullFS.hpp"
#include "common/exception/Errnum.hpp"
#include <fcntl.h>

namespace cta {
  
  MockRemoteFullFS::MockRemoteFullFS() {
    char path[] = "/tmp/MockRemoteFullFS_XXXXXX";
    exception::Errnum::throwOnNull(mkdtemp(path), "In MockRemoteFullFS::MockRemoteFullFS(): failed to create the temporary directory");
    m_basePath = path;
    std::string regex = std::string("file://") + path + "(/.+)";
    m_pathRegex.reset(new castor::tape::utils::Regex(regex.c_str()));
  }
  
  std::string MockRemoteFullFS::createFullURL(const std::string& relativePath) {
    if (relativePath[0] == '/')
      return std::string ("file://") + m_basePath + relativePath;
    else 
      return std::string ("file://") + m_basePath + "/" + relativePath;
  }
  
  void MockRemoteFullFS::createFile(const std::string& URL, size_t size) {
    auto match = m_pathRegex->exec(URL);
    if (match.empty())
      throw exception::Exception("In MockRemoteFullFS::createFile(): invalid path");
    std::string path = m_basePath + "/" + match.at(1);
    // Create the file
    int randomFd = ::open("/dev/urandom", 0);
    exception::Errnum::throwOnMinusOne(randomFd, "Error opening /dev/urandom");
    int fd = ::open(path.c_str(), O_CREAT | O_EXCL | O_WRONLY);
    exception::Errnum::throwOnMinusOne(fd, "Error creating a file");
    char * buff = NULL;
    try {
      buff = new char[size];
      if(!buff) { throw exception::Exception("Failed to allocate memory to read /dev/urandom"); }
      exception::Errnum::throwOnMinusOne(read(randomFd, buff, size));
      exception::Errnum::throwOnMinusOne(write(fd, buff, size));
      //m_checksum = adler32(0L, Z_NULL, 0);
      //m_checksum = adler32(m_checksum, (const Bytef *)buff, size);
      close(randomFd);
      close(fd);
      exception::Errnum::throwOnNonZero(::chmod(path.c_str(), 0777));
      delete[] buff;
    } catch (...) {
      delete[] buff;
      unlink(path.c_str());
      throw;
    }
  }
  
  void MockRemoteFullFS::deleteFile(const std::string& URL) {
    auto match = m_pathRegex->exec(URL);
    if (match.empty())
      throw exception::Exception("In MockRemoteFullFS::deleteFile(): invalid path");
    std::string path = m_basePath + "/" + match.at(1);
    // delete the file
    unlink(path.c_str());
  }
  
  std::string MockRemoteFullFS::getFilename(const RemotePath& remoteFile) const {
    auto match = m_pathRegex->exec(remoteFile.getRaw());
    if (match.size())
      return match.at(1);
    throw exception::Exception("In MockRemoteFullFS::getFilename(): invalid path");
  }
  
  void MockRemoteFullFS::rename(const RemotePath& remoteFile, const RemotePath& newRemoteFile) {
    auto srcMatch = m_pathRegex->exec(remoteFile.getRaw());
    auto dstMatch = m_pathRegex->exec(newRemoteFile.getRaw());
    if (srcMatch.empty() || dstMatch.empty())
      throw exception::Exception("In MockRemoteFullFS::rename(): invalid path");
    std::string srcPath = m_basePath + srcMatch.at(1);
    std::string dstPath = m_basePath + dstMatch.at(1);
    exception::Errnum::throwOnNonZero(::rename(srcPath.c_str(), dstPath.c_str()));
  }
  
  std::unique_ptr<RemoteFileStatus> MockRemoteFullFS::statFile(const RemotePath& path) const {
    std::unique_ptr<RemoteFileStatus> ret (new RemoteFileStatus);
    auto pathMatch = m_pathRegex->exec(path.getRaw());
    if (pathMatch.empty())
      throw exception::Exception("In MockRemoteFullFS::statFile(): invalid path");
    std::string realPath = m_basePath + pathMatch.at(1);
    struct ::stat statBuff;
    exception::Errnum::throwOnNonZero(::stat(realPath.c_str(), &statBuff));
    ret->mode = statBuff.st_mode;
    ret->owner.uid = statBuff.st_uid;
    ret->owner.gid = statBuff.st_gid;
    ret->size = statBuff.st_size;
    return ret;
  }

  int MockRemoteFullFS::deleteFileOrDirectory(const char* fpath,
      const struct ::stat* sb, int tflag, struct ::FTW * ftwbuf) {
    switch (tflag) {
      case FTW_D:
      case FTW_DNR:
      case FTW_DP:
        rmdir(fpath);
        break;
      default:
        unlink(fpath);
        break;
    }
    return 0;
  }

  MockRemoteFullFS::~MockRemoteFullFS() throw() {
    // Delete the created directories recursively
    nftw(m_basePath.c_str(), deleteFileOrDirectory, 100, FTW_DEPTH);
  }





  
} // end of namespace cta