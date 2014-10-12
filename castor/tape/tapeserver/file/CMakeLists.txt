find_package (xrootd REQUIRED)
include_directories (${XROOTD_INCLUDE_DIR})
add_library(File
  File.cpp
  DiskFile.cpp
  Structures.cpp
  OpenSSLLocker.cpp
  ../exception/XrootCl.cpp
  ../exception/OpenSSL.cpp)
target_link_libraries (File castorrfio XrdCl cryptopp)

