find_package (xrootd REQUIRED)
include_directories (${XROOTD_INCLUDE_DIR})
add_library(File
  File.cpp
  DiskFile.cpp
  Structures.cpp
  ../exception/XrootCl.cpp)
target_link_libraries (File castorrfio XrdCl cryptopp)

