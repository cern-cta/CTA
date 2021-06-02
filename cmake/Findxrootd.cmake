# - Find xroot
# Finds the header files of xrootd-devel by searching for XrdVersion.hh
# Finds the header files of xrootd-private-devel by searching for XrdOssApi.hh
#
# XROOTD_FOUND               - true if xrootd has been found
# XROOTD_INCLUDE_DIR         - location of the xrootd-devel header files
# XROOTD_PRIVATE_INCLUDE_DIR - location of the private xrootd files, in other
#                              words the header files that do not contribute to
#                              the xrootd ABI.
# XROOTD_XRDCL_LIB           - location of the XrdCl library
# XROOTD_XRDPOSIX_LIB        - location of the XrdPosix library
# XROOTD_XRDSERVER_LIB       - location of the XrdServer library
# XROOTD_XRDUTILS_LIB        - location of the XrdUtils library

# Be silent if XROOTD_INCLUDE_DIR is already cached
if (XROOTD_INCLUDE_DIR)
  set(XROOTD_FIND_QUIETLY TRUE)
endif (XROOTD_INCLUDE_DIR)

find_path (XROOTD_INCLUDE_DIR XrdVersion.hh
  PATH_SUFFIXES include/xrootd
)

find_path (XROOTD_PRIVATE_INCLUDE_DIR XrdOss/XrdOssApi.hh
  PATH_SUFFIXES include/xrootd/private
)

find_library (XROOTD_XRDCL_LIB XrdCl)
find_library (XROOTD_XRDPOSIX_LIB XrdPosixPreload)
find_library (XROOTD_XRDSERVER_LIB XrdServer)
find_library (XROOTD_XRDSSI_LIB NAMES XrdSsi-4 XrdSsi-5)
find_library (XROOTD_XRDUTILS_LIB XrdUtils)

message (STATUS "XROOTD_INCLUDE_DIR         = ${XROOTD_INCLUDE_DIR}")
message (STATUS "XROOTD_PRIVATE_INCLUDE_DIR = ${XROOTD_PRIVATE_INCLUDE_DIR}")
message (STATUS "XROOTD_XRDCL_LIB           = ${XROOTD_XRDCL_LIB}")
message (STATUS "XROOTD_XRDPOSIX_LIB        = ${XROOTD_XRDPOSIX_LIB}")
message (STATUS "XROOTD_XRDSERVER_LIB       = ${XROOTD_XRDSERVER_LIB}")
message (STATUS "XROOTD_XRDSSI_LIB          = ${XROOTD_XRDSSI_LIB}")
message (STATUS "XROOTD_XRDUTILS_LIB        = ${XROOTD_XRDUTILS_LIB}")

include (FindPackageHandleStandardArgs)
find_package_handle_standard_args (xrootd DEFAULT_MSG
  XROOTD_INCLUDE_DIR
  XROOTD_PRIVATE_INCLUDE_DIR
  XROOTD_XRDCL_LIB
  XROOTD_XRDPOSIX_LIB
  XROOTD_XRDSERVER_LIB
  XROOTD_XRDSSI_LIB
  XROOTD_XRDUTILS_LIB)
