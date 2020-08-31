# - Find xrootclient
# Finds the header files of xrootd-devel by searching for XrdVersion.hh
#
# XROOTD_FOUND               - true if xrootd has been found
# XROOTD_INCLUDE_DIR         - location of the xrootd-devel header files
#                              words the header files that do not contribute to
#                              the xrootd ABI.
# XROOTD_XRDCL_LIB           - location of the XrdCl library

# Be silent if XROOTD_INCLUDE_DIR is already cached
if (XROOTD_INCLUDE_DIR)
  set(XROOTD_FIND_QUIETLY TRUE)
endif (XROOTD_INCLUDE_DIR)

find_path (XROOTD_INCLUDE_DIR XrdVersion.hh
  PATH_SUFFIXES include/xrootd
)

find_library (XROOTD_XRDCL_LIB XrdCl)

message (STATUS "XROOTD_INCLUDE_DIR         = ${XROOTD_INCLUDE_DIR}")
message (STATUS "XROOTD_XRDCL_LIB           = ${XROOTD_XRDCL_LIB}")

include (FindPackageHandleStandardArgs)
find_package_handle_standard_args (xrootd DEFAULT_MSG 
  XROOTD_INCLUDE_DIR
  XROOTD_XRDCL_LIB)
