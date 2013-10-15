cmake_minimum_required (VERSION 2.6)

include_directories ("..")
include_directories ("../h")

set (COMMON_LIB_SRC_FILES
  compat.c getacct.c getacctent.c getconfent.cpp
  getifnam.c getuser.c log.c marshall.c
  readc.c remote.c
  serror.c socket.c solveln.c trace.c
  wsacct.c ypgetacct.c ypgetacctent.c
  Cthread.c Cglobals.c Cgrp.c Cmutex.c Cuuid.c
  Cdomainname.c Cnetdb.c Cpool.c Cpwd.c Cstrerror.c
  socket_timeout.c u64subr.c Cinitdaemon.c Cgetopt.c
  Cregexp.c Cexportconf.c Csnprintf.c util.c
  Csecloader.c castor1compat.c
  ../castor/exception/BadVersion.cpp
  ../castor/exception/Busy.cpp
  ../castor/exception/DismountFailed.cpp
  ../castor/exception/Exception.cpp
  ../castor/exception/FailedToExecutePython.cpp
  ../castor/exception/InvalidArgument.cpp
  ../castor/exception/InvalidConfigEntry.cpp
  ../castor/exception/InvalidConfiguration.cpp
  ../castor/exception/Internal.cpp
  ../castor/exception/Mismatch.cpp
  ../castor/exception/NotSupported.cpp
  ../castor/exception/OutOfMemory.cpp
  ../castor/exception/QueryVolumeFailed.cpp
  ../castor/exception/Communication.cpp
  ../castor/exception/MissingOperand.cpp
  ../castor/exception/MountFailed.cpp
  ../castor/exception/NoEntry.cpp
  ../castor/exception/NoPortInRange.cpp
  ../castor/exception/NoSegmentFound.cpp
  ../castor/exception/NoValue.cpp
  ../castor/exception/PermissionDenied.cpp
  ../castor/exception/RequestCanceled.cpp
  ../castor/exception/RequestFailed.cpp
  ../castor/exception/Security.cpp
  ../castor/exception/SQLError.cpp
  ../castor/exception/TapeNetAcceptInterrupted.cpp
  ../castor/exception/TimeOut.cpp
  ../castor/exception/TooBig.cpp
  ../castor/common/CastorConfiguration.cpp)

add_library (castorcommon SHARED ${COMMON_LIB_SRC_FILES})
set_target_properties (castorcommon
  PROPERTIES SOVERSION ${MAJOR_CASTOR_VERSION})
install (TARGETS castorcommon DESTINATION $CASTOR_DEST_LIB_DIR)
