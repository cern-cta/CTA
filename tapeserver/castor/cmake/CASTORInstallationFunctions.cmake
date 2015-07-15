################################################################################
# Global rules and variables
################################################################################
set (CASTOR_DEST_BIN_DIR /usr/bin)
set (CASTOR_DEST_LIB_DIR /usr/lib64)
set (CASTOR_DEST_MAN_DIR /usr/share/man)
set (CASTOR_DEST_INCLUDE_DIR /usr/include)
set (CASTOR_DEST_PERL_LIBDIR /usr/lib/perl/CASTOR)
set (CASTOR_DEST_C_HEADERS_DIR ${CASTOR_DEST_INCLUDE_DIR}/castor/h)
set (CASTOR_DEST_CPP_HEADERS_DIR ${CASTOR_DEST_INCLUDE_DIR}/castor/castor)
set (CASTOR_DEST_WWW_DIR /var/www)
set (CASTOR_DEST_UDEV_RULE_DIR /etc/udev/rules.d)
set (CASTOR_DEST_SQL_DIR /usr/share/castor-${CASTOR_VERSION}-${CASTOR_RELEASE}/sql)

message (STATUS "CMAKE_SYSTEM_NAME = ${CMAKE_SYSTEM_NAME}")
message (STATUS "CMAKE_SIZEOF_VOID_P = ${CMAKE_SIZEOF_VOID_P}")
if (${CMAKE_SYSTEM_NAME} MATCHES "Linux" AND
   ${CMAKE_SIZEOF_VOID_P} MATCHES "8")
  set (CASTOR_DEST_LIB_DIR /usr/lib64)
  message (STATUS "CASTOR_DEST_LIB_DIR = ${CASTOR_DEST_LIB_DIR}")
else ()
  set (CASTOR_DEST_LIB_DIR /usr/lib)
  message (STATUS "CASTOR_DEST_LIB_DIR = ${CASTOR_DEST_LIB_DIR}")
endif ()

set (CASTOR_BIN_SCRIPT_PERMS
  OWNER_READ OWNER_WRITE OWNER_EXECUTE
  GROUP_READ             OWNER_EXECUTE
  WORLD_READ             WORLD_EXECUTE)

set (CASTOR_ETC_PERMS
  OWNER_READ OWNER_WRITE
  GROUP_READ
  WORLD_READ)

set (CASTOR_ETC_CRON_D_PERMS
  OWNER_READ OWNER_WRITE
  GROUP_READ
  WORLD_READ)

set (CASTOR_ETC_XINETD_D_PERMS
  OWNER_READ OWNER_WRITE
  GROUP_READ
  WORLD_READ)

set (CASTOR_EXAMPLE_PERMS
  OWNER_READ OWNER_WRITE
  GROUP_READ
  WORLD_READ)

set (CASTOR_HEADER_PERMS
  OWNER_READ OWNER_WRITE
  GROUP_READ
  WORLD_READ)

set (CASTOR_INITSCRIPT_PERMS
  OWNER_READ OWNER_WRITE OWNER_EXECUTE
  GROUP_READ             GROUP_EXECUTE
  WORLD_READ             WORLD_EXECUTE)

set (CASTOR_LOGROTATE_PERMS
  OWNER_READ OWNER_WRITE
  GROUP_READ
  WORLD_READ)

set (CASTOR_MAN_PAGE_PERMS
  OWNER_READ OWNER_WRITE
  GROUP_READ
  WORLD_READ)

set (CASTOR_NON_EXEC_PERMS
  OWNER_READ OWNER_WRITE
  GROUP_READ
  WORLD_READ)

set (CASTOR_PYTHON_LIB_PERMS
  OWNER_READ OWNER_WRITE
  GROUP_READ            
  WORLD_READ)

set (CASTOR_SCRIPT_PERMS
  OWNER_READ OWNER_WRITE OWNER_EXECUTE
  GROUP_READ             GROUP_EXECUTE
  WORLD_READ             WORLD_EXECUTE)

set (CASTOR_SYSCONFIG_PERMS
  OWNER_READ OWNER_WRITE
  GROUP_READ
  WORLD_READ)

set (CASTOR_UDEV_RULES_PERMS
  OWNER_READ OWNER_WRITE
  GROUP_READ
  WORLD_READ)

set (CASTOR_SQL_PERMS
  OWNER_READ OWNER_WRITE
  GROUP_READ
  WORLD_READ)

function (CastorInstallDir _name)
  install (CODE
    "message (STATUS \"Installing directory \$ENV{DESTDIR}${_name}\")")
  install (CODE "file (MAKE_DIRECTORY \$ENV{DESTDIR}${_name})")
endfunction ()

function (CastorInstallEtcCronD _name)
  install (FILES ${_name}.cron_d
    DESTINATION /etc/cron.d
    PERMISSIONS ${CASTOR_ETC_CRON_D_PERMS}
    RENAME ${_name})
endfunction ()

function (CastorInstallEtcExample _name)
  install (FILES ${_name}.etc
    DESTINATION /etc
    PERMISSIONS ${CASTOR_ETC_PERMS}
    RENAME ${_name}.example)
endfunction ()

function (CastorInstallEtcXinetdD _name)
  install (FILES ${_name}.xinetd_d
    DESTINATION /etc/xinetd.d
    PERMISSIONS ${CASTOR_ETC_XINETD_D_PERMS}
    RENAME ${_name})
endfunction ()

function (CastorInstallExample _name _dest)
  install (FILES ${_name}
    DESTINATION ${_dest}
    PERMISSIONS ${CASTOR_EXAMPLE_PERMS}
    RENAME ${_name}.example)
endfunction ()

function (CastorInstallScript _name)
  install (FILES ${_name}
    DESTINATION ${CASTOR_DEST_BIN_DIR}
    PERMISSIONS ${CASTOR_BIN_SCRIPT_PERMS})
endfunction ()

function (CastorInstallManPage _name _section)
  install (FILES ${_name}.man
    DESTINATION ${CASTOR_DEST_MAN_DIR}/man${_section}
    PERMISSIONS ${CASTOR_MAN_PAGE_PERMS}
    RENAME ${_name}.${_section}castor)
endfunction ()

function (CastorInstallExeManPage _name)
  CastorInstallManPage (${_name} 1)
endfunction ()

function (CastorInstallSysManPage _name)
  CastorInstallManPage (${_name} 2)
endfunction ()

function (CastorInstallLibManPage _name)
  CastorInstallManPage (${_name} 3)
endfunction ()

function (CastorInstallFileManPage _name)
  CastorInstallManPage (${_name} 4)
endfunction ()

function (CastorInstallAdmManPage _name)
  CastorInstallManPage (${_name} 8)
endfunction ()

function (CastorInstallLogrotate _name)
  install (FILES ${_name}.logrotate
    DESTINATION /etc/logrotate.d
    PERMISSIONS ${CASTOR_LOGROTATE_PERMS}
    RENAME ${_name})
endfunction ()

function (CastorInstallSysconfigExample _name)
  install (FILES ${_name}.sysconfig
    DESTINATION /etc/sysconfig
    PERMISSIONS ${CASTOR_SYSCONFIG_PERMS}
    RENAME ${_name}.example)
endfunction ()

function (CastorInstallInitScript _name)
  install (FILES ${_name}.init
    DESTINATION /etc/init.d
    PERMISSIONS ${CASTOR_INITSCRIPT_PERMS}
    RENAME ${_name})
endfunction ()

set (CASTOR_CONFIG_PERMS
  OWNER_READ OWNER_WRITE
  GROUP_READ)

function (CastorInstallConfigFile _name)
  install (FILES ${_name}CONFIG
    DESTINATION /etc/castor
    PERMISSIONS ${CASTOR_CONFIG_PERMS}
    RENAME ${_name}CONFIG.example)
endfunction ()

function (CastorInstallConfigFileLowercase _name)
  install (FILES ${_name}
    DESTINATION /etc/castor
    PERMISSIONS ${CASTOR_CONFIG_PERMS}
    RENAME ${_name}.example)
endfunction ()

function (CastorInstallConfigNoRename _name)
  install (FILES ${_name}
    DESTINATION /etc/castor
    PERMISSIONS ${CASTOR_CONFIG_PERMS})
endfunction ()

function (CastorSetLibraryVersions _name)
  set_target_properties (${_name} PROPERTIES
  VERSION ${MAJOR_CASTOR_VERSION}.${MINOR_CASTOR_VERSION}
  SOVERSION ${MAJOR_CASTOR_VERSION})
endfunction ()

function (CastorInstallUdevRule _name)
  install (FILES ${_name}
    DESTINATION ${CASTOR_DEST_UDEV_RULE_DIR}
    PERMISSIONS ${CASTOR_UDEV_RULES_PERMS})
endfunction ()

function (CastorInstallSQL _name)
  install (FILES ${CMAKE_CURRENT_BINARY_DIR}/${_name}
    DESTINATION ${CASTOR_DEST_SQL_DIR}
    PERMISSIONS ${CASTOR_SQL_PERMS})
endfunction ()

function (CastorInstallSQLFromSource _name)
  install (FILES ${CMAKE_CURRENT_SOURCE_DIR}/${_name}
    DESTINATION ${CASTOR_DEST_SQL_DIR}
    PERMISSIONS ${CASTOR_SQL_PERMS})
endfunction ()
