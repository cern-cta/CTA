################################################################################
# Global rules and variables
################################################################################
set (CTA_DEST_BIN_DIR /usr/bin)
set (CTA_DEST_LIB_DIR /usr/lib64)
set (CTA_DEST_MAN_DIR /usr/share/man)
set (CTA_DEST_INCLUDE_DIR /usr/include)
set (CTA_DEST_PERL_LIBDIR /usr/lib/perl/CTA)
set (CTA_DEST_WWW_DIR /var/www)
set (CTA_DEST_UDEV_RULE_DIR /etc/udev/rules.d)

message (STATUS "CMAKE_SYSTEM_NAME = ${CMAKE_SYSTEM_NAME}")
message (STATUS "CMAKE_SIZEOF_VOID_P = ${CMAKE_SIZEOF_VOID_P}")
if (${CMAKE_SYSTEM_NAME} MATCHES "Linux" AND
   ${CMAKE_SIZEOF_VOID_P} MATCHES "8")
  set (CTA_DEST_LIB_DIR /usr/lib64)
  message (STATUS "CTA_DEST_LIB_DIR = ${CTA_DEST_LIB_DIR}")
else ()
  set (CTA_DEST_LIB_DIR /usr/lib)
  message (STATUS "CTA_DEST_LIB_DIR = ${CTA_DEST_LIB_DIR}")
endif ()

set (CTA_BIN_SCRIPT_PERMS
  OWNER_READ OWNER_WRITE OWNER_EXECUTE
  GROUP_READ             OWNER_EXECUTE
  WORLD_READ             WORLD_EXECUTE)

set (CTA_ETC_PERMS
  OWNER_READ OWNER_WRITE
  GROUP_READ
  WORLD_READ)

set (CTA_ETC_CRON_D_PERMS
  OWNER_READ OWNER_WRITE
  GROUP_READ
  WORLD_READ)

set (CTA_ETC_XINETD_D_PERMS
  OWNER_READ OWNER_WRITE
  GROUP_READ
  WORLD_READ)

set (CTA_EXAMPLE_PERMS
  OWNER_READ OWNER_WRITE
  GROUP_READ
  WORLD_READ)

set (CTA_HEADER_PERMS
  OWNER_READ OWNER_WRITE
  GROUP_READ
  WORLD_READ)

set (CTA_INITSCRIPT_PERMS
  OWNER_READ OWNER_WRITE OWNER_EXECUTE
  GROUP_READ             GROUP_EXECUTE
  WORLD_READ             WORLD_EXECUTE)

set (CTA_LOGROTATE_PERMS
  OWNER_READ OWNER_WRITE
  GROUP_READ
  WORLD_READ)

set (CTA_MAN_PAGE_PERMS
  OWNER_READ OWNER_WRITE
  GROUP_READ
  WORLD_READ)

set (CTA_NON_EXEC_PERMS
  OWNER_READ OWNER_WRITE
  GROUP_READ
  WORLD_READ)

set (CTA_PYTHON_LIB_PERMS
  OWNER_READ OWNER_WRITE
  GROUP_READ            
  WORLD_READ)

set (CTA_SCRIPT_PERMS
  OWNER_READ OWNER_WRITE OWNER_EXECUTE
  GROUP_READ             GROUP_EXECUTE
  WORLD_READ             WORLD_EXECUTE)

set (CTA_SYSCONFIG_PERMS
  OWNER_READ OWNER_WRITE
  GROUP_READ
  WORLD_READ)

set (CTA_UDEV_RULES_PERMS
  OWNER_READ OWNER_WRITE
  GROUP_READ
  WORLD_READ)

set (CTA_SQL_PERMS
  OWNER_READ OWNER_WRITE
  GROUP_READ
  WORLD_READ)

function (CTAInstallDir _name)
  install (CODE
    "message (STATUS \"Installing directory \$ENV{DESTDIR}${_name}\")")
  install (CODE "file (MAKE_DIRECTORY \$ENV{DESTDIR}${_name})")
endfunction ()

function (CTAInstallEtcCronD _name)
  install (FILES ${_name}.cron_d
    DESTINATION /etc/cron.d
    PERMISSIONS ${CTA_ETC_CRON_D_PERMS}
    RENAME ${_name})
endfunction ()

function (CTAInstallEtcExample _name)
  install (FILES ${_name}.etc
    DESTINATION /etc
    PERMISSIONS ${CTA_ETC_PERMS}
    RENAME ${_name}.example)
endfunction ()

function (CTAInstallEtcXinetdD _name)
  install (FILES ${_name}.xinetd_d
    DESTINATION /etc/xinetd.d
    PERMISSIONS ${CTA_ETC_XINETD_D_PERMS}
    RENAME ${_name})
endfunction ()

function (CTAInstallExample _name _dest)
  install (FILES ${_name}
    DESTINATION ${_dest}
    PERMISSIONS ${CTA_EXAMPLE_PERMS}
    RENAME ${_name}.example)
endfunction ()

function (CTAInstallScript _name)
  install (FILES ${_name}
    DESTINATION ${CTA_DEST_BIN_DIR}
    PERMISSIONS ${CTA_BIN_SCRIPT_PERMS})
endfunction ()

function (CTAInstallPythonLib _name)
  if (${ARGC} GREATER 1)
    set (_subdir "/${ARGV1}")
  else ()
    set (_subdir "")
  endif ()
  install (FILES ${_name}
    DESTINATION ${CTA_DEST_PYTHON_LIBDIR}${_subdir}
    PERMISSIONS ${CTA_PYTHON_LIB_PERMS})
endfunction ()

function (CTAInstallManPage _name _section)
  install (FILES ${_name}.man
    DESTINATION ${CTA_DEST_MAN_DIR}/man${_section}
    PERMISSIONS ${CTA_MAN_PAGE_PERMS}
    RENAME ${_name}.${_section}cta)
endfunction ()

function (CTAInstallExeManPage _name)
  CTAInstallManPage (${_name} 1)
endfunction ()

function (CTAInstallSysManPage _name)
  CTAInstallManPage (${_name} 2)
endfunction ()

function (CTAInstallLibManPage _name)
  CTAInstallManPage (${_name} 3)
endfunction ()

function (CTAInstallFileManPage _name)
  CTAInstallManPage (${_name} 4)
endfunction ()

function (CTAInstallAdmManPage _name)
  CTAInstallManPage (${_name} 8)
endfunction ()

function (CTAInstallLogrotate _name)
  install (FILES ${_name}.logrotate
    DESTINATION /etc/logrotate.d
    PERMISSIONS ${CTA_LOGROTATE_PERMS}
    RENAME ${_name})
endfunction ()

function (CTAInstallSysconfigExample _name)
  install (FILES ${_name}.sysconfig
    DESTINATION /etc/sysconfig
    PERMISSIONS ${CTA_SYSCONFIG_PERMS}
    RENAME ${_name}.example)
endfunction ()

function (CTAInstallInitScript _name)
  install (FILES ${_name}.init
    DESTINATION /etc/init.d
    PERMISSIONS ${CTA_INITSCRIPT_PERMS}
    RENAME ${_name})
endfunction ()

set (CTA_CONFIG_PERMS
  OWNER_READ OWNER_WRITE
  GROUP_READ)

function (CTAInstallConfigFile _name)
  install (FILES ${_name}CONFIG
    DESTINATION /etc/cta
    PERMISSIONS ${CTA_CONFIG_PERMS}
    RENAME ${_name}CONFIG.example)
endfunction ()

function (CTAInstallConfigFileLowercase _name)
  install (FILES ${_name}
    DESTINATION /etc/cta
    PERMISSIONS ${CTA_CONFIG_PERMS}
    RENAME ${_name}.example)
endfunction ()

function (CTAInstallConfigNoRename _name)
  install (FILES ${_name}
    DESTINATION /etc/cta
    PERMISSIONS ${CTA_CONFIG_PERMS})
endfunction ()

function (CastorSetLibraryVersions _name)
  set_target_properties (${_name} PROPERTIES
  VERSION ${MAJOR_CTA_VERSION}.${MINOR_CTA_VERSION}
  SOVERSION ${MAJOR_CTA_VERSION})
endfunction ()

function (CTAInstallUdevRule _name)
  install (FILES ${_name}
    DESTINATION ${CTA_DEST_UDEV_RULE_DIR}
    PERMISSIONS ${CTA_UDEV_RULES_PERMS})
endfunction ()

function (CTAInstallSQL _name)
  install (FILES ${CMAKE_CURRENT_BINARY_DIR}/${_name}
    DESTINATION ${CTA_DEST_SQL_DIR}
    PERMISSIONS ${CTA_SQL_PERMS})
endfunction ()

function (CTAInstallSQLFromSource _name)
  install (FILES ${CMAKE_CURRENT_SOURCE_DIR}/${_name}
    DESTINATION ${CTA_DEST_SQL_DIR}
    PERMISSIONS ${CTA_SQL_PERMS})
endfunction ()
