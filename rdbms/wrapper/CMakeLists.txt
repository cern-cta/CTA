# SPDX-FileCopyrightText: 2015 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

add_compile_options(-Wshadow)

find_package (sqlite REQUIRED)
include_directories (${POSTGRES_INCLUDE_DIRS})

if(CTA_WITH_ORACLE)
  include_directories (${ORACLE-INSTANTCLIENT_INCLUDE_DIRS})
  add_compile_definitions(SUPPORT_OCCI)
endif()

set (RDBMS_WRAPPER_LIB_SRC_FILES
  ColumnNameToIdx.cpp
  ColumnNameToIdxAndType.cpp
  ConnFactoryFactory.cpp
  ConnWrapper.cpp
  ParamNameToIdx.cpp
  RsetWrapper.cpp
  StmtWrapper.cpp
)

set (RDBMS_SQLITE_LIB_SRC_FILES
  Sqlite.cpp
  SqliteConn.cpp
  SqliteConnFactory.cpp
  SqliteRset.cpp
  SqliteStmt.cpp
)

set (RDBMS_POSTGRES_LIB_SRC_FILES
  PostgresConn.cpp
  PostgresConnFactory.cpp
  PostgresRset.cpp
  PostgresStmt.cpp
  PostgresColumn.cpp)

if(CTA_WITH_ORACLE)
  set (RDBMS_OCCI_LIB_SRC_FILES
    OcciColumn.cpp
    OcciConn.cpp
    OcciConnFactory.cpp
    OcciEnv.cpp
    OcciEnvSingleton.cpp
    OcciRset.cpp
    OcciStmt.cpp)
endif()

# SQLITE
## For a plugin that may not be linked by other targets,
## but may be dynamically loaded at runtime using dlopen-like functionality:
##   add_library (ctardbmssqlite MODULE
##
add_library (ctardbmssqlite SHARED
  ${RDBMS_SQLITE_LIB_SRC_FILES})
set_property(TARGET ctardbmssqlite PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctardbmssqlite PROPERTY   VERSION "${CTA_LIBVERSION}")

target_compile_definitions(ctardbmssqlite PRIVATE VERSION_API="${CTA_VERSION}-${CTA_RELEASE}")

target_link_libraries (ctardbmssqlite ctardbms ctacommon ctapluginmanager
  ${SQLITE_LIBRARIES})

install(TARGETS ctardbmssqlite DESTINATION ${CMAKE_INSTALL_LIBDIR})

# POSTGRES
add_library (ctardbmspostgres SHARED
  ${RDBMS_POSTGRES_LIB_SRC_FILES})
set_property(TARGET ctardbmspostgres PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctardbmspostgres PROPERTY   VERSION "${CTA_LIBVERSION}")

target_compile_definitions(ctardbmspostgres PRIVATE VERSION_API="${CTA_VERSION}-${CTA_RELEASE}")

target_link_libraries (ctardbmspostgres ctardbms ctacommon ctapluginmanager
  ${POSTGRES_LIBRARIES})

install(TARGETS ctardbmspostgres DESTINATION ${CMAKE_INSTALL_LIBDIR})

# OCCI
if(CTA_WITH_ORACLE)
  add_library (ctardbmsocci SHARED
    ${RDBMS_OCCI_LIB_SRC_FILES})
  set_property(TARGET ctardbmsocci PROPERTY SOVERSION "${CTA_SOVERSION}")
  set_property(TARGET ctardbmsocci PROPERTY   VERSION "${CTA_LIBVERSION}")

  target_compile_definitions(ctardbmsocci PRIVATE VERSION_API="${CTA_VERSION}-${CTA_RELEASE}")

  target_link_libraries (ctardbmsocci ctardbms ctacommon ctapluginmanager
    ${ORACLE-INSTANTCLIENT_LIBRARIES})

  install(TARGETS ctardbmsocci DESTINATION ${CMAKE_INSTALL_LIBDIR})
endif()

add_library (ctardbmswrapper SHARED
  ${RDBMS_WRAPPER_LIB_SRC_FILES})
set_property(TARGET ctardbmswrapper PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctardbmswrapper PROPERTY   VERSION "${CTA_LIBVERSION}")

target_compile_definitions(ctardbmswrapper PRIVATE
  CTA_ABI_SOVERSION="${CTA_SOVERSION}"
  VERSION_API="${CTA_VERSION}-${CTA_RELEASE}")

target_link_libraries (ctardbmswrapper
  ctacommon
  ctapluginmanager
)

if(CTA_WITH_ORACLE)
  set_property (TARGET ctardbmswrapper APPEND PROPERTY INSTALL_RPATH ${ORACLE-INSTANTCLIENT_RPATH})
endif()

install (TARGETS ctardbmswrapper DESTINATION ${CMAKE_INSTALL_LIBDIR})

set(RDBMS_WRAPPER_UNIT_TESTS_LIB_SRC_FILES
  ConnTest.cpp
  ParamNameToIdxTest.cpp
  PostgresStmtTest.cpp
  SqliteStmtTest.cpp)

if(CTA_WITH_ORACLE)
  set(RDBMS_WRAPPER_UNIT_TESTS_LIB_SRC_FILES
    ${RDBMS_WRAPPER_UNIT_TESTS_LIB_SRC_FILES}
    OcciColumnTest.cpp)
endif()

add_library (ctardbmswrapperunittests SHARED
  ${RDBMS_WRAPPER_UNIT_TESTS_LIB_SRC_FILES})
set_property(TARGET ctardbmswrapperunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctardbmswrapperunittests PROPERTY   VERSION "${CTA_LIBVERSION}")

target_link_libraries (ctardbmswrapperunittests ctardbmspostgres)

if(CTA_WITH_ORACLE)
  target_link_libraries (ctardbmswrapperunittests ctardbmsocci)
endif()

install(TARGETS ctardbmswrapperunittests DESTINATION ${CMAKE_INSTALL_LIBDIR})
