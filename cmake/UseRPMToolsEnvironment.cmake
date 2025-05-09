# Small duplication of the main UseRPMToolsEnvironement
# in order to have rpm variables handy before calling
# CPack (which in turn should be done before UseRPMTools)

IF (WIN32)
  MESSAGE(STATUS "RPM tools not available on Win32 systems")
ENDIF(WIN32)

IF (UNIX)
  # Look for RPM builder executable
  FIND_PROGRAM(RPMTools_RPMBUILD_EXECUTABLE
    NAMES rpmbuild
    PATHS "/usr/bin;/usr/lib/rpm"
    PATH_SUFFIXES bin
    DOC "The RPM builder tool"
  )

  IF (RPMTools_RPMBUILD_EXECUTABLE)
    MESSAGE(STATUS "Looking for RPMTools... - found rpmbuild at ${RPMTools_RPMBUILD_EXECUTABLE}")
    SET(RPMTools_RPMBUILD_FOUND "YES")
    GET_FILENAME_COMPONENT(RPMTools_BINARY_DIRS ${RPMTools_RPMBUILD_EXECUTABLE} PATH)
  ELSE()
    SET(RPMTools_RPMBUILD_FOUND "NO")
    MESSAGE(STATUS "Looking for RPMTools... - rpmbuild NOT FOUND")
  ENDIF()

  set(RPMTools_RPMBUILD_DIST "")
  set(OSV "")

  # Detect OS and version from /etc/os-release
  # For now we only support detecting el9
  if(EXISTS "/etc/os-release")
    file(READ "/etc/os-release" OS_RELEASE_CONTENTS)

    string(REGEX MATCH "ID=\"?([a-zA-Z0-9]+)\"?" _dummy "${OS_RELEASE_CONTENTS}")
    set(OS_ID "${CMAKE_MATCH_1}")

    string(REGEX MATCH "VERSION_ID=\"?([0-9]+)" _dummy "${OS_RELEASE_CONTENTS}")
    set(OSV "${CMAKE_MATCH_1}")

    if(OS_ID STREQUAL "centos" OR OS_ID STREQUAL "rhel" OR OS_ID STREQUAL "almalinux" OR OS_ID STREQUAL "rocky")
      set(RPMTools_RPMBUILD_DIST "el${OSV}")
    endif()
  endif()

  if(RPMTools_RPMBUILD_DIST STREQUAL "" OR OSV STREQUAL "")
    message(FATAL_ERROR "Could not determine RPM platform or OS version. RPMTools_RPMBUILD_DIST='${RPMTools_RPMBUILD_DIST}', OSV='${OSV}'")
  endif()

  message(STATUS "Detected OS ID: ${OS_ID}")
  message(STATUS "Detected OS Version: ${OSV}")
  message(STATUS "Set rpmbuild dist: ${RPMTools_RPMBUILD_DIST}")
ENDIF()
