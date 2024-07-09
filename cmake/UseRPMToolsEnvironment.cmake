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
    DOC "The RPM builder tool")
  
  IF (RPMTools_RPMBUILD_EXECUTABLE)
    MESSAGE(STATUS "Looking for RPMTools... - found rpmuild is ${RPMTools_RPMBUILD_EXECUTABLE}")
    SET(RPMTools_RPMBUILD_FOUND "YES")
    GET_FILENAME_COMPONENT(RPMTools_BINARY_DIRS ${RPMTools_RPMBUILD_EXECUTABLE} PATH)
  ELSE (RPMTools_RPMBUILD_EXECUTABLE) 
    SET(RPMTools_RPMBUILD_FOUND "NO")
    MESSAGE(STATUS "Looking for RPMTools... - rpmbuild NOT FOUND")
  ENDIF (RPMTools_RPMBUILD_EXECUTABLE)

  # Detetect rpmbuild environment (dist variable)
  execute_process(COMMAND ${RPMTools_RPMBUILD_EXECUTABLE} --showrc
    OUTPUT_VARIABLE RPMTools_RPMBUILD_SHOWRC)
  string(REGEX MATCH "-14: dist[^\n]*" RPMTools_RPMBUILD_DIST "${RPMTools_RPMBUILD_SHOWRC}")
  # message(STATUS "Found line for rpmbuild dist: ${RPMTools_RPMBUILD_DIST}")z
  string(REGEX REPLACE ".*\t" "" RPMTools_RPMBUILD_DIST "${RPMTools_RPMBUILD_DIST}")
  message(STATUS "Detected rpmbuild dist: ${RPMTools_RPMBUILD_DIST}")
ENDIF (UNIX)