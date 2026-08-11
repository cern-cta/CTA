# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# Creates source and binary RPM targets using CPack for the source archive and
# rpmbuild for package generation. Tool discovery is intentionally deferred
# until this function is called so non-packaging builds do not require rpmbuild.
function(rpmtools_add_rpm_targets rpm_name spec_file)
  if(NOT UNIX)
    message(FATAL_ERROR "RPM packaging is only supported on Unix systems")
  endif()
  if(NOT DEFINED CPACK_PACKAGE_NAME)
    message(FATAL_ERROR "CPack must be included before creating RPM targets")
  endif()

  find_program(RPMTOOLS_RPMBUILD_EXECUTABLE
    NAMES rpmbuild
    PATHS /usr/bin /usr/lib/rpm
    PATH_SUFFIXES bin
    DOC "RPM package builder")
  if(NOT RPMTOOLS_RPMBUILD_EXECUTABLE)
    message(FATAL_ERROR "RPM packaging was requested, but rpmbuild was not found")
  endif()

  set(rpm_root "${CMAKE_BINARY_DIR}/RPM")
  foreach(directory IN ITEMS tmp BUILD RPMS SOURCES SPECS SRPMS)
    file(MAKE_DIRECTORY "${rpm_root}/${directory}")
  endforeach()

  get_filename_component(spec_extension "${spec_file}" EXT)
  if(spec_extension STREQUAL ".spec")
    get_filename_component(spec_name "${spec_file}" NAME)
    configure_file("${spec_file}" "${rpm_root}/SPECS/${spec_name}" COPYONLY)
  else()
    get_filename_component(spec_name "${spec_file}" NAME_WE)
    set(spec_name "${spec_name}.spec")
    configure_file("${spec_file}" "${rpm_root}/SPECS/${spec_name}" @ONLY)
  endif()

  message(STATUS "RPM package builder: ${RPMTOOLS_RPMBUILD_EXECUTABLE}")
  message(STATUS "RPM build root: ${rpm_root}")

  # Both targets share the same source archive and configured spec file.
  add_custom_target("${rpm_name}_srpm"
    COMMAND "${CMAKE_CPACK_COMMAND}" -G TZST --config CPackSourceConfig.cmake
    COMMAND "${CMAKE_COMMAND}" -E copy
      "${CPACK_SOURCE_PACKAGE_FILE_NAME}.tar.zst"
      "${rpm_root}/SOURCES"
    COMMAND "${RPMTOOLS_RPMBUILD_EXECUTABLE}"
      -bs
      "--define=_topdir ${rpm_root}"
      "--define=_source_filedigest_algorithm md5"
      "--define=_binary_filedigest_algorithm md5"
      "--define=neutralpackage 1"
      --nodeps
      "--buildroot=${rpm_root}/tmp"
      "${rpm_root}/SPECS/${spec_name}"
    VERBATIM)

  add_custom_target("${rpm_name}_rpm"
    COMMAND "${CMAKE_CPACK_COMMAND}" -G TZST --config CPackSourceConfig.cmake
    COMMAND "${CMAKE_COMMAND}" -E copy
      "${CPACK_SOURCE_PACKAGE_FILE_NAME}.tar.zst"
      "${rpm_root}/SOURCES"
    COMMAND "${RPMTOOLS_RPMBUILD_EXECUTABLE}"
      -bb
      "--define=_topdir ${rpm_root}"
      $ENV{RPMDEFS}
      "--buildroot=${rpm_root}/tmp"
      "${rpm_root}/SPECS/${spec_name}"
    JOB_POOL console
    VERBATIM)
endfunction()
