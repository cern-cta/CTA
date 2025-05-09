include(${CMAKE_SOURCE_DIR}/cmake/UseRPMToolsEnvironment.cmake)

# Version constraint variables for packages based on project.json
execute_process(
  COMMAND ${CMAKE_COMMAND} -E env PYTHONUNBUFFERED=1 python3
          ${CMAKE_SOURCE_DIR}/continuousintegration/utils/project-json/generate_version_constraints.py
          --platform ${RPMTools_RPMBUILD_DIST}
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  OUTPUT_VARIABLE BUILD_VERSION_RAW
  OUTPUT_STRIP_TRAILING_WHITESPACE
)

string(REPLACE "\n" ";" BUILD_VERSION_LINES "${BUILD_VERSION_RAW}")
foreach(line IN LISTS BUILD_VERSION_LINES)
  if(line MATCHES "^([A-Z0-9_]+)=\"(.*)\"$")
    set("${CMAKE_MATCH_1}" "${CMAKE_MATCH_2}")
  endif()
endforeach()

# Build requirements for spec file based on project.json
execute_process(
  COMMAND ${CMAKE_COMMAND} -E env PYTHONUNBUFFERED=1 python3
          ${CMAKE_SOURCE_DIR}/continuousintegration/utils/project-json/generate_build_requires.py
          --platform ${RPMTools_RPMBUILD_DIST}
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  OUTPUT_VARIABLE BUILD_REQUIREMENTS
  OUTPUT_STRIP_TRAILING_WHITESPACE
)

file(READ "${CMAKE_SOURCE_DIR}/project.json" PROJECT_JSON_STRING)
string(JSON PROJECT_SUMMARY GET ${PROJECT_JSON_STRING} summary)
string(JSON PROJECT_DESCRIPTION GET ${PROJECT_JSON_STRING} description)
string(JSON PROJECT_LICENSE GET ${PROJECT_JSON_STRING} license)
string(JSON PROJECT_CPP_VERSION GET ${PROJECT_JSON_STRING} dev cppVersion)
string(JSON PROJECT_DEFAULT_BUILD_TYPE GET ${PROJECT_JSON_STRING} dev defaultBuildType)

set("PROJECT_SUMMARY" "${PROJECT_SUMMARY}")
set("PROJECT_DESCRIPTION" "${PROJECT_DESCRIPTION}")
set("PROJECT_LICENSE" "${PROJECT_LICENSE}")
set("PROJECT_CPP_VERSION" "${PROJECT_CPP_VERSION}")
set("PROJECT_DEFAULT_BUILD_TYPE" "${PROJECT_DEFAULT_BUILD_TYPE}")
