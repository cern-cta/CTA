# SPDX-FileCopyrightText: 2015 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# This module will set the following variables:
#     JSON-C_FOUND
#     JSON-C_INCLUDE_DIRS
#     JSON-C_LIBRARIES

find_path(JSON-C_INCLUDE_DIRS
  json.h
  PATH_SUFFIXES json-c)

find_library(JSON-C_LIBRARIES
  NAMES json-c)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(json-c DEFAULT_MSG
  JSON-C_INCLUDE_DIRS JSON-C_LIBRARIES)
