# SPDX-FileCopyrightText: 2015 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# This module will set the following variables:
#     PYTHON_FOUND
#     PYTHON_PROGRAM

find_program(PYTHON_PROGRAM python)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(python DEFAULT_MSG
  PYTHON_PROGRAM)
