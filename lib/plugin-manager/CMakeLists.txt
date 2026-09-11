# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

add_library(ctapluginmanager INTERFACE)

target_include_directories(ctapluginmanager INTERFACE
  ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(ctapluginmanager INTERFACE ${CMAKE_DL_LIBS})
