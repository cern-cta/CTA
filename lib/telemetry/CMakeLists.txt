# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

add_library(ctatelemetry SHARED
  src/CtaOtelLogHandler.cpp
  src/OtelInit.cpp
  src/TelemetryInit.cpp
  src/config/TelemetryConfig.cpp
  src/metrics/InstrumentRegistry.cpp
  src/metrics/MetricsUtils.cpp)

set_property(TARGET ctatelemetry PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctatelemetry PROPERTY VERSION "${CTA_LIBVERSION}")

target_include_directories(ctatelemetry PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/include
  ${OPENTELEMETRY_CPP_INCLUDE_DIRS})
target_include_directories(ctatelemetry PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(ctatelemetry PUBLIC ctacommon ${OPENTELEMETRY_CPP_LIBRARIES})

install(TARGETS ctatelemetry DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(FILES cta-otel.example.yaml
  DESTINATION ${CMAKE_INSTALL_DOCDIR}
  PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ)

foreach(component frontend maintd objectstore rdbms scheduler taped)
  string(SUBSTRING "${component}" 0 1 component_initial)
  string(TOUPPER "${component_initial}" component_initial)
  string(SUBSTRING "${component}" 1 -1 component_rest)
  set(metrics_name "${component_initial}${component_rest}Metrics")

  add_library(ctatelemetry${component} SHARED
    src/metrics/${metrics_name}.cpp)
  set_property(TARGET ctatelemetry${component} PROPERTY SOVERSION "${CTA_SOVERSION}")
  set_property(TARGET ctatelemetry${component} PROPERTY VERSION "${CTA_LIBVERSION}")
  target_include_directories(ctatelemetry${component} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
  target_link_libraries(ctatelemetry${component} PUBLIC ctatelemetry)
  install(TARGETS ctatelemetry${component} DESTINATION ${CMAKE_INSTALL_LIBDIR})
endforeach()
