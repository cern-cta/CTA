# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# Resources shared by the CTA services
add_library(cta-service-resources STATIC src/ServiceResources.cpp)
add_public_headers(cta-service-resources)
if(CTA_USE_PGSCHED)
  target_link_libraries(cta-service-resources PUBLIC ctacommon ctacatalogue ctascheduler ctaruntime)
else()
  target_link_libraries(cta-service-resources PUBLIC ctacommon ctaobjectstore ctacatalogue ctascheduler ctaruntime)
endif()

# Unit tests for the service resources library
add_library(cta-service-resources-unit-tests SHARED test/ServiceResourcesTest.cpp)
target_link_libraries(cta-service-resources-unit-tests cta-service-resources ctacommon ctaruntime gtest)
set_property(TARGET cta-service-resources-unit-tests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET cta-service-resources-unit-tests PROPERTY VERSION "${CTA_LIBVERSION}")
if(CTA_BUILD_TEST_PACKAGES)
  install(TARGETS cta-service-resources-unit-tests DESTINATION ${CMAKE_INSTALL_LIBDIR})
endif()
