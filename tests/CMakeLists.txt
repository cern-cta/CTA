cmake_minimum_required (VERSION 2.6)

add_executable(unittests
  unit_tests.cpp)

target_link_libraries(unittests
  castorexceptionunittests
  castoriounittests
  castorlegacymsgunittests
  castorlogunittests
  castormessagesunittests
  castorserverunittests
  castortapereactorunittests
  castortapeserverutilsunittests
  castorutilsunittests
  ctacommon
  ctacommonunittests
  CTAObjectStore
  ctaobjectstoreunittests
  ctanameserverunittests
  ctaremotensunittests
  ctaschedulerunittests
  gmock
  gtest
  pthread)
