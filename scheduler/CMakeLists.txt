cmake_minimum_required (VERSION 2.6)

add_library (ctascheduler SHARED
  Scheduler.cpp)

target_link_libraries(ctascheduler
  ctacommon)
