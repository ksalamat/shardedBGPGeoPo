cmake_minimum_required(VERSION 3.10)
project(BGPGeopolitics)
set (CMAKE_CXX_STANDARD 11)
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libstdc++")
#set(CMAKE_CXX_FLAGS_RELEASE "-O3")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")
#set(CMAKE_CXX_FLAGS_DEBUG  "-g")
set(CMAKE_BUILD_TYPE Debug)
#find_package(MPI REQUIRED)

include_directories(${MPI_INCLUDE_PATH})

set(LIBTRACEIO "nothreads")

find_package( Boost COMPONENTS thread system graph filesystem iostreams REQUIRED)
include_directories( ${Boost_INCLUDE_DIRS} )

set(CMAKE_VERBOSE_MAKEFILE ON)
link_directories(/usr/local/lib/)
link_directories(/usr/lib/)
include_directories(/usr/local/include/ /usr/lib/x86_64-linux-gnu/openmpi/include/)
#include_directories(${INCLUDE_DIRS})


#SET(CMAKE_EXE_LINKER_FLAGS "-L./")
add_executable(BGPGeopolitics BGPRedis.cpp main.cpp BlockingQueue.h BGPGeopolitics.h BGPGeopolitics.cpp cache.h BGPGraph.h BGPGeopolitics.cpp cache.cpp BGPTables.h BGPTables.cpp BGPSaver.h BGPEvent.h tojson.h apibgpview.h apibgpview.cpp BGPSource.cpp cache_structures.h LruCache.h)
target_link_libraries(BGPGeopolitics bgpstream tbb pthread ${MPI_LIBRARIES})
target_link_libraries(BGPGeopolitics ${Boost_SYSTEM_LIBRARY} ${Boost_FILESYSTEM_LIBRARY} ${Boost_THREAD_LIBRARY} ${Boost_IOSTREAMS_LIBRARY})
target_link_libraries(BGPGeopolitics sqlite3)
target_link_libraries(BGPGeopolitics curl hiredis redis++ rdkafka)
