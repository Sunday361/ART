file(GLOB_RECURSE sources ${PROJECT_SOURCE_DIR}/src/*/*.cpp ${PROJECT_SOURCE_DIR}/src/*/*.h
        ${PROJECT_SOURCE_DIR}/src/*.cpp ${PROJECT_SOURCE_DIR}/src/*.h)
add_library(art_shared SHARED ${sources})

find_package(TBB)
target_link_libraries(art_shared TBB::tbb)