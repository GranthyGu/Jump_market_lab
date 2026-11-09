add_library(marketlab_bpt STATIC
  cmu/src/buffer/arc_replacer.cpp
  cmu/src/buffer/buffer_pool_manager.cpp
  cmu/src/storage/page/page_guard.cpp
  cmu/src/storage/page/b_plus_tree_page.cpp
  # These units provide GenericKey/RID instantiations in the full CMU build.
  # Include them here too so tools get the standalone include paths and defines.
  cmu/src/storage/page/b_plus_tree_internal_page.cpp
  cmu/src/storage/page/b_plus_tree_leaf_page.cpp
  cmu/src/storage/index/b_plus_tree.cpp
  cmu/src/storage/index/index_iterator.cpp
  cmu/src/storage/disk/disk_scheduler.cpp
  cmu/src/storage/disk/disk_manager.cpp
  page_file.cpp)
target_include_directories(marketlab_bpt PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/cmu/src/include")
target_compile_definitions(marketlab_bpt PUBLIC MARKETLAB_BPT_STANDALONE=1)
target_compile_features(marketlab_bpt PUBLIC cxx_std_20)
target_link_libraries(marketlab_bpt PUBLIC Threads::Threads)
target_compile_options(marketlab_bpt PRIVATE -Wall -Wextra -Wpedantic)
