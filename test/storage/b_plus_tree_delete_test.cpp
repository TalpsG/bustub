//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_delete_test.cpp
//
// Identification: test/storage/b_plus_tree_delete_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <random>  // std::default_random_engine
#include <string>

#include "buffer/buffer_pool_manager.h"
#include "gtest/gtest.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/index/b_plus_tree.h"
#include "test_util.h"  // NOLINT

namespace bustub {

using bustub::DiskManagerUnlimitedMemory;

TEST(BPlusTreeTests, DISABLED_DeleteTest1) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", header_page->GetPageId(), bpm, comparator);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  std::vector<int64_t> remove_keys = {1, 5};
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    std::cout << key << "\n";
    tree.Remove(index_key, transaction);
    std::cout << tree.DrawBPlusTree();
  }

  int64_t size = 0;
  bool is_present;

  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    is_present = tree.GetValue(index_key, &rids);

    if (!is_present) {
      EXPECT_NE(std::find(remove_keys.begin(), remove_keys.end(), key), remove_keys.end());
    } else {
      EXPECT_EQ(rids.size(), 1);
      EXPECT_EQ(rids[0].GetPageId(), 0);
      EXPECT_EQ(rids[0].GetSlotNum(), key);
      size = size + 1;
    }
  }

  EXPECT_EQ(size, 3);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete bpm;
}

TEST(BPlusTreeTests, DISABLED_DeleteTest2) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", header_page->GetPageId(), bpm, comparator);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  std::vector<int64_t> remove_keys = {1, 5, 3, 4};
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
  }

  int64_t size = 0;
  bool is_present;

  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    is_present = tree.GetValue(index_key, &rids);

    if (!is_present) {
      EXPECT_NE(std::find(remove_keys.begin(), remove_keys.end(), key), remove_keys.end());
    } else {
      EXPECT_EQ(rids.size(), 1);
      EXPECT_EQ(rids[0].GetPageId(), 0);
      EXPECT_EQ(rids[0].GetSlotNum(), key);
      size = size + 1;
    }
  }

  EXPECT_EQ(size, 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete bpm;
}
TEST(BPlusTreeTests, DISABLED_talps_special_delete) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", header_page->GetPageId(), bpm, comparator, 4, 4);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

  std::vector<int64_t> keys = {4, 6, 5, 7, 2, 1, 9, 8, 0, 3};
  std::vector<int64_t> remove_keys = {0, 3, 6, 9};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    std::cout << "i: " << key << "\n";
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  std::cout << tree.DrawBPlusTree();
  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    std::cout << "d: " << key << "\n";
    tree.Remove(index_key, transaction);
    std::cout << tree.DrawBPlusTree();
  }

  int64_t size = 0;
  bool is_present;

  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    is_present = tree.GetValue(index_key, &rids);

    if (!is_present) {
      EXPECT_NE(std::find(remove_keys.begin(), remove_keys.end(), key), remove_keys.end());
    } else {
      EXPECT_EQ(rids.size(), 1);
      EXPECT_EQ(rids[0].GetPageId(), 0);
      EXPECT_EQ(rids[0].GetSlotNum(), key);
      size = size + 1;
    }
  }

  EXPECT_EQ(size, keys.size() - remove_keys.size());

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete bpm;
}
TEST(BPlusTreeTests, DISABLED_talps_special_delete_2) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", header_page->GetPageId(), bpm, comparator, 7, 7);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

  std::vector<int64_t> keys = {30, 18, 52, 38, 73, 29, 15, 27, 95, 99, 17, 12, 61, 6,  10, 91, 54, 83, 80, 71,
                               28, 63, 2,  39, 31, 70, 37, 60, 19, 72, 89, 97, 16, 47, 14, 57, 58, 48, 56, 32,
                               1,  59, 20, 33, 51, 93, 11, 40, 90, 22, 24, 8,  35, 44, 67, 88, 46, 76, 0,  3,
                               9,  23, 21, 85, 13, 86, 41, 64, 26, 45, 82, 34, 75, 98, 96, 65, 50, 49, 77, 36,
                               74, 94, 68, 5,  25, 66, 43, 7,  4,  81, 84, 62, 79, 69, 78, 87, 92, 42, 53, 55};
  std::vector<int64_t> remove_keys = {16, 72, 54, 86, 84, 64, 48, 68, 90, 30, 70, 50, 96, 18, 38, 80, 26,
                                      0,  10, 2,  20, 78, 46, 40, 52, 76, 22, 44, 74, 28, 36, 24, 66, 92,
                                      82, 62, 4,  58, 94, 6,  88, 98, 8,  42, 14, 56, 60, 12, 32, 34};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    std::cout << "i: " << key << "\n";
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  std::cout << tree.DrawBPlusTree();
  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int n = 0;
  //  std::string prefix = "treemap";
  //  std::string suffix = ".dot";
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    std::cout << "d: " << key << "\n";
    tree.Remove(index_key, transaction);
    //    std::ofstream ofs(prefix + std::to_string(n) + suffix);
    //    tree.Draw(bpm, ofs);
    int size = 0;
    for (auto i = tree.Begin(); i != tree.End(); ++i) {
      size++;
    }
    n++;
    std::cout << remove_keys.size() << "\n";
    EXPECT_EQ(size, keys.size() - n);
  }
  std::cout << tree.DrawBPlusTree() << "\n";
  auto size = 0;
  for (auto i = tree.Begin(); i != tree.End(); ++i) {
    std::cout << "iter : " << (*i).first.ToString() << "\n";
    size++;
  }

  EXPECT_EQ(keys.size() - remove_keys.size(), size);
  bool is_present;
  size = 0;

  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    is_present = tree.GetValue(index_key, &rids);

    if (!is_present) {
      std::cout << "not found: " << key << "\n";
      EXPECT_NE(std::find(remove_keys.begin(), remove_keys.end(), key), remove_keys.end());
    } else {
      EXPECT_EQ(rids.size(), 1);
      EXPECT_EQ(rids[0].GetPageId(), 0);
      EXPECT_EQ(rids[0].GetSlotNum(), key);
      size = size + 1;
    }
  }

  EXPECT_EQ(size, keys.size() - remove_keys.size());

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete bpm;
}
TEST(BPlusTreeTests, DISABLED_talps_delete1) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", header_page->GetPageId(), bpm, comparator, 7, 7);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

  std::vector<int64_t> keys = {};
  std::vector<int64_t> remove_keys = {};
  int scale = 10000;
  keys.reserve(scale);
  for (int i = 0; i < scale; i++) {
    keys.push_back(i);
    if (i % 2 == 0) {
      remove_keys.push_back(i);
    }
  }
  unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
  std::default_random_engine engine(seed);

  // 打乱vector
  std::shuffle(keys.begin(), keys.end(), engine);
  std::shuffle(remove_keys.begin(), remove_keys.end(), engine);
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    // std::cout << "i: " << key << "\n" << std::flush;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  // std::cout << tree.DrawBPlusTree();
  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int n = 0;
  for (auto key : remove_keys) {
    int size = 0;
    index_key.SetFromInteger(key);
    // std::cout << "d: " << key << "\n" << std::flush;
    tree.Remove(index_key, transaction);
    n++;
    for (auto i = tree.Begin(); i != tree.End(); ++i) {
      size++;
    }
    EXPECT_EQ(size, keys.size() - n);
  }

  int64_t size = 0;
  bool is_present;

  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    is_present = tree.GetValue(index_key, &rids);

    if (!is_present) {
      EXPECT_NE(std::find(remove_keys.begin(), remove_keys.end(), key), remove_keys.end());
    } else {
      EXPECT_EQ(rids.size(), 1);
      EXPECT_EQ(rids[0].GetPageId(), 0);
      EXPECT_EQ(rids[0].GetSlotNum(), key);
      size = size + 1;
    }
  }

  EXPECT_EQ(size, keys.size() - remove_keys.size());

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete bpm;
}
TEST(BPlusTreeTests, talps_delete2) {
  // create KeyComparator and index schema
  for (int x = 5; x < 88; x += 7) {
    auto key_schema = ParseCreateStatement("a bigint");
    GenericComparator<8> comparator(key_schema.get());

    auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
    auto *bpm = new BufferPoolManager(50, disk_manager.get());
    // create and fetch header_page
    page_id_t page_id;
    auto header_page = bpm->NewPage(&page_id);
    // create b+ tree
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", header_page->GetPageId(), bpm, comparator, x,
                                                             x + 2);
    GenericKey<8> index_key;
    RID rid;
    // create transaction
    auto *transaction = new Transaction(0);

    int scale = x * 100;
    std::vector<int64_t> keys = {};
    std::vector<int64_t> remove_keys = {};
    unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
    std::default_random_engine engine(seed);

    // 打乱vector
    std::shuffle(keys.begin(), keys.end(), engine);
    std::shuffle(remove_keys.begin(), remove_keys.end(), engine);
    keys.reserve(scale);
    for (int i = 0; i < scale; i++) {
      keys.push_back(i);
      if (i % 3 == 0) {
        remove_keys.push_back(i);
      }
    }
    for (auto key : keys) {
      int64_t value = key & 0xFFFFFFFF;
      rid.Set(static_cast<int32_t>(key >> 32), value);
      index_key.SetFromInteger(key);
      tree.Insert(index_key, rid, transaction);
    }

    std::vector<RID> rids;
    for (auto key : keys) {
      rids.clear();
      index_key.SetFromInteger(key);
      tree.GetValue(index_key, &rids);
      EXPECT_EQ(rids.size(), 1);

      int64_t value = key & 0xFFFFFFFF;
      EXPECT_EQ(rids[0].GetSlotNum(), value);
    }

    for (auto key : remove_keys) {
      index_key.SetFromInteger(key);
      tree.Remove(index_key, transaction);
    }

    int64_t size = 0;
    bool is_present;

    for (auto key : keys) {
      rids.clear();
      index_key.SetFromInteger(key);
      is_present = tree.GetValue(index_key, &rids);

      std::cout << key << "\n";
      if (!is_present) {
        EXPECT_NE(std::find(remove_keys.begin(), remove_keys.end(), key), remove_keys.end());
      } else {
        EXPECT_EQ(rids.size(), 1);
        EXPECT_EQ(rids[0].GetPageId(), 0);
        EXPECT_EQ(rids[0].GetSlotNum(), key);
        size = size + 1;
      }
    }

    EXPECT_EQ(size, keys.size() - remove_keys.size());

    bpm->UnpinPage(HEADER_PAGE_ID, true);
    delete transaction;
    delete bpm;
  }
}

}  // namespace bustub
