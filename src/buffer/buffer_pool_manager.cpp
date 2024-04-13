//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include <cstddef>
#include <iostream>
#include <stdexcept>

#include "common/config.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  for (size_t i = 0; i < pool_size_; ++i) {
    pages_[i].page_id_ = INVALID_PAGE_ID;
    pages_[i].pin_count_ = 0;
    pages_[i].is_dirty_ = false;
  }
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> lkgd(latch_);
  // 是否有可用的frame
  Page *p = nullptr;
  if (!free_list_.empty()) {
    // 有空余的frame
    // 取frame_id 和page_id绑定
    // 初始化page结构体
    frame_id_t frame_id = free_list_.front();
    free_list_.pop_front();
    *page_id = AllocatePage();
    for (size_t i = 0; i < pool_size_; ++i) {
      if (pages_[i].page_id_ == INVALID_PAGE_ID) {
        p = &pages_[i];
        break;
      }
    }
    p->page_id_ = *page_id;
    p->is_dirty_ = false;
    p->pin_count_ = 1;
    page_table_[*page_id] = frame_id;
    frame_table_[frame_id] = p;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
  } else {
    // 需要驱逐获得frame
    frame_id_t frame_id = 0;
    if (!replacer_->Evict(&frame_id)) {
      // 驱逐失败
      return nullptr;
    }
    p = frame_table_[frame_id];
    page_table_.erase(p->page_id_);
    *page_id = AllocatePage();
    if (p->IsDirty()) {
      disk_manager_->WritePage(p->page_id_, p->GetData());
      p->is_dirty_ = false;
    }
    p->ResetMemory();
    p->is_dirty_ = false;
    p->page_id_ = *page_id;
    p->pin_count_ = 1;
    page_table_[*page_id] = frame_id;
    frame_table_[frame_id] = p;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
  }
  return p;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard<std::mutex> lkgd(latch_);
  frame_id_t frame_id = 0;
  if (page_table_.count(page_id) == 0) {
    // page 不在buffer 中
    if (free_list_.empty()) {
      // 没有空余的frame
      frame_id = 0;
      if (!replacer_->Evict(&frame_id)) {
        // 无法驱逐
        return nullptr;
      }
      Page *p = frame_table_[frame_id];
      if (p->IsDirty()) {
        disk_manager_->WritePage(p->page_id_, p->GetData());
        p->is_dirty_ = false;
      }
      // 移除老的 frame 和 pagetable中的记录
      page_table_.erase(p->page_id_);
      frame_table_[frame_id] = p;
      p->page_id_ = page_id;
      p->is_dirty_ = false;
      p->pin_count_ = 1;
      p->ResetMemory();
      disk_manager_->ReadPage(page_id, p->GetData());
      page_table_[page_id] = frame_id;
      replacer_->RecordAccess(frame_id);
      replacer_->SetEvictable(frame_id, false);
    } else {
      // 有空余的frame
      frame_id = free_list_.front();
      Page *p = nullptr;
      free_list_.pop_front();
      for (size_t i = 0; i < pool_size_; ++i) {
        if (pages_[i].page_id_ == INVALID_PAGE_ID) {
          p = &pages_[i];
          break;
        }
      }
      if (p == nullptr) {
        free_list_.push_front(frame_id);
        return nullptr;
      }
      page_table_[page_id] = frame_id;
      frame_table_[frame_id] = p;
      p->page_id_ = page_id;
      p->is_dirty_ = false;
      p->pin_count_ = 1;
      p->ResetMemory();
      disk_manager_->ReadPage(page_id, p->GetData());
      replacer_->RecordAccess(frame_id);
      replacer_->SetEvictable(frame_id, false);
    }
  } else {
    frame_id = page_table_[page_id];
    Page *p = frame_table_[frame_id];
    ++p->pin_count_;
    replacer_->RecordAccess(frame_id);
    if (p->pin_count_ == 1) {
      replacer_->SetEvictable(frame_id, false);
    }
  }
  return frame_table_[frame_id];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard<std::mutex> lkgd(latch_);
  if (page_table_.count(page_id) == 0) {
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  Page *p = frame_table_[frame_id];
  if (p->pin_count_ == 0) {
    return false;
  }
  --p->pin_count_;
  p->is_dirty_ |= is_dirty;
  if (p->pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }

  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lkgd(latch_);
  if (page_table_.count(page_id) == 0) {
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  Page *p = frame_table_[frame_id];
  disk_manager_->WritePage(page_id, p->GetData());
  p->is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::lock_guard<std::mutex> lkgd(latch_);
  auto iter = page_table_.begin();
  while (iter != page_table_.end()) {
    auto page_id = iter->first;
    disk_manager_->WritePage(page_id, frame_table_[page_table_[page_id]]->GetData());
    frame_table_[page_table_[page_id]]->is_dirty_ = false;
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lkgd(latch_);
  if (page_table_.count(page_id) == 0) {
    return true;
  }
  frame_id_t frame_id = page_table_[page_id];
  Page *p = frame_table_[frame_id];
  if (p->pin_count_ > 0) {
    return false;
  }
  if (p->IsDirty()) {
    disk_manager_->WritePage(p->page_id_, p->GetData());
    p->is_dirty_ = false;
  }
  page_table_.erase(page_id);
  frame_table_.erase(frame_id);
  free_list_.push_back(frame_id);
  p->page_id_ = INVALID_PAGE_ID;
  p->is_dirty_ = false;
  p->ResetMemory();
  p->pin_count_ = 0;
  replacer_->Remove(frame_id);

  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  auto p = FetchPage(page_id);
  return {this, p};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  auto p = FetchPage(page_id);
  p->RLatch();
  return {this, p};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  auto p = FetchPage(page_id);
  p->WLatch();
  return {this, p};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, NewPage(page_id)}; }
auto BufferPoolManager::NewWriteGuarded(page_id_t *page_id) -> WritePageGuard {
  auto p = NewPage(page_id);
  p->WLatch();
  return {this, p};
}

}  // namespace bustub
