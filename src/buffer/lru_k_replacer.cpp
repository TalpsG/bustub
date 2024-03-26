//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <iostream>
#include <limits>
#include <list>
#include <stdexcept>
#include "common/config.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lkgd(latch_);
  auto now = std::chrono::steady_clock::now();

  // 将时间长度转换为毫秒
  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch());
  // 时间戳是毫秒数
  current_timestamp_ = static_cast<size_t>(duration.count());
  frame_id_t want = -1;
  bool infinity = false;
  size_t distance = 0;
  for (auto &f : node_store_) {
    if (!f.second.is_evictable_) {
      continue;
    }
    size_t dis_normal = current_timestamp_ - f.second.history_.back();
    if (want == -1) {
      want = f.first;
      infinity = k_ != f.second.k_;
      distance = infinity ? 0 : dis_normal;
      continue;
    }
    if (f.second.k_ < k_) {
      // f的distance 是inf
      // want 是上一个候选驱逐frame
      if (infinity) {
        // want 的distance 是inf
        // f的frame也是
        if (f.second.history_.back() < node_store_[want].history_.back()) {
          want = f.first;
        }
      } else {
        want = f.first;
        infinity = true;
        distance = 0;
      }
    } else {
      // f不是inf
      if (!infinity) {
        if (distance < dis_normal) {
          distance = dis_normal;
          want = f.first;
        }
      }
    }
  }
  if (want != -1) {
    node_store_.erase(want);
    --curr_size_;
    *frame_id = want;
    return true;
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> lkgd(latch_);
  auto now = std::chrono::system_clock::now();
  // 将时间长度转换为毫秒
  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch());
  // 时间戳是毫秒数
  current_timestamp_ = static_cast<size_t>(duration.count());
  if (node_store_.count(frame_id) == 0) {
    if (frame_id > static_cast<frame_id_t>(replacer_size_)) {
      throw std::runtime_error("RecordAccess: frame_id larger than replacer_size_");
    }
    LRUKNode temp;
    temp.history_.clear();
    temp.history_.push_front(current_timestamp_);
    temp.k_ = temp.history_.size();
    temp.fid_ = frame_id;
    temp.is_evictable_ = false;
    node_store_[frame_id] = temp;
  } else {
    auto &node = node_store_[frame_id];
    node.history_.push_front(current_timestamp_);
    if (node.k_ == k_) {
      node.history_.pop_back();
    } else {
      node.k_ = node.history_.size();
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lkgd(latch_);
  if (node_store_.count(frame_id) == 0) {
    throw std::runtime_error("SetEvictable: no such frame");
  }
  auto &node = node_store_[frame_id];
  if (node.is_evictable_ != set_evictable) {
    if (node.is_evictable_) {
      --curr_size_;
    } else {
      if (curr_size_ == replacer_size_) {
        throw std::runtime_error("SetEvictable : pool full");
      }
      ++curr_size_;
    }
    node.is_evictable_ = set_evictable;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lkgd(latch_);
  if (node_store_.count(frame_id) == 0) {
    return;
  }
  auto &node = node_store_[frame_id];
  if (!node.is_evictable_) {
    throw std::runtime_error("Remove: remove a non-evictable node");
  }
  node_store_.erase(frame_id);
  --curr_size_;
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lkgd(latch_);
  return curr_size_;
}

}  // namespace bustub
