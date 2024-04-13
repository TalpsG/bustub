#include "storage/page/page_guard.h"
#include <utility>
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept
    : bpm_(that.bpm_), page_(that.page_), is_dirty_(that.is_dirty_) {
  that.page_ = nullptr;
  that.bpm_ = nullptr;
  that.is_dirty_ = false;
}

void BasicPageGuard::Drop() {
  if (page_ != nullptr) {
    if (is_dirty_) {
      bpm_->FlushPage(PageId());
    }
    bpm_->UnpinPage(PageId(), false);
  }
  page_ = nullptr;
  bpm_ = nullptr;
  is_dirty_ = false;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  if (&that == this) {
    return *this;
  }
  // 释放自己的资源
  if (page_ != nullptr) {
    if (is_dirty_) {
      bpm_->FlushPage(PageId());
      is_dirty_ = false;
    }
    bpm_->UnpinPage(PageId(), false);
  }
  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;

  that.page_ = nullptr;
  that.bpm_ = nullptr;
  that.is_dirty_ = false;

  return *this;
}

BasicPageGuard::~BasicPageGuard() {
  if (page_ != nullptr) {
    if (is_dirty_) {
      bpm_->FlushPage(PageId());
      is_dirty_ = false;
    }
    bpm_->UnpinPage(PageId(), false);
  }
  page_ = nullptr;
  is_dirty_ = false;
  bpm_ = nullptr;
};  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
  if (&that == this) {
    return;
  }
  if (guard_.page_ != nullptr) {
    guard_.page_->RUnlatch();
  }
  guard_ = std::move(that.guard_);
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  // 构造时会自动上锁，因此此处直接移动即可。
  // 移动会销毁之前的guard_内的相关元素
  if (&that == this) {
    return *this;
  }
  if (guard_.page_ != nullptr) {
    if (guard_.is_dirty_) {
      guard_.bpm_->FlushPage(guard_.PageId());
      guard_.is_dirty_ = false;
    }
    guard_.bpm_->UnpinPage(guard_.PageId(), false);
    guard_.page_->RUnlatch();
  }
  guard_.page_ = nullptr;
  guard_ = std::move(that.guard_);
  return *this;
}

void ReadPageGuard::Drop() {
  if (guard_.page_ != nullptr) {
    if (guard_.is_dirty_) {
      guard_.bpm_->FlushPage(guard_.PageId());
      guard_.is_dirty_ = false;
    }
    guard_.bpm_->UnpinPage(guard_.PageId(), false);
    guard_.page_->RUnlatch();
  }
  guard_.bpm_ = nullptr;
  guard_.page_ = nullptr;
  guard_.is_dirty_ = false;
}

ReadPageGuard::~ReadPageGuard() {
  if (guard_.page_ != nullptr) {
    if (guard_.is_dirty_) {
      guard_.bpm_->FlushPage(guard_.PageId());
      guard_.is_dirty_ = false;
    }
    guard_.bpm_->UnpinPage(guard_.PageId(), false);
    guard_.page_->RUnlatch();
  }
  guard_.page_ = nullptr;
  guard_.bpm_ = nullptr;
  guard_.is_dirty_ = false;
}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  if (&that == this) {
    return;
  }
  if (guard_.page_ != nullptr) {
    guard_.page_->WUnlatch();
  }
  guard_ = std::move(that.guard_);
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (&that == this) {
    return *this;
  }
  if (guard_.page_ != nullptr) {
    if (guard_.is_dirty_) {
      guard_.bpm_->FlushPage(guard_.PageId());
      guard_.is_dirty_ = false;
    }
    guard_.bpm_->UnpinPage(guard_.PageId(), false);
    guard_.page_->WUnlatch();
  }
  guard_.page_ = nullptr;
  guard_ = std::move(that.guard_);

  return *this;
}

void WritePageGuard::Drop() {
  if (guard_.page_ != nullptr) {
    if (guard_.is_dirty_) {
      guard_.bpm_->FlushPage(guard_.PageId());
      guard_.is_dirty_ = false;
    }
    guard_.bpm_->UnpinPage(guard_.PageId(), false);
    guard_.page_->WUnlatch();
  }
  guard_.page_ = nullptr;
  guard_.bpm_ = nullptr;
  guard_.is_dirty_ = false;
}

WritePageGuard::~WritePageGuard() {
  if (guard_.page_ != nullptr) {
    if (guard_.is_dirty_) {
      guard_.bpm_->FlushPage(guard_.PageId());
      guard_.is_dirty_ = false;
    }
    guard_.bpm_->UnpinPage(guard_.PageId(), false);
    guard_.page_->WUnlatch();
  }
  guard_.page_ = nullptr;
  guard_.bpm_ = nullptr;
  guard_.is_dirty_ = false;
}  // NOLINT
}  // namespace bustub
