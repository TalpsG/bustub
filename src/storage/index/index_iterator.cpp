/**
 * index_iterator.cpp
 */
#include <cassert>
#include <utility>
#include "common/config.h"
#include "storage/page/page_guard.h"

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, page_id_t page_id, int pos)
    : bpm_(bpm), page_id_(page_id), pos_(pos) {
  if (page_id_ == INVALID_PAGE_ID) {
    return;
  }
  rpg_ = bpm_->FetchPageRead(page_id_);
  page_ = rpg_.As<LeafPage>();
}
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return page_id_ == INVALID_PAGE_ID && pos_ == -1; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator=(IndexIterator &&iter) noexcept -> IndexIterator & {
  if (&iter == this) {
    return *this;
  }
  this->page_id_ = iter.page_id_;
  this->pos_ = iter.pos_;
  this->bpm_ = iter.bpm_;
  this->page_ = iter.page_;
  this->rpg_ = std::move(iter.rpg_);
  return *this;
}
INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { return page_->PairAt(pos_); }

INDEX_TEMPLATE_ARGUMENTS auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (page_id_ == INVALID_PAGE_ID) {
    return *this;
  }
  pos_++;
  if (pos_ == page_->GetSize()) {
    if (page_->GetNextPageId() == INVALID_PAGE_ID) {
      page_ = nullptr;
      pos_ = -1;
      page_id_ = INVALID_PAGE_ID;
    } else {
      rpg_ = std::move(bpm_->FetchPageRead(page_->GetNextPageId()));
      page_ = rpg_.As<LeafPage>();
      pos_ = 0;
      page_id_ = rpg_.PageId();
    }
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
