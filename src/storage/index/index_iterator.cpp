/**
 * index_iterator.cpp
 */
#include <iostream>
#include <utility>
#include "buffer/buffer_pool_manager.h"
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
  if (page_id == INVALID_PAGE_ID) {
    pos_ = -1;
    return;
  }
  rpg_.emplace_back(bpm->FetchPageRead(page_id));
  page_ = rpg_.front().As<LeafPage>();
}
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return page_id_ == INVALID_PAGE_ID; }

INDEX_TEMPLATE_ARGUMENTS auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { return page_->GetPair(pos_); }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (IsEnd()) {
    pos_ = -1;
    return *this;
  }
  ++pos_;
  if (pos_ == page_->GetSize()) {
    page_id_ = page_->GetNextPageId();
    if (page_id_ == INVALID_PAGE_ID) {
      pos_ = -1;
      return *this;
    }
    rpg_.emplace_back(bpm_->FetchPageRead(page_id_));
    rpg_.pop_front();
    page_ = rpg_.back().As<LeafPage>();
    pos_ = 0;
  }
  // TODO(talps):
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
