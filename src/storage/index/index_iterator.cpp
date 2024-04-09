/**
 * index_iterator.cpp
 */
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
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, ReadPageGuard &&rpg, int pos)
    : bpm_(bpm), rpg_(std::move(rpg)), page_(rpg_.As<LeafPage>()), pos_(pos) {}
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  return page_->GetNextPageId() == INVALID_PAGE_ID && pos_ == page_->GetSize();
}

INDEX_TEMPLATE_ARGUMENTS auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { return page_->GetPair(pos_); }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (page_->GetNextPageId() == INVALID_PAGE_ID) {
    if (page_->GetSize() != pos_) {
      pos_ += 1;
    }
  } else {
    pos_ += 1;
    if (page_->GetSize() == pos_) {
      rpg_ = bpm_->FetchPageRead(page_->GetNextPageId());
      pos_ = 0;
    } else {
      pos_ += 1;
    }
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
