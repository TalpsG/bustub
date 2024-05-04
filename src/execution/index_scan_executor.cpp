//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include <iostream>
#include "common/config.h"
#include "storage/index/b_plus_tree_index.h"
#include "storage/table/tuple.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  catalog_ = exec_ctx_->GetCatalog();
  index_info_ = catalog_->GetIndex(plan_->index_oid_);
  index_ = reinterpret_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info_->index_.get());
  iter_ = index_->GetBeginIterator();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  TupleMeta meta;
  while (true) {
    if (iter_.IsEnd()) {
      return false;
    }
    *rid = (*iter_).second;
    if (rid->GetPageId() == INVALID_PAGE_ID) {
      ++iter_;
      continue;
    }
    meta = catalog_->GetTable(index_info_->table_name_)->table_->GetTupleMeta(*rid);
    if (meta.is_deleted_) {
      ++iter_;
    } else {
      break;
    }
  }
  *tuple = catalog_->GetTable(index_info_->table_name_)->table_->GetTuple(*rid).second;
  ++iter_;
  return true;
}

}  // namespace bustub
