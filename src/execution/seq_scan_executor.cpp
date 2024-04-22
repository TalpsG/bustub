//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include <iostream>
#include <utility>
#include "common/rid.h"
#include "storage/table/table_iterator.h"
#include "storage/table/tuple.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      iter_(exec_ctx_->GetCatalog()->GetTable(plan->table_oid_)->table_->MakeIterator()) {
  std::cout << "seq scan ctor\n";
}

void SeqScanExecutor::Init() { std::cout << "seq scan init\n"; }

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  std::pair<TupleMeta, Tuple> ret;
  while (true) {
    if (iter_.IsEnd()) {
      return false;
    }
    ret = iter_.GetTuple();
    if (ret.first.is_deleted_) {
      ++iter_;
    } else {
      break;
    }
  }
  *tuple = ret.second;
  *rid = ret.second.GetRid();
  ++iter_;
  return true;
}
}  // namespace bustub
