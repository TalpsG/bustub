//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <memory>
#include <utility>
#include <vector>
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "fmt/core.h"
#include "type/type_id.h"
#include "type/value.h"

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  child_executor_->Init();
  is_first_ = true;
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  auto table_id = plan_->table_oid_;
  auto child_plan = plan_->GetChildPlan();
  auto table = exec_ctx_->GetCatalog()->GetTable(table_id);
  auto indices = exec_ctx_->GetCatalog()->GetTableIndexes(table->name_);
  Tuple tp;
  RID rd;
  int sum = 0;
  while (child_executor_->Next(&tp, &rd)) {
    ++sum;
    auto option_rid = table->table_->InsertTuple(TupleMeta{INVALID_TXN_ID, INVALID_TXN_ID, false}, tp);
    if (option_rid.has_value()) {
      rd = option_rid.value();
    }
    for (auto index : indices) {
      index->index_->InsertEntry(
          tp.KeyFromTuple(child_plan->OutputSchema(), index->key_schema_, index->index_->GetKeyAttrs()), rd,
          exec_ctx_->GetTransaction());
    }
  }
  std::vector<Value> values{};
  if (sum == 0 && !is_first_) {
    return false;
  }
  is_first_ = false;
  values.emplace_back(TypeId::INTEGER, sum);
  *tuple = Tuple{values, &GetOutputSchema()};
  return true;
}

}  // namespace bustub
