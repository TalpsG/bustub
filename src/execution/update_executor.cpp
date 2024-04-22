//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <iostream>
#include <memory>
#include <vector>
#include "storage/table/tuple.h"
#include "type/type_id.h"

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  std::cout << "update ctor\n";
}

void UpdateExecutor::Init() {
  std::cout << "update init\n";
  child_executor_->Init();
  first_ = true;
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  auto indices = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  Tuple tp;
  RID id;
  int num = 0;
  while (child_executor_->Next(&tp, &id)) {
    // 找到旧tuple
    num++;
    TupleMeta meta = table_info_->table_->GetTupleMeta(tp.GetRid());
    meta.is_deleted_ = true;
    table_info_->table_->UpdateTupleMeta(meta, tp.GetRid());
    // 删除索引
    for (auto index : indices) {
      index->index_->DeleteEntry(
          tp.KeyFromTuple(child_executor_->GetOutputSchema(), index->key_schema_, index->index_->GetKeyAttrs()),
          tp.GetRid(), exec_ctx_->GetTransaction());
    }

    std::vector<Value> values;
    values.reserve(plan_->target_expressions_.size());
    // 获取新数据
    for (const auto &expr : plan_->target_expressions_) {
      values.push_back(expr->Evaluate(&tp, child_executor_->GetOutputSchema()));
    }
    Tuple new_tuple{values, &child_executor_->GetOutputSchema()};
    auto option_rid = table_info_->table_->InsertTuple(TupleMeta{INVALID_TXN_ID, INVALID_TXN_ID, false}, new_tuple);
    if (option_rid.has_value()) {
      id = option_rid.value();
    }
    for (auto index : indices) {
      index->index_->InsertEntry(
          new_tuple.KeyFromTuple(child_executor_->GetOutputSchema(), index->key_schema_, index->index_->GetKeyAttrs()),
          id, exec_ctx_->GetTransaction());
    }
  }
  if (num == 0 && !first_) {
    return false;
  }
  first_ = false;
  std::vector<Value> values{};
  values.emplace_back(TypeId::INTEGER, num);
  *tuple = {values, &GetOutputSchema()};
  return true;
}

}  // namespace bustub
