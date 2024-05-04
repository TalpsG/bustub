//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <memory>
#include <vector>
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  catalog_ = exec_ctx_->GetCatalog();
  table_ = catalog_->GetTable(plan_->table_oid_);
  child_executor_->Init();
  first_ = true;
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  auto indices = catalog_->GetTableIndexes(table_->name_);
  Tuple tp;
  RID id;
  int num = 0;
  while (child_executor_->Next(&tp, &id)) {
    auto meta = table_->table_->GetTupleMeta(tp.GetRid());
    meta.is_deleted_ = true;
    table_->table_->UpdateTupleMeta(meta, tp.GetRid());
    num++;
    for (auto index : indices) {
      index->index_->DeleteEntry(
          tp.KeyFromTuple(child_executor_->GetOutputSchema(), index->key_schema_, index->index_->GetKeyAttrs()),
          tp.GetRid(), exec_ctx_->GetTransaction());
    }
  }
  if (num == 0 && !first_) {
    return false;
  }
  std::vector<Value> values{};
  values.emplace_back(TypeId::INTEGER, num);
  *tuple = Tuple{values, &GetOutputSchema()};
  first_ = false;
  return true;
}

}  // namespace bustub
