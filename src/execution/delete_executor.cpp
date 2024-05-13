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
  done_ = false;
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (done_) {
    return false;
  }
  int num = 0;
  auto indices = catalog_->GetTableIndexes(table_->name_);
  while (child_executor_->Next(tuple, rid)) {
    auto meta = table_->table_->GetTupleMeta(tuple->GetRid());
    meta.is_deleted_ = true;
    table_->table_->UpdateTupleMeta(meta, tuple->GetRid());
    auto write_record = TableWriteRecord(table_->oid_, tuple->GetRid(), table_->table_.get());
    write_record.wtype_ = WType::DELETE;
    exec_ctx_->GetTransaction()->AppendTableWriteRecord(write_record);
    for (auto index : indices) {
      index->index_->DeleteEntry(
          tuple->KeyFromTuple(child_executor_->GetOutputSchema(), index->key_schema_, index->index_->GetKeyAttrs()),
          tuple->GetRid(), exec_ctx_->GetTransaction());
    }
    num++;
  }
  std::vector<Value> values{};
  values.emplace_back(TypeId::INTEGER, num);
  *tuple = Tuple{values, &GetOutputSchema()};
  done_ = true;
  return true;
}

}  // namespace bustub
