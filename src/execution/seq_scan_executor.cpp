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
#include <memory>
#include <utility>
#include "common/exception.h"
#include "common/rid.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"
#include "storage/table/table_iterator.h"
#include "storage/table/tuple.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  auto txn = exec_ctx_->GetTransaction();
  switch (txn->GetIsolationLevel()) {
    case bustub::IsolationLevel::REPEATABLE_READ:
    case bustub::IsolationLevel::READ_COMMITTED: {
      bool is_downgrade = exec_ctx_->GetTransaction()->IsTableSharedLocked(plan_->table_oid_) ||
                          exec_ctx_->GetTransaction()->IsTableIntentionSharedLocked(plan_->table_oid_) ||
                          exec_ctx_->GetTransaction()->IsTableExclusiveLocked(plan_->table_oid_) ||
                          exec_ctx_->GetTransaction()->IsTableIntentionExclusiveLocked(plan_->table_oid_) ||
                          exec_ctx_->GetTransaction()->IsTableSharedIntentionExclusiveLocked(plan_->table_oid_);
      if (!is_downgrade) {
        exec_ctx_->GetLockManager()->LockTable(txn, LockManager::LockMode::INTENTION_SHARED, plan_->table_oid_);
      }
      break;
    }
    case bustub::IsolationLevel::READ_UNCOMMITTED: {
      break;
    }
  }
  iter_ = std::make_unique<TableIterator>(
      exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)->table_->MakeEagerIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  std::pair<TupleMeta, Tuple> ret;
  while (true) {
    if (iter_->IsEnd()) {
      return false;
    }
    *rid = iter_->GetRID();
    auto lock_mode = LockManager::LockMode::SHARED;
    auto txn = exec_ctx_->GetTransaction();
    switch (txn->GetIsolationLevel()) {
      case IsolationLevel::REPEATABLE_READ:
      case IsolationLevel::READ_COMMITTED: {
        if (!txn->IsRowExclusiveLocked(plan_->table_oid_, *rid)) {
          exec_ctx_->GetLockManager()->LockRow(txn, lock_mode, plan_->table_oid_, *rid);
        }
        break;
      }
      case IsolationLevel::READ_UNCOMMITTED:
        break;
    }
    // 如果是删除，需要对行加写锁
    if (exec_ctx_->IsDelete()) {
      try {
        exec_ctx_->GetLockManager()->LockTable(txn, LockManager::LockMode::INTENTION_EXCLUSIVE, plan_->table_oid_);
        exec_ctx_->GetLockManager()->LockRow(txn, LockManager::LockMode::EXCLUSIVE, plan_->table_oid_, *rid);
      } catch (TransactionAbortException &e) {
        throw ExecutionException(e.GetInfo());
        // just for diff
      }
    }
    auto [meta, tp] = iter_->GetTuple();
    ++(*iter_);
    if (!meta.is_deleted_) {
      // delete的filter被下推到了seqscan中
      if (plan_->filter_predicate_ != nullptr) {
        auto value = plan_->filter_predicate_->Evaluate(&tp, GetOutputSchema());
        if (!value.IsNull() && !value.GetAs<bool>()) {
          try {
            exec_ctx_->GetLockManager()->UnlockRow(txn, plan_->table_oid_, *rid, true);
          } catch (TransactionAbortException &e) {
            throw ExecutionException(e.GetInfo());
          }
          continue;
        }
      }
      if (!exec_ctx_->IsDelete()) {
        // 根据隔离级别解锁
        if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
          exec_ctx_->GetLockManager()->UnlockRow(txn, plan_->table_oid_, *rid);
        }
      }

      *tuple = tp;
      return true;
    }
    // 被删除了的行解锁
    if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      exec_ctx_->GetLockManager()->UnlockRow(txn, plan_->table_oid_, *rid, true);
    }
  }
  return false;
}
}  // namespace bustub
