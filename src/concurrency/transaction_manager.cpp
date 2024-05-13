//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <mutex>  // NOLINT
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "common/macros.h"
#include "storage/table/table_heap.h"
namespace bustub {

void TransactionManager::Commit(Transaction *txn) {
  // Release all the locks.
  ReleaseLocks(txn);

  txn->SetState(TransactionState::COMMITTED);
}

void TransactionManager::Abort(Transaction *txn) {
  /* TODO: revert all the changes in write set */
  auto write_set_ref = txn->GetWriteSet();
  while (!write_set_ref->empty()) {
    auto record = write_set_ref->back();
    switch (record.wtype_) {
      case WType::INSERT: {
        auto meta = record.table_heap_->GetTupleMeta(record.rid_);
        meta.is_deleted_ = true;
        record.table_heap_->UpdateTupleMeta(meta, record.rid_);
        break;
      }
      case WType::DELETE: {
        auto meta = record.table_heap_->GetTupleMeta(record.rid_);
        meta.is_deleted_ = false;
        record.table_heap_->UpdateTupleMeta(meta, record.rid_);
        break;
      }
      case WType::UPDATE:
        break;
    }
    write_set_ref->pop_back();
  }

  ReleaseLocks(txn);

  txn->SetState(TransactionState::ABORTED);
}

void TransactionManager::BlockAllTransactions() { UNIMPLEMENTED("block is not supported now!"); }

void TransactionManager::ResumeTransactions() { UNIMPLEMENTED("resume is not supported now!"); }

}  // namespace bustub
