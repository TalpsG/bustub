//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <deque>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/executors/aggregation_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"
#include "type/type.h"

namespace bustub {

struct HashKey {
  std::vector<Value> keys_;
  auto operator==(const HashKey &key) const -> bool {
    if (keys_.size() != key.keys_.size()) {
      return false;
    }
    for (size_t i = 0; i < keys_.size(); ++i) {
      if (keys_[i].CompareEquals(key.keys_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};
struct HashValue {
  std::vector<Value> values_;
  bool has_join_{false};
};
class MyClassHash {
 public:
  auto operator()(const HashKey &obj) const -> std::size_t {
    size_t curr_hash = 0;
    for (const auto &key : obj.keys_) {
      if (!key.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return curr_hash;
  }
};
/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> left_executor_;
  std::unique_ptr<AbstractExecutor> right_executor_;
  std::unordered_map<HashKey, std::deque<HashValue>, MyClassHash> ht_kv_;
  std::unordered_set<HashKey, MyClassHash> key_set_;
  std::unordered_multimap<HashKey, HashValue, MyClassHash> ht_k_;
  std::unordered_multimap<HashKey, HashValue, MyClassHash>::iterator ht_iter_;
  std::deque<Tuple> results_;
};

}  // namespace bustub
