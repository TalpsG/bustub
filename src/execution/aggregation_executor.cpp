//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <iostream>
#include <memory>
#include <vector>
#include "execution/plans/aggregation_plan.h"
#include "type/type.h"

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  child_->Init();
  aht_.Clear();
  auto group_by = plan_->group_bys_;
  auto agg = plan_->aggregates_;
  Tuple tp;
  RID id;
  bool empty_table = true;
  while (child_->Next(&tp, &id)) {
    empty_table = false;
    aht_.InsertCombine(MakeAggregateKey(&tp), MakeAggregateValue(&tp));
  }
  if (empty_table) {
    if (group_by.empty()) {
      aht_.InsertEmpty();
    }
  }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (aht_iterator_ == aht_.End()) {
    return false;
  }
  auto result_value = aht_iterator_.Val().aggregates_;
  auto result = aht_iterator_.Key().group_bys_;
  for (const auto &i : result_value) {
    result.push_back(i);
  }
  *tuple = Tuple{result, &GetOutputSchema()};
  *rid = tuple->GetRid();
  ++aht_iterator_;
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
