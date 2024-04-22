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

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {
  std::cout << "agg ctor\n";
}

void AggregationExecutor::Init() { std::cout << "agg init\n"; }

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  child_->Init();
  aht_iterator_ = aht_.Begin();
  Tuple tp;
  RID id;
  std::vector<Value> result;
  std::vector<std::vector<Value>> input;
  while (child_->Next(&tp, &id)) {
    std::cout << tp.ToString(&GetOutputSchema()) << '\n';
  }
  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
