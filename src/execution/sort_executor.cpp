#include "execution/executors/sort_executor.h"
#include <algorithm>
#include <iostream>
#include <utility>
#include "binder/bound_order_by.h"
#include "type/type.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  result_.clear();
  Tuple tp;
  RID id;
  while (child_executor_->Next(&tp, &id)) {
    result_.push_back(tp);
  }
  auto cmp = [&](const Tuple &t1, const Tuple &t2) -> bool {
    for (const auto &expr : plan_->order_bys_) {
      if (expr.first == OrderByType::DEFAULT || expr.first == OrderByType::ASC) {
        auto v1 = expr.second->Evaluate(&t1, child_executor_->GetOutputSchema());
        auto v2 = expr.second->Evaluate(&t2, child_executor_->GetOutputSchema());
        if (v1.CompareEquals(v2) != CmpBool::CmpTrue) {
          return v1.CompareLessThan(v2) == CmpBool::CmpTrue;
        }
      } else if (expr.first == OrderByType::DESC) {
        auto v1 = expr.second->Evaluate(&t1, child_executor_->GetOutputSchema());
        auto v2 = expr.second->Evaluate(&t2, child_executor_->GetOutputSchema());
        if (v1.CompareEquals(v2) != CmpBool::CmpTrue) {
          return v1.CompareGreaterThan(v2) == CmpBool::CmpTrue;
        }
      }
    }
    return true;
  };
  std::sort(result_.begin(), result_.end(), cmp);
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (result_.empty()) {
    return false;
  }
  *tuple = result_.front();
  *rid = tuple->GetRid();
  result_.pop_front();
  return true;
}

}  // namespace bustub
