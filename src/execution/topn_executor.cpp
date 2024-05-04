#include "execution/executors/topn_executor.h"
#include <queue>
#include <vector>
#include "storage/table/tuple.h"

namespace bustub {
using std::priority_queue;

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();
  results_.clear();
  Tuple tp;
  RID id;
  auto cmp = [&](const Tuple &t1, const Tuple &t2) {
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
  priority_queue<Tuple, std::vector<Tuple>, decltype(cmp)> pq(cmp);
  while (child_executor_->Next(&tp, &id)) {
    pq.push(tp);
    if (pq.size() == plan_->GetN() + 1) {
      pq.pop();
    }
  }
  auto size = pq.size();
  for (size_t i = 0; i < size; i++) {
    results_.push_front(pq.top());
    pq.pop();
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (results_.empty()) {
    return false;
  }
  *tuple = results_.front();
  *rid = tuple->GetRid();
  results_.pop_front();
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return results_.size(); };

}  // namespace bustub
