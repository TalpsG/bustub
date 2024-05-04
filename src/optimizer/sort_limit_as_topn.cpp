#include <memory>
#include "common/macros.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::Limit) {
    auto limit_plan = dynamic_cast<const LimitPlanNode *>(optimized_plan.get());
    if (limit_plan != nullptr) {
      BUSTUB_ENSURE(limit_plan->children_.size() == 1, "limit must have 1 child plan");
      const auto &sort_plan = limit_plan->children_[0];
      if (sort_plan->GetType() == PlanType::Sort) {
        auto p = dynamic_cast<const SortPlanNode *>(sort_plan.get());
        if (p != nullptr) {
          return std::make_shared<TopNPlanNode>(limit_plan->output_schema_, sort_plan->children_[0], p->order_bys_,
                                                limit_plan->GetLimit());
        }
      }
    }
  }
  return optimized_plan;
}

}  // namespace bustub
