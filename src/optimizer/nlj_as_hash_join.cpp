#include <algorithm>
#include <iostream>
#include <memory>
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Spring: You should at least support join keys of the form:
  // 1. <column expr> = <column expr>
  // 2. <column expr> = <column expr> AND <column expr> = <column expr>

  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nested_loop_join_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    // Has exactly one child
    BUSTUB_ENSURE(optimized_plan->children_.size() == 2, "NLJ must have to child plan");
    // If the schema is the same (except column name)

    const auto &left_child = optimized_plan->children_[0];
    const auto &right_child = optimized_plan->children_[1];
    std::vector<AbstractExpressionRef> left_exprs;
    std::vector<AbstractExpressionRef> right_exprs;

    if (const auto *expr = dynamic_cast<const ComparisonExpression *>(nested_loop_join_plan.predicate_.get());
        expr != nullptr) {
      if (expr->comp_type_ == ComparisonType::Equal) {
        if (const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[0].get());
            left_expr != nullptr) {
          if (const auto *right_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[1].get());
              right_expr != nullptr) {
            left_exprs.push_back(expr->children_[0]);
            right_exprs.push_back(expr->children_[1]);
            if (left_expr->GetTupleIdx() == 0 && right_expr->GetTupleIdx() == 1) {
              return std::make_shared<HashJoinPlanNode>(nested_loop_join_plan.output_schema_, left_child, right_child,
                                                        left_exprs, right_exprs, nested_loop_join_plan.join_type_);
            }
            return std::make_shared<HashJoinPlanNode>(nested_loop_join_plan.output_schema_, left_child, right_child,
                                                      right_exprs, left_exprs, nested_loop_join_plan.join_type_);
          }
        }
      }
    }
    if (const auto *expr = dynamic_cast<const LogicExpression *>(nested_loop_join_plan.predicate_.get());
        expr != nullptr) {
      BUSTUB_ENSURE(expr->children_.size() == 2, "logic expression only have 2 children");
      if (expr->logic_type_ == LogicType::And) {
        if (const auto *left_expr = dynamic_cast<const ComparisonExpression *>(expr->children_[0].get());
            left_expr != nullptr) {
          if (left_expr->comp_type_ == ComparisonType::Equal) {
            if (const auto *right_expr = dynamic_cast<const ComparisonExpression *>(expr->children_[1].get());
                right_expr != nullptr) {
              if (right_expr->comp_type_ == ComparisonType::Equal) {
                // 需要修改plan的结构
                BUSTUB_ENSURE(left_expr->children_.size() == 2, "comparison_expression children must be two");
                BUSTUB_ENSURE(right_expr->children_.size() == 2, "comparison_expression children must be two");
                std::vector<const ColumnValueExpression *> old_exprs;
                old_exprs.push_back(dynamic_cast<const ColumnValueExpression *>(left_expr->children_[0].get()));
                old_exprs.push_back(dynamic_cast<const ColumnValueExpression *>(left_expr->children_[1].get()));
                old_exprs.push_back(dynamic_cast<const ColumnValueExpression *>(right_expr->children_[0].get()));
                old_exprs.push_back(dynamic_cast<const ColumnValueExpression *>(right_expr->children_[1].get()));
                for (auto i : old_exprs) {
                  if (i->GetTupleIdx() == 0) {
                    left_exprs.push_back(
                        std::make_shared<ColumnValueExpression>(0, i->GetColIdx(), i->GetReturnType()));
                  } else if (i->GetTupleIdx() == 1) {
                    right_exprs.push_back(
                        std::make_shared<ColumnValueExpression>(1, i->GetColIdx(), i->GetReturnType()));
                  }
                }

                return std::make_shared<HashJoinPlanNode>(nested_loop_join_plan.output_schema_, left_child, right_child,
                                                          left_exprs, right_exprs, nested_loop_join_plan.join_type_);
                //
                //              return std::make_shared<HashJoinPlanNode>(nested_loop_join_plan.output_schema_,
                //              left_child, right_child,
                //                                                        left_exprs, right_exprs,
                //                                                        nested_loop_join_plan.join_type_);
              }
            }
          }
        }
      }
    }
  }

  return optimized_plan;
}

}  // namespace bustub
