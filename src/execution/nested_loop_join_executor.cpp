//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include <iostream>
#include <utility>
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "storage/table/tuple.h"
#include "type/type.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  Tuple tp1;
  RID id1;
  while (right_executor_->Next(&tp1, &id1)) {
    right_table_.push_back(tp1);
  }
  right_id_ = 0;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple tp1;
  RID id1;
  if (!result_.empty()) {
    right_executor_->Init();
    *tuple = result_.front();
    result_.pop_front();
    return true;
  }
  if (plan_->join_type_ == JoinType::LEFT) {
    right_id_ = 0;
    right_executor_->Init();
    if (!left_executor_->Next(&tp1, &id1)) {
      return false;
    }
    // 当一次遍历内表没有找到时要有一个null的tuple
    bool found = false;
    while (right_id_ != right_table_.size()) {
      auto tp2 = right_table_[right_id_++];
      auto value = plan_->predicate_->EvaluateJoin(&tp1, left_executor_->GetOutputSchema(), &tp2,
                                                   right_executor_->GetOutputSchema());
      if (!value.IsNull() && value.GetAs<bool>()) {
        found = true;
        std::vector<Value> values{};
        values.reserve(left_executor_->GetOutputSchema().GetColumnCount() +
                       right_executor_->GetOutputSchema().GetColumnCount());
        for (size_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
          values.push_back(tp1.GetValue(&left_executor_->GetOutputSchema(), i));
        }
        for (size_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
          values.push_back(tp2.GetValue(&right_executor_->GetOutputSchema(), i));
        }
        result_.emplace_back(values, &GetOutputSchema());
      }
    }
    if (!found) {
      std::vector<Value> values{};
      values.reserve(left_executor_->GetOutputSchema().GetColumnCount() +
                     right_executor_->GetOutputSchema().GetColumnCount());
      for (size_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
        values.push_back(tp1.GetValue(&left_executor_->GetOutputSchema(), i));
      }
      for (size_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
        values.push_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(i).GetType()));
      }
      result_.emplace_back(values, &GetOutputSchema());
    }
  } else {
    // 内连接
    while (left_executor_->Next(&tp1, &id1)) {
      bool found = false;
      right_executor_->Init();
      Tuple tp2;
      RID id2;
      while (right_executor_->Next(&tp2, &id2)) {
        auto value = plan_->predicate_->EvaluateJoin(&tp1, left_executor_->GetOutputSchema(), &tp2,
                                                     right_executor_->GetOutputSchema());
        if (!value.IsNull() && value.GetAs<bool>()) {
          found = true;
          std::vector<Value> values{};
          values.reserve(left_executor_->GetOutputSchema().GetColumnCount() +
                         right_executor_->GetOutputSchema().GetColumnCount());
          for (size_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
            values.push_back(tp1.GetValue(&left_executor_->GetOutputSchema(), i));
          }
          for (size_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
            values.push_back(tp2.GetValue(&right_executor_->GetOutputSchema(), i));
          }
          result_.emplace_back(values, &GetOutputSchema());
        }
      }
      if (found) {
        break;
      }
    }
  }
  if (!result_.empty()) {
    *tuple = result_.front();
    result_.pop_front();
    return true;
  }

  return false;
}

}  // namespace bustub
