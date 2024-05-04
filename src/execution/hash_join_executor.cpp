//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include <iostream>
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  Value v;
}

void HashJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  ht_k_.clear();
  ht_kv_.clear();
  results_.clear();
  key_set_.clear();
  Tuple tp;
  RID id;
  while (left_executor_->Next(&tp, &id)) {
    HashKey key;
    for (const auto &left_key_expression : plan_->left_key_expressions_) {
      key.keys_.push_back(left_key_expression->Evaluate(&tp, left_executor_->GetOutputSchema()));
    }
    key_set_.insert(key);
    HashValue value;
    for (size_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
      value.values_.push_back(tp.GetValue(&left_executor_->GetOutputSchema(), i));
    }
    ht_k_.insert({key, value});
  }
  while (right_executor_->Next(&tp, &id)) {
    HashKey key;
    for (const auto &right_key_expression : plan_->right_key_expressions_) {
      key.keys_.push_back(right_key_expression->Evaluate(&tp, right_executor_->GetOutputSchema()));
    }
    if (ht_k_.count(key) != 0) {
      auto range = ht_k_.equal_range(key);
      for (auto iter = range.first; iter != range.second; iter++) {
        HashValue value;
        for (const auto &v : iter->second.values_) {
          value.values_.push_back(v);
        }
        for (size_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
          value.values_.push_back(tp.GetValue(&right_executor_->GetOutputSchema(), i));
        }
        ht_kv_[key].push_back(value);
      }
    }
  }
  if (plan_->join_type_ == JoinType::LEFT) {
    for (const auto &key : key_set_) {
      // keyset 存放所有左表的key
      auto range = ht_k_.equal_range(key);
      // 遍历左表key 的所有左表中的tuple
      if (ht_kv_.count(key) == 0) {
        // 如果没有则生成null
        for (auto iter = range.first; iter != range.second; iter++) {
          auto value = iter->second.values_;
          for (size_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
            value.push_back(
                ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(i).GetType()));
          }
          results_.emplace_back(value, &GetOutputSchema());
        }
      } else {
        // 如果有则对生成tuple
        for (const auto &v : ht_kv_[key]) {
          auto value = v.values_;
          results_.emplace_back(value, &GetOutputSchema());
        }
      }
    }
  } else {
    // 内连接
    for (const auto &key : key_set_) {
      // keyset 存放所有左表的key
      if (ht_kv_.count(key) != 0) {
        for (const auto &v : ht_kv_[key]) {
          auto value = v.values_;
          results_.emplace_back(value, &GetOutputSchema());
        }
      }
    }
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (results_.empty()) {
    return false;
  }
  *tuple = results_.front();
  results_.pop_front();
  return true;
}

}  // namespace bustub
