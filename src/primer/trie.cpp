#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  if (root_ == nullptr) {
    return nullptr;
  }
  auto p = root_;
  for (auto i : key) {
    if (p->children_.count(i) == 0) {
      return nullptr;
    }
    p = p->children_.at(i);
  }
  auto node_p = dynamic_cast<const TrieNodeWithValue<T> *>(p.get());
  if (node_p == nullptr) {
    return nullptr;
  }
  return node_p->value_.get();

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
  Trie tree;
  std::shared_ptr<TrieNode> now;
  if (root_ == nullptr) {
    if (key.empty()) {
      now = std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
      tree.root_ = now;
      return tree;
    }
    now = std::make_shared<TrieNode>();
  } else {
    if (key.empty()) {
      now = std::make_shared<TrieNodeWithValue<T>>(root_->children_, std::make_shared<T>(std::move(value)));
      tree.root_ = now;
      return tree;
    }
    now = root_->Clone();
  }
  tree.root_ = now;
  for (auto iter = key.begin(); iter != key.end(); ++iter) {
    char c = *iter;
    if (std::next(iter) == key.end()) {
      std::shared_ptr<T> vp(std::make_shared<T>(std::move(value)));
      if (now->children_.count(c) == 0) {
        // 如果要添加的节点不是叶子节点
        // 需要新建带值的节点
        auto node = std::make_shared<TrieNodeWithValue<T>>(vp);
        now->children_[c] = node;
      } else {
        auto node = std::make_shared<TrieNodeWithValue<T>>(now->children_[c]->children_, vp);
        now->children_[c] = node;
      }
      return tree;
    }
    if (now->children_.count(c) == 0) {
      // 没有key是c的节点，
      // 新建节点，插入now
      // 然后now = 新节点
      auto node = std::make_shared<TrieNode>();
      now->children_[c] = node;
      now = node;

    } else {
      // 存在key是c的节点
      // 复制这个节点
      // 添加到上一次复制的节点也就是now的chidren里
      std::shared_ptr<TrieNode> node_p = now->children_.at(c)->Clone();
      now->children_[c] = node_p;
      now = node_p;
    }
  }

  return tree;
}

auto Trie::Remove(std::string_view key) const -> Trie {
  if (key.empty()) {
    return Trie{};
  }
  if (root_ == nullptr) {
    return Trie{};
  }
  auto node = root_;
  for (auto c : key) {
    if (node->children_.count(c) == 0) {
      return *this;
    }
    node = node->children_.at(c);
  }
  Trie tree;
  std::function<std::shared_ptr<TrieNode>(const std::shared_ptr<const TrieNode> &, size_t)> travel =
      [&](const std::shared_ptr<const TrieNode> &p, size_t index) -> std::shared_ptr<TrieNode> {
    char c = key[index];
    if (index == key.size() - 1) {
      // 查找儿子中是否有目的节点
      // 删除目的节点或替换值节点为普通的节点
      std::shared_ptr<TrieNode> new_node = p->Clone();
      auto target = new_node->children_.at(c);
      if (target->is_value_node_ && !target->children_.empty()) {
        std::shared_ptr<TrieNode> temp(std::make_shared<TrieNode>(new_node->children_[c]->children_));
        new_node->children_[c] = temp;
      } else {
        new_node->children_.erase(c);
      }
      // 返回上一层
      return new_node;
    }
    std::shared_ptr<TrieNode> new_node = p->Clone();
    // 先从上向下复制
    auto ret = travel(p->children_.at(c), index + 1);
    // 查找完成后在根据孩子节点
    // 判断是否孩子节点是否需要被删除
    if (ret->children_.empty() && !ret->is_value_node_) {
      new_node->children_.erase(c);
    } else {
      new_node->children_[c] = ret;
    }
    return new_node;
  };
  tree.root_ = travel(root_, 0);
  // 因为是通过父节点判断孩子
  // 因此最后还会有root没经过判断
  // 判断root是否需要删除
  if (tree.root_->children_.empty() && !root_->is_value_node_) {
    tree.root_ = nullptr;
  }
  return tree;

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
