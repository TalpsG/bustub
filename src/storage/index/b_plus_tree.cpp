#include <cstdlib>
#include <fstream>
#include <functional>
#include <iostream>
#include <sstream>
#include <string>
#include <tuple>
#include <utility>

#include "common/config.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
  std::cout << "leafsize: " << leaf_max_size_ << " internalsize: " << internal_max_size_ << "\n";
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  auto page_guard = bpm_->FetchPageBasic(header_page_id_);
  auto page_data = page_guard.AsMut<BPlusTreeHeaderPage>();
  return page_data->root_page_id_ == INVALID_PAGE_ID;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  // Declaration of context instance.
  Context ctx;
  ctx.read_set_.push_back(bpm_->FetchPageRead(header_page_id_));
  auto header_page = ctx.read_set_.back().As<BPlusTreeHeaderPage>();
  ctx.root_page_id_ = header_page->root_page_id_;
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    return false;
  }
  // search之后read_set_里面应该只有一个元素
  SearchLeafRead(ctx, key, ctx.root_page_id_);
  const auto *p = ctx.read_set_.back().As<const LeafPage>();
  for (auto i = 0; i < p->GetSize(); ++i) {
    if (comparator_(key, p->KeyAt(i)) == 0) {
      result->push_back(p->ValueAt(i));
      return true;
    }
  }

  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SearchLeafRead(Context &ctx, const KeyType &key, page_id_t page_id) -> page_id_t {
  ctx.read_set_.pop_front();
  ctx.read_set_.push_back(bpm_->FetchPageRead(page_id));
  const auto *p = ctx.read_set_.back().As<const InternalPage>();
  if (p->IsLeafPage()) {
    return page_id;
  }
  for (auto i = 1; i < p->GetSize(); ++i) {
    if (comparator_(key, p->KeyAt(i)) < 0) {
      page_id_t next_page = p->ValueAt(i - 1);
      return SearchLeafRead(ctx, key, next_page);
    }
  }
  return SearchLeafRead(ctx, key, p->ValueAt(p->GetSize() - 1));
}
void ReleaseAncestor(Context &ctx) {
  auto temp = std::move(ctx.write_set_.back());
  while (!ctx.write_set_.empty()) {
    ctx.write_set_.pop_back();
  }
  ctx.write_set_.push_back(std::move(temp));
  if (ctx.header_page_.has_value()) {
    ctx.header_page_.reset();
  }
}
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SearchLeafInsert(Context &ctx, const KeyType &key, page_id_t page_id) -> page_id_t {
  const auto *p = ctx.write_set_.back().As<InternalPage>();
  if (p->IsLeafPage()) {
    return page_id;
  }
  for (auto i = 1; i < p->GetSize(); ++i) {
    if (comparator_(key, p->KeyAt(i)) < 0) {
      // 如果孩子是安全的，则释放以上所有祖先节点
      page_id_t next_page_id = p->ValueAt(i - 1);
      ctx.write_set_.push_back(bpm_->FetchPageWrite(next_page_id));
      auto *next_page = ctx.write_set_.back().AsMut<BPlusTreePage>();
      bool is_safe = next_page->GetRealMax() > next_page->GetSize();
      if (is_safe) {
        ReleaseAncestor(ctx);
      }

      return SearchLeafInsert(ctx, key, next_page_id);
    }
  }
  page_id_t next_page_id = p->ValueAt(p->GetSize() - 1);

  ctx.write_set_.push_back(bpm_->FetchPageWrite(next_page_id));
  auto *next_page = ctx.write_set_.back().AsMut<InternalPage>();
  bool is_safe = next_page->GetRealMax() != next_page->GetSize();
  if (is_safe) {
    ReleaseAncestor(ctx);
  }
  return SearchLeafInsert(ctx, key, next_page_id);
}
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitLeaf(Context &ctx, LeafPage *old_page, const KeyType &key, const ValueType &value)
    -> std::tuple<KeyType, page_id_t, page_id_t> {
  page_id_t old_page_id = ctx.write_set_.back().PageId();
  page_id_t new_page_id = 0;
  auto new_page_guard = bpm_->NewPageWriteGuarded(&new_page_id);
  auto *new_page = new_page_guard.AsMut<LeafPage>();
  new_page->SetPageType(IndexPageType::LEAF_PAGE);
  new_page->SetMaxSize(leaf_max_size_);
  new_page->SetSize(0);
  int pos;
  for (pos = 0; pos < old_page->GetSize(); ++pos) {
    auto diff = comparator_(key, old_page->KeyAt(pos));
    if (diff < 0) {
      break;
    }
  }

  MappingType temp[leaf_max_size_ + 1];
  // 整理进temp 临时区域
  int i;
  for (i = 0; i < pos; ++i) {
    temp[i].first = old_page->KeyAt(i);
    temp[i].second = old_page->ValueAt(i);
  }
  temp[i].first = key;
  temp[i].second = value;
  for (i = pos + 1; i < old_page->GetRealMax() + 1; ++i) {
    temp[i].first = old_page->KeyAt(i - 1);
    temp[i].second = old_page->ValueAt(i - 1);
  }

  // 整理完毕 挪到old_page和new_page里
  for (int i = 0; i < old_page->GetMinSize(); ++i) {
    old_page->SetKey(i, temp[i].first);
    old_page->SetValue(i, temp[i].second);
  }

  for (int i = old_page->GetMinSize(); i < old_page->GetRealMax() + 1; ++i) {
    new_page->SetKey(i - old_page->GetMinSize(), temp[i].first);
    new_page->SetValue(i - old_page->GetMinSize(), temp[i].second);
  }
  // 设置 size
  old_page->SetSize(old_page->GetMinSize());
  new_page->SetSize(old_page->GetRealMax() + 1 - old_page->GetMinSize());
  // 设置next
  new_page->SetNextPageId(old_page->GetNextPageId());
  old_page->SetNextPageId(new_page_id);

  ctx.write_set_.pop_back();

  return std::make_tuple(new_page->KeyAt(0), old_page_id, new_page_id);
}
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitInternal(Context &ctx, InternalPage *old_page,
                                   std::tuple<KeyType, page_id_t, page_id_t> k_p_p)
    -> std::tuple<KeyType, page_id_t, page_id_t> {
  auto [key, old_id, new_id] = k_p_p;
  page_id_t new_page_id = 0;
  auto new_page_guard = bpm_->NewPageWriteGuarded(&new_page_id);
  auto *new_page = new_page_guard.AsMut<InternalPage>();
  new_page->SetPageType(IndexPageType::INTERNAL_PAGE);
  new_page->SetMaxSize(internal_max_size_);
  new_page->SetSize(1);
  int pos;
  std::tuple<KeyType, page_id_t, page_id_t> res;
  for (pos = 0; pos < old_page->GetSize(); ++pos) {
    if (old_page->ValueAt(pos) == old_id) {
      break;
    }
  }
  std::pair<KeyType, page_id_t> temp[internal_max_size_ + 1];
  // 把节点内的key都搬到temp里
  int i;
  for (i = 0; i <= pos; ++i) {
    temp[i].first = old_page->KeyAt(i);
    temp[i].second = old_page->ValueAt(i);
  }
  temp[i].first = key;
  temp[i].second = new_id;
  for (; i < old_page->GetSize(); ++i) {
    temp[i + 1].first = old_page->KeyAt(i);
    temp[i + 1].second = old_page->ValueAt(i);
  }
  // 复制完毕 开始挪
  for (int i = 0; i < old_page->GetMinSize(); ++i) {
    old_page->SetKeyAt(i, temp[i].first);
    old_page->SetValueAt(i, temp[i].second);
  }
  new_page->SetValueAt(0, temp[old_page->GetMinSize()].second);
  for (int i = old_page->GetMinSize() + 1; i < internal_max_size_ + 1; ++i) {
    new_page->SetKeyAt(i - old_page->GetMinSize(), temp[i].first);
    new_page->SetValueAt(i - old_page->GetMinSize(), temp[i].second);
  }
  std::get<0>(res) = temp[old_page->GetMinSize()].first;
  std::get<1>(res) = ctx.write_set_.back().PageId();
  std::get<2>(res) = new_page_id;

  old_page->SetSize(old_page->GetMinSize());
  new_page->SetSize(internal_max_size_ - old_page->GetMinSize() + 1);

  // 叶子节点不会再修改了，向上查找有没有分裂的内部节点
  ctx.write_set_.pop_back();

  return res;
}
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertParent(Context &ctx, std::tuple<KeyType, page_id_t, page_id_t> k_p_p) {
  auto [key, old_id, new_id] = k_p_p;
  if (ctx.write_set_.empty()) {
    // write_set_ 空了
    // 要新建根节点了
    // 如果是parent是root节点
    page_id_t new_root_id;
    auto new_root_guard = bpm_->NewPageWriteGuarded(&new_root_id);
    auto *new_root_page = new_root_guard.AsMut<InternalPage>();
    new_root_page->SetSize(2);
    new_root_page->SetPageType(IndexPageType::INTERNAL_PAGE);
    new_root_page->SetMaxSize(internal_max_size_);
    new_root_page->SetValueAt(0, old_id);
    new_root_page->SetKeyAt(1, key);
    new_root_page->SetValueAt(1, new_id);
    if (ctx.header_page_.has_value()) {
      ctx.header_page_.value().AsMut<BPlusTreeHeaderPage>()->root_page_id_ = new_root_id;
    } else {
      auto header_guard = bpm_->FetchPageWrite(header_page_id_);
      header_guard.AsMut<BPlusTreeHeaderPage>()->root_page_id_ = new_root_id;
    }
    return;
  }
  auto *parent_page = ctx.write_set_.back().AsMut<InternalPage>();
  if (parent_page->GetSize() < parent_page->GetRealMax()) {
    // 可以直接插入，不需要分裂
    int pos;
    for (pos = 0; pos < parent_page->GetSize(); ++pos) {
      if (parent_page->ValueAt(pos) == old_id) {
        break;
      }
    }
    for (int i = parent_page->GetSize() - 1; i > pos; --i) {
      parent_page->SetKeyAt(i + 1, parent_page->KeyAt(i));
      parent_page->SetValueAt(i + 1, parent_page->ValueAt(i));
    }
    parent_page->SetKeyAt(pos + 1, key);
    parent_page->SetValueAt(pos + 1, new_id);
    parent_page->SetSize(parent_page->GetSize() + 1);
    return;
  }
  // 需要分裂
  auto key_old_new = SplitInternal(ctx, parent_page, k_p_p);
  InsertParent(ctx, key_old_new);
}
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  // Declaration of context instance.
  //
  static int64_t max_key = -1;
  auto now_key = key.GetFromData();
  if (now_key > max_key) {
    max_key = now_key;
  }

  Context ctx;
  (void)ctx;
  ctx.header_page_.emplace(bpm_->FetchPageWrite(header_page_id_));
  // 拿到rootpage
  if (IsEmpty()) {
    auto header_page = ctx.header_page_.value().AsMut<BPlusTreeHeaderPage>();
    auto root_guard = bpm_->NewPageGuarded(&ctx.root_page_id_);
    auto root_page = root_guard.AsMut<LeafPage>();
    header_page->root_page_id_ = ctx.root_page_id_;
    root_page->SetPageType(IndexPageType::LEAF_PAGE);
    root_page->SetKey(0, key);
    root_page->SetValue(0, value);
    root_page->SetSize(1);
    root_page->SetMaxSize(leaf_max_size_);
    root_page->SetNextPageId(INVALID_PAGE_ID);
    return true;
  }
  ctx.root_page_id_ = ctx.header_page_.value().As<BPlusTreeHeaderPage>()->root_page_id_;

  ctx.write_set_.push_back(bpm_->FetchPageWrite(ctx.root_page_id_));
  SearchLeafInsert(ctx, key, ctx.root_page_id_);
  // 当前查找到要插入的节点位置
  auto *page_data = ctx.write_set_.back().AsMut<LeafPage>();
  int pos;
  for (pos = 0; pos < page_data->GetSize(); ++pos) {
    auto diff = comparator_(key, page_data->KeyAt(pos));
    if (diff == 0) {
      // 存在key则直接返回false
      return false;
    }
    if (diff < 0) {
      break;
    }
  }
  // pos 就是该插入的位置
  // 如果可以直接插入
  if (page_data->GetSize() < page_data->GetRealMax()) {
    // 从pos还是向后挪一个位置
    for (int i = page_data->GetSize() - 1; i >= pos; --i) {
      page_data->SetKey(i + 1, page_data->KeyAt(i));
      page_data->SetValue(i + 1, page_data->ValueAt(i));
    }
    page_data->SetKey(pos, key);
    page_data->SetValue(pos, value);
    page_data->SetSize(page_data->GetSize() + 1);
    return true;
  }
  // 需要分裂
  auto k_p_p = SplitLeaf(ctx, page_data, key, value);
  InsertParent(ctx, k_p_p);

  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DeleteEntry(Context &ctx, page_id_t page_id, const KeyType &key) {
  if (page_id == INVALID_PAGE_ID) {
    return;
  }
  auto *page = ctx.write_set_.back().AsMut<BPlusTreePage>();
  // 在leafpage页面删除kv
  if (page->IsLeafPage()) {
    auto *leaf_page = reinterpret_cast<LeafPage *>(page);
    int pos = 0;
    bool delete_flag = false;
    for (; pos < leaf_page->GetSize(); ++pos) {
      auto diff = comparator_(key, leaf_page->KeyAt(pos));
      if (diff == 0) {
        delete_flag = true;
        for (int i = pos + 1; i < leaf_page->GetSize(); ++i) {
          leaf_page->SetKey(i - 1, leaf_page->KeyAt(i));
          leaf_page->SetValue(i - 1, leaf_page->ValueAt(i));
        }
        leaf_page->SetSize(leaf_page->GetSize() - 1);
        auto root_id = ctx.root_page_id_;
        // 如果叶子节点就是跟节点
        if (root_id == page_id) {
          // 且删除后没有元素了，就要清空树
          if (leaf_page->GetSize() == 0 && ctx.header_page_.has_value()) {
            ctx.header_page_.value().AsMut<BPlusTreeHeaderPage>()->root_page_id_ = INVALID_PAGE_ID;
          }
          // 删除完毕 返回
          ctx.write_set_.pop_back();
          return;
        }
        break;
      }
      if (diff < 0) {
        return;
      }
    }
    // 没找到 直接退出
    if (!delete_flag) {
      return;
    }

  } else {
    auto *internal_page = ctx.write_set_.back().AsMut<InternalPage>();
    int pos = 1;
    bool is_delete = false;
    for (; pos < internal_page->GetSize(); ++pos) {
      auto diff = comparator_(key, internal_page->KeyAt(pos));
      // 找到了内部节 点中要删除的key
      if (diff < 0) {
        is_delete = true;
        for (int i = pos; i < internal_page->GetSize(); ++i) {
          internal_page->SetKeyAt(i - 1, internal_page->KeyAt(i));
          internal_page->SetValueAt(i - 1, internal_page->ValueAt(i));
        }
        internal_page->SetSize(internal_page->GetSize() - 1);
        auto root_id = ctx.root_page_id_;
        // 如果节点是跟节点
        if (root_id == page_id) {
          // 且删除后只有一个儿子,则将儿子提到跟节点
          if (internal_page->GetSize() == 1 && ctx.header_page_.has_value()) {
            ctx.header_page_.value().AsMut<BPlusTreeHeaderPage>()->root_page_id_ = internal_page->ValueAt(0);
          }
          // 删除完毕 返回
          //
          ctx.write_set_.pop_back();
          return;
        }
        break;
      }
    }
    if (!is_delete) {
      internal_page->SetSize(internal_page->GetSize() - 1);
      auto root_id = ctx.root_page_id_;
      // 如果节点是跟节点
      if (root_id == page_id) {
        // 且删除后只有一个儿子,则将儿子提到跟节点
        if (internal_page->GetSize() == 1 && ctx.header_page_.has_value()) {
          ctx.header_page_.value().AsMut<BPlusTreeHeaderPage>()->root_page_id_ = internal_page->ValueAt(0);
        }
        // 删除完毕 返回
        ctx.write_set_.pop_back();
        return;
      }
    }
  }
  int prev_size = page->GetSize();
  int min_size = page->GetMinSize();
  // 向上回溯
  ctx.write_set_.pop_back();

  if (ctx.write_set_.empty()) {
    return;
  }
  auto *parent_page = ctx.write_set_.back().AsMut<InternalPage>();

  // 如果剩余key太少不到一半
  if (min_size > prev_size) {
    auto pos = 0;
    for (; pos < parent_page->GetSize(); ++pos) {
      if (page_id == parent_page->ValueAt(pos)) {
        break;
      }
    }
    // 找到相邻的兄弟节点，优先找左兄弟
    WritePageGuard left_sibling_guard;
    WritePageGuard right_sibling_guard;
    page_id_t left_sibling_id = INVALID_PAGE_ID;
    page_id_t right_sibling_id = INVALID_PAGE_ID;
    BPlusTreePage *left_sibling_page = nullptr;
    BPlusTreePage *right_sibling_page = nullptr;
    int internal_key_id;
    KeyType internal_key;
    auto page_guard = bpm_->FetchPageWrite(page_id);
    auto *page_data = page_guard.AsMut<BPlusTreePage>();
    bool merge_left = false;
    bool merge_right = false;
    bool borrow_left = false;
    if (pos != parent_page->GetSize() - 1) {
      right_sibling_id = parent_page->ValueAt(pos + 1);
      right_sibling_guard = bpm_->FetchPageWrite(right_sibling_id);
      right_sibling_page = right_sibling_guard.AsMut<BPlusTreePage>();
      internal_key_id = pos + 1;
      internal_key = parent_page->KeyAt(pos + 1);
      merge_right = (right_sibling_page->GetSize() + page_data->GetSize()) <= page_data->GetRealMax();
    }
    if (pos != 0) {
      left_sibling_id = parent_page->ValueAt(pos - 1);
      left_sibling_guard = bpm_->FetchPageWrite(left_sibling_id);
      left_sibling_page = left_sibling_guard.AsMut<BPlusTreePage>();
      internal_key_id = pos;
      internal_key = parent_page->KeyAt(pos);
      merge_left = (left_sibling_page->GetSize() + page_data->GetSize()) <= page_data->GetRealMax();
      borrow_left = !merge_left;
    }

    BPlusTreePage *sibling_page;
    if (merge_left || merge_right) {
      if (merge_left) {
        sibling_page = left_sibling_page;
      } else {
        sibling_page = right_sibling_page;
      }
      // 让sibling始终指向左兄弟
      if (!merge_left) {
        auto temp = page_data;
        page_data = sibling_page;
        sibling_page = temp;
      }
      // 如果不是叶子
      if (!page_data->IsLeafPage()) {
        // 不是叶子
        auto *sibling = reinterpret_cast<InternalPage *>(sibling_page);
        auto *page = reinterpret_cast<InternalPage *>(page_data);
        // 把右兄弟的kv都搬到左兄弟里
        sibling->SetKeyAt(sibling->GetSize(), internal_key);
        sibling->SetValueAt(sibling->GetSize(), page->ValueAt(0));
        for (int i = 1; i < page->GetSize(); ++i) {
          sibling->SetKeyAt(i + sibling->GetSize(), page->KeyAt(i));
          sibling->SetValueAt(i + sibling->GetSize(), page->ValueAt(i));
        }
        sibling->SetSize(sibling->GetSize() + page->GetSize());
      } else {
        // 是叶子
        auto *sibling = reinterpret_cast<LeafPage *>(sibling_page);
        auto *page = reinterpret_cast<LeafPage *>(page_data);
        for (int i = 0; i < page->GetSize(); ++i) {
          sibling->SetKey(i + sibling->GetSize(), page->KeyAt(i));
          sibling->SetValue(i + sibling->GetSize(), page->ValueAt(i));
        }
        sibling->SetSize(sibling->GetSize() + page->GetSize());
        // 修改父节点internal key对应的pageid
        sibling->SetNextPageId(page->GetNextPageId());
      }
      auto parent_id = ctx.write_set_.back().PageId();
      DeleteEntry(ctx, parent_id, internal_key);
    } else {
      // 没有合并，选择借用
      if (borrow_left) {
        sibling_page = left_sibling_page;
        // 如果是非叶子
        if (!page_data->IsLeafPage()) {
          // page的kv往后挪一个
          auto *sibling = reinterpret_cast<InternalPage *>(sibling_page);
          auto *page = reinterpret_cast<InternalPage *>(page_data);
          for (int i = page->GetSize() - 1; i >= 0; --i) {
            page->SetKeyAt(i + 1, page->KeyAt(i));
            page->SetValueAt(i + 1, page->ValueAt(i));
          }
          // 插入借过来的kv
          auto replace_key = sibling->KeyAt(sibling->GetSize() - 1);
          page->SetKeyAt(1, internal_key);
          page->SetValueAt(0, sibling->ValueAt(sibling->GetSize() - 1));
          page->SetSize(page->GetSize() + 1);
          sibling->SetSize(sibling->GetSize() - 1);
          parent_page->SetKeyAt(internal_key_id, replace_key);
        } else {
          // 如果是叶子
          auto *sibling = reinterpret_cast<LeafPage *>(sibling_page);
          auto *page = reinterpret_cast<LeafPage *>(page_data);
          for (int i = page->GetSize() - 1; i >= 0; --i) {
            page->SetKey(i + 1, page->KeyAt(i));
            page->SetValue(i + 1, page->ValueAt(i));
          }
          page->SetKey(0, sibling->KeyAt(sibling->GetSize() - 1));
          page->SetValue(0, sibling->ValueAt(sibling->GetSize() - 1));
          page->SetSize(page->GetSize() + 1);
          sibling->SetSize(sibling->GetSize() - 1);
          parent_page->SetKeyAt(internal_key_id, page->KeyAt(0));
        }
      } else {
        // 兄弟是右兄弟
        sibling_page = right_sibling_page;
        if (!page_data->IsLeafPage()) {
          auto *sibling = reinterpret_cast<InternalPage *>(sibling_page);
          auto *page = reinterpret_cast<InternalPage *>(page_data);
          page->SetKeyAt(page->GetSize(), internal_key);
          page->SetValueAt(page->GetSize(), sibling->ValueAt(0));

          auto replace_key = sibling->KeyAt(1);
          sibling->SetValueAt(0, sibling->ValueAt(1));
          for (int i = 1; i < sibling->GetSize() - 1; ++i) {
            sibling->SetKeyAt(i, sibling->KeyAt(i + 1));
            sibling->SetValueAt(i, sibling->ValueAt(i + 1));
          }
          page->SetSize(page->GetSize() + 1);
          sibling->SetSize(sibling->GetSize() - 1);

          parent_page->SetKeyAt(internal_key_id, replace_key);
        } else {
          // TODO(talps) :照着上面写
          auto *sibling = reinterpret_cast<LeafPage *>(sibling_page);
          auto *page = reinterpret_cast<LeafPage *>(page_data);
          page->SetKey(page->GetSize(), sibling->KeyAt(0));
          page->SetValue(page->GetSize(), sibling->ValueAt(0));
          for (int i = 1; i < sibling->GetSize(); ++i) {
            sibling->SetKey(i - 1, sibling->KeyAt(i));
            sibling->SetValue(i - 1, sibling->ValueAt(i));
          }
          page->SetSize(page->GetSize() + 1);
          sibling_page->SetSize(sibling->GetSize() - 1);
          parent_page->SetKeyAt(internal_key_id, sibling->KeyAt(0));
        }
      }
    }
  }
}
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SearchLeafRemove(Context &ctx, const KeyType &key, page_id_t page_id) -> page_id_t {
  auto p = ctx.write_set_.back().As<InternalPage>();
  if (p->IsLeafPage()) {
    return page_id;
  }
  for (auto i = 1; i < p->GetSize(); ++i) {
    if (comparator_(key, p->KeyAt(i)) < 0) {
      page_id_t next_page_id = p->ValueAt(i - 1);
      ctx.write_set_.push_back(bpm_->FetchPageWrite(next_page_id));
      auto *next_page = ctx.write_set_.back().AsMut<BPlusTreePage>();
      bool is_safe = next_page->GetSize() > next_page->GetMinSize();

      if (is_safe) {
        ReleaseAncestor(ctx);
      }
      return SearchLeafRemove(ctx, key, next_page_id);
    }
  }
  page_id_t next_page_id = p->ValueAt(p->GetSize() - 1);
  ctx.write_set_.push_back(bpm_->FetchPageWrite(next_page_id));
  auto *next_page = ctx.write_set_.back().AsMut<InternalPage>();
  // TODO(talps) :只要不能直接删除，就都是不安全的
  bool is_safe = next_page->GetSize() > next_page->GetMinSize();
  if (is_safe) {
    ReleaseAncestor(ctx);
  }

  return SearchLeafRemove(ctx, key, next_page_id);
}
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  // Declaration of context instance.
  Context ctx;
  ctx.header_page_.emplace(bpm_->FetchPageWrite(header_page_id_));
  ctx.root_page_id_ = ctx.header_page_->AsMut<BPlusTreeHeaderPage>()->root_page_id_;
  ctx.write_set_.push_back(bpm_->FetchPageWrite(ctx.root_page_id_));
  auto leaf_id = SearchLeafRemove(ctx, key, ctx.root_page_id_);
  DeleteEntry(ctx, leaf_id, key);
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return End();
  }
  auto root_guard = bpm_->FetchPageRead(bpm_->FetchPageRead(header_page_id_).As<BPlusTreeHeaderPage>()->root_page_id_);
  auto root_page = root_guard.As<BPlusTreePage>();
  const InternalPage *ip;
  page_id_t page_id = root_guard.PageId();
  while (!root_page->IsLeafPage()) {
    ip = reinterpret_cast<const InternalPage *>(root_page);
    root_guard = bpm_->FetchPageRead(ip->ValueAt(0));
    page_id = root_guard.PageId();
    root_page = root_guard.As<BPlusTreePage>();
  }
  root_guard.Drop();
  return INDEXITERATOR_TYPE(bpm_, page_id, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  Context ctx;
  ctx.read_set_.push_back(bpm_->FetchPageRead(header_page_id_));
  SearchLeafRead(ctx, key, ctx.read_set_.back().As<BPlusTreeHeaderPage>()->root_page_id_);
  auto leaf_page = ctx.read_set_.back().template As<LeafPage>();
  int i = 0;
  for (i = 0; i < leaf_page->GetSize(); ++i) {
    if (comparator_(key, leaf_page->KeyAt(i)) == 0) {
      break;
    }
  }
  page_id_t page_id = ctx.read_set_.back().PageId();
  ctx.read_set_.pop_back();
  return INDEXITERATOR_TYPE(bpm_, page_id, i);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(bpm_, INVALID_PAGE_ID, -1); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  return bpm_->FetchPageRead(header_page_id_).As<BPlusTreeHeaderPage>()->root_page_id_;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << '\n';
    std::cout << '\n';

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << '\n';

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << '\n';
    std::cout << '\n';
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << '\n';
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << '\n';
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;
  proot.page_id_ = root_id;
  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
