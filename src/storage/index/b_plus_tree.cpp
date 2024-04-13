#include <deque>
#include <fstream>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
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
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  return bpm_->FetchPageRead(header_page_id_).As<BPlusTreeHeaderPage>()->root_page_id_ == INVALID_PAGE_ID;
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
  auto header_guard = bpm_->FetchPageRead(header_page_id_);
  page_id_t page_id = header_guard.As<BPlusTreeHeaderPage>()->root_page_id_;
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  ctx.read_set_.push_back(bpm_->FetchPageRead(page_id));
  const auto *page = ctx.read_set_.back().As<BPlusTreePage>();
  while (!page->IsLeafPage()) {
    // 不是叶子 继续向下找
    const auto *internal_page = reinterpret_cast<const InternalPage *>(page);
    bool found = false;
    for (int i = 1; i < internal_page->GetSize(); ++i) {
      if (comparator_(key, internal_page->KeyAt(i)) < 0) {
        // 找到了
        found = true;
        page_id = internal_page->ValueAt(i - 1);
        ctx.read_set_.push_back(bpm_->FetchPageRead(page_id));
        page = ctx.read_set_.back().As<BPlusTreePage>();
        ctx.read_set_.pop_front();
        break;
      }
    }
    if (!found) {
      // 如果前面没找到，就是在最后一个value里
      found = true;
      page_id = internal_page->ValueAt(internal_page->GetSize() - 1);
      ctx.read_set_.push_back(bpm_->FetchPageRead(page_id));
      page = ctx.read_set_.back().As<BPlusTreePage>();
      ctx.read_set_.pop_front();
    }
  }
  // 找到了叶子节点，开始找key
  const auto *p = reinterpret_cast<const LeafPage *>(page);
  for (int i = 0; i < p->GetSize(); i++) {
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

void ReleaseLock(Context &ctx) {
  auto temp = std::move(ctx.write_set_.back());
  while (!ctx.write_set_.empty()) {
    ctx.write_set_.pop_back();
  }
  ctx.write_set_.push_back(std::move(temp));
}
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertParent(Context &ctx, page_id_t old_value, page_id_t new_value, const KeyType &key) {
  if (old_value == ctx.root_page_id_) {
    // 也就是已经回溯到跟节点了
    page_id_t new_id;
    auto new_guard = bpm_->NewWriteGuarded(&new_id);
    auto *new_page = new_guard.AsMut<InternalPage>();
    new_page->SetPageType(IndexPageType::INTERNAL_PAGE);
    new_page->SetMaxSize(internal_max_size_);
    new_page->SetSize(2);
    new_page->SetValueAt(0, old_value);
    new_page->SetKeyAt(1, key);
    new_page->SetValueAt(1, new_value);
    if (ctx.header_page_.has_value()) {
      ctx.header_page_.value().AsMut<BPlusTreeHeaderPage>()->root_page_id_ = new_id;
    }
    return;
  }
  page_id_t parent_id = ctx.write_set_.back().PageId();
  (void)parent_id;
  auto *parent_page = ctx.write_set_.back().AsMut<InternalPage>();
  // 找到key该放的位置
  int pos;
  for (pos = 0; pos < parent_page->GetSize(); pos++) {
    if (parent_page->ValueAt(pos) == old_value) {
      // 新的kv一定在老的右边，因此找到老的就确定了新的位置
      pos += 1;
      break;
    }
  }
  if (parent_page->GetSize() < parent_page->GetRealMax()) {
    // 可以直接插入
    for (int i = parent_page->GetSize(); i >= pos; i--) {
      parent_page->SetKeyAt(i + 1, parent_page->KeyAt(i));
      parent_page->SetValueAt(i + 1, parent_page->ValueAt(i));
    }
    parent_page->SetKeyAt(pos, key);
    parent_page->SetValueAt(pos, new_value);
    parent_page->IncreaseSize(1);
    return;
  }
  //  需要分裂
  std::deque<std::pair<KeyType, page_id_t>> temp;
  for (int i = 0; i < pos; i++) {
    temp.push_back({parent_page->KeyAt(i), parent_page->ValueAt(i)});
  }
  temp.push_back({key, new_value});
  for (int i = pos; i < parent_page->GetSize(); ++i) {
    temp.push_back({parent_page->KeyAt(i), parent_page->ValueAt(i)});
  }
  // 给parent_page min_size 个
  for (int i = 0; i < parent_page->GetMinSize(); ++i) {
    parent_page->SetKeyAt(i, temp.front().first);
    parent_page->SetValueAt(i, temp.front().second);
    temp.pop_front();
  }
  parent_page->SetSize(parent_page->GetMinSize());
  // 新建页面装入剩下的kv
  page_id_t new_id;
  auto new_guard = bpm_->NewWriteGuarded(&new_id);
  auto *new_page = new_guard.AsMut<InternalPage>();
  new_page->SetPageType(IndexPageType::INTERNAL_PAGE);
  new_page->SetMaxSize(internal_max_size_);
  int i = 0;
  while (!temp.empty()) {
    new_page->SetKeyAt(i, temp.front().first);
    new_page->SetValueAt(i, temp.front().second);
    i++;
    temp.pop_front();
  }
  new_page->SetSize(i);
  ctx.write_set_.pop_back();
  InsertParent(ctx, parent_id, new_id, new_page->KeyAt(0));
}
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  // Declaration of context instance.
  Context ctx;
  ctx.header_page_.emplace(bpm_->FetchPageWrite(header_page_id_));
  ctx.root_page_id_ = ctx.header_page_->As<BPlusTreeHeaderPage>()->root_page_id_;
  if (ctx.root_page_id_ == INVALID_PAGE_ID) {
    // 树空
    page_id_t root_id;
    auto root_guard = bpm_->NewWriteGuarded(&root_id);
    ctx.header_page_->AsMut<BPlusTreeHeaderPage>()->root_page_id_ = root_id;
    auto *root_page = root_guard.AsMut<LeafPage>();
    root_page->SetPageType(IndexPageType::LEAF_PAGE);
    root_page->SetMaxSize(leaf_max_size_);
    root_page->SetSize(1);
    root_page->SetKeyAt(0, key);
    root_page->SetValueAt(0, value);
    root_page->SetNextPageId(INVALID_PAGE_ID);
    return true;
  }
  // 搜索
  page_id_t page_id = ctx.root_page_id_;
  ctx.write_set_.push_back(bpm_->FetchPageWrite(page_id));
  auto *page = ctx.write_set_.back().AsMut<BPlusTreePage>();
  while (!page->IsLeafPage()) {
    auto *internal_page = reinterpret_cast<InternalPage *>(page);
    bool found = false;
    for (int i = 1; i < internal_page->GetSize(); ++i) {
      if (comparator_(key, internal_page->KeyAt(i)) < 0) {
        found = true;
        page_id = internal_page->ValueAt(i - 1);
        ctx.write_set_.push_back(bpm_->FetchPageWrite(page_id));
        page = ctx.write_set_.back().AsMut<BPlusTreePage>();
        if (page->GetRealMax() > page->GetSize()) {
          // 释放锁
          ReleaseLock(ctx);
        }
        break;
      }
    }
    if (!found) {
      found = true;
      page_id = internal_page->ValueAt(internal_page->GetSize() - 1);
      ctx.write_set_.push_back(bpm_->FetchPageWrite(page_id));
      page = ctx.write_set_.back().AsMut<BPlusTreePage>();
      if (page->GetRealMax() > page->GetSize()) {
        // 释放锁
        ReleaseLock(ctx);
      }
    }
  }
  // 找到叶子节点
  auto *leaf_page = reinterpret_cast<LeafPage *>(page);
  // pos就是新kv的位置
  int pos;
  for (pos = 0; pos < leaf_page->GetSize(); pos++) {
    auto diff = comparator_(key, leaf_page->KeyAt(pos));
    if (diff == 0) {
      return false;
    }
    if (diff < 0) {
      break;
    }
  }
  if (leaf_page->GetSize() < leaf_page->GetRealMax()) {
    // 如果可以直接插入，先将部分元素后移一个
    for (int i = leaf_page->GetSize() - 1; i >= pos; i--) {
      leaf_page->SetKeyAt(i + 1, leaf_page->KeyAt(i));
      leaf_page->SetValueAt(i + 1, leaf_page->ValueAt(i));
    }
    leaf_page->SetKeyAt(pos, key);
    leaf_page->SetValueAt(pos, value);
    leaf_page->IncreaseSize(1);
    ctx.write_set_.pop_back();
    return true;
  }
  // 需要分裂
  page_id_t new_id;
  auto new_guard = bpm_->NewWriteGuarded(&new_id);
  auto *new_page = new_guard.AsMut<LeafPage>();
  new_page->SetPageType(IndexPageType::LEAF_PAGE);
  new_page->SetSize(0);
  new_page->SetMaxSize(leaf_max_size_);
  std::deque<std::pair<KeyType, ValueType>> temp;
  // 将leafpage和新加的kv全搬运至temp里
  for (int i = 0; i < pos; i++) {
    temp.push_back({leaf_page->KeyAt(i), leaf_page->ValueAt(i)});
  }
  temp.push_back({key, value});
  for (int i = pos; i < leaf_page->GetSize(); ++i) {
    temp.push_back({leaf_page->KeyAt(i), leaf_page->ValueAt(i)});
  }
  // 搬回leafpage一部分
  for (int i = 0; i < leaf_page->GetMinSize(); i++) {
    leaf_page->SetKeyAt(i, temp.front().first);
    leaf_page->SetValueAt(i, temp.front().second);
    temp.pop_front();
  }
  // 剩下的都搬进newpage里
  int i = 0;
  while (!temp.empty()) {
    new_page->SetKeyAt(i, temp.front().first);
    new_page->SetValueAt(i, temp.front().second);
    i++;
    temp.pop_front();
  }

  // 设置size 和nextpageid
  leaf_page->SetSize(leaf_page->GetMinSize());
  new_page->SetSize(i);
  new_page->SetNextPageId(leaf_page->GetNextPageId());
  leaf_page->SetNextPageId(new_id);

  // 弹出当前节点的guard
  ctx.write_set_.pop_back();
  InsertParent(ctx, page_id, new_id, new_page->KeyAt(0));

  return true;
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
void BPLUSTREE_TYPE::DeleteEntry(Context &ctx, const KeyType &key) {
  // 从节点中删除这个key
  auto *now_page = ctx.write_set_.back().AsMut<BPlusTreePage>();
  auto now_id = ctx.write_set_.back().PageId();
  if (now_page->IsLeafPage()) {
    // 如果节点是叶子
    auto *leaf_page = reinterpret_cast<LeafPage *>(now_page);
    int pos = 0;
    bool found = false;
    for (pos = 0; pos < leaf_page->GetSize(); pos++) {
      if (comparator_(key, leaf_page->KeyAt(pos)) == 0) {
        found = true;
        break;
      }
    }
    if (!found) {
      return;
    }
    // 开始删除
    for (int i = pos + 1; i < leaf_page->GetSize(); i++) {
      leaf_page->SetKeyAt(i - 1, leaf_page->KeyAt(i));
      leaf_page->SetValueAt(i - 1, leaf_page->ValueAt(i));
    }
    leaf_page->IncreaseSize(-1);
    if (leaf_page->GetSize() == 0 && now_id == ctx.root_page_id_) {
      // 如果删除的叶子没有元素且是根节点，则直接清空树
      ctx.header_page_->AsMut<BPlusTreeHeaderPage>()->root_page_id_ = INVALID_PAGE_ID;
      ctx.write_set_.pop_back();
      return;
    }

  } else {
    // 如果非叶子
    auto *internal_page = reinterpret_cast<InternalPage *>(now_page);
    int pos;
    for (pos = 1; pos < internal_page->GetSize(); pos++) {
      if (comparator_(key, internal_page->KeyAt(pos)) < 0) {
        break;
      }
    }
    // 最后得到的pos是internal_page中比key大的最小的key的位置
    for (int i = pos; i < internal_page->GetSize(); i++) {
      internal_page->SetKeyAt(i - 1, internal_page->KeyAt(i));
      internal_page->SetValueAt(i - 1, internal_page->ValueAt(i));
    }
    internal_page->IncreaseSize(-1);
    // 删除完毕
    // now如果是internal节点且只有一个孩子，则直接让孩子是根
    if (internal_page->GetSize() == 1 && ctx.root_page_id_ == now_id) {
      // TODO(talps) :移动孩子直接变成根
      ctx.header_page_->AsMut<BPlusTreeHeaderPage>()->root_page_id_ = internal_page->ValueAt(0);
      ctx.write_set_.back().Drop();
      ctx.write_set_.pop_back();
      return;
    }
  }
  int now_size = now_page->GetSize();
  int min_size = now_page->GetMinSize();
  if (ctx.write_set_.size() == 1) {
    // 如果size为1,则只有now在write_set_里
    // 没有父节点，即now为根节点
    ctx.write_set_.pop_back();
    return;
  }
  if (now_size < min_size) {
    // kv过少，需要合并或借
    auto *parent_page = ctx.write_set_[ctx.write_set_.size() - 2].AsMut<InternalPage>();
    page_id_t sibling_id = 0;
    WritePageGuard sibling_guard;
    BPlusTreePage *sibling_page = nullptr;
    bool is_right = false;
    KeyType internal_key;
    int internal_id;
    int now_pos;
    for (now_pos = 0; now_pos < parent_page->GetSize(); now_pos++) {
      if (parent_page->ValueAt(now_pos) == now_id) {
        break;
      }
    }
    // sibling 优先选左兄弟
    if (now_pos == 0) {
      is_right = true;
      sibling_id = parent_page->ValueAt(1);
      internal_key = parent_page->KeyAt(1);
      internal_id = 1;
    } else {
      sibling_id = parent_page->ValueAt(now_pos - 1);
      internal_key = parent_page->KeyAt(now_pos);
      internal_id = now_pos;
    }
    sibling_guard = bpm_->FetchPageWrite(sibling_id);
    sibling_page = sibling_guard.AsMut<BPlusTreePage>();
    if (now_size + sibling_page->GetSize() <= sibling_page->GetRealMax()) {
      // NOTE:合并
      if (is_right) {
        // 交换双方指针，让sibling指针是左边的
        auto temp = now_page;
        now_page = sibling_page;
        sibling_page = temp;
      }
      if (!now_page->IsLeafPage()) {
        auto *sibling = reinterpret_cast<InternalPage *>(sibling_page);
        auto *now = reinterpret_cast<InternalPage *>(now_page);
        sibling->SetKeyAt(sibling->GetSize(), internal_key);
        sibling->SetValueAt(sibling->GetSize(), now->ValueAt(0));
        for (int i = 1; i < now->GetSize(); i++) {
          sibling->SetKeyAt(sibling->GetSize() + i, now->KeyAt(i));
          sibling->SetValueAt(sibling->GetSize() + i, now->ValueAt(i));
        }
        sibling->IncreaseSize(now->GetSize());
      } else {
        auto *sibling = reinterpret_cast<LeafPage *>(sibling_page);
        auto *now = reinterpret_cast<LeafPage *>(now_page);
        for (int i = 0; i < now->GetSize(); i++) {
          sibling->SetKeyAt(i + sibling->GetSize(), now->KeyAt(i));
          sibling->SetValueAt(i + sibling->GetSize(), now->ValueAt(i));
        }
        sibling->IncreaseSize(now->GetSize());
        // 叶子节点要额外设置一下nextpageid
        sibling->SetNextPageId(now->GetNextPageId());
      }
      ctx.write_set_.pop_back();
      DeleteEntry(ctx, internal_key);
    } else {
      // NOTE:借
      if (!is_right) {
        if (!now_page->IsLeafPage()) {
          auto *sibling = reinterpret_cast<InternalPage *>(sibling_page);
          auto *now = reinterpret_cast<InternalPage *>(now_page);
          // 对于非叶子节点借kv
          // 简单来说就是
          // 把左兄弟的最后一个kv借给now
          // 然后再把now里的借过来的key和parentpage中的internalkey交换一下
          // 先后移一个位置腾空间
          for (int i = now->GetSize() - 1; i >= 0; i--) {
            now->SetKeyAt(i + 1, now->KeyAt(i));
            now->SetValueAt(i + 1, now->ValueAt(i));
          }
          now->SetKeyAt(1, internal_key);
          now->SetValueAt(0, sibling->ValueAt(sibling->GetSize() - 1));
          parent_page->SetKeyAt(internal_id, sibling->KeyAt(sibling->GetSize() - 1));
          sibling->IncreaseSize(-1);
          now->IncreaseSize(1);
        } else {
          auto *sibling = reinterpret_cast<LeafPage *>(sibling_page);
          auto *now = reinterpret_cast<LeafPage *>(now_page);
          for (int i = now->GetSize() - 1; i >= 0; i--) {
            now->SetKeyAt(i + 1, now->KeyAt(i));
            now->SetValueAt(i + 1, now->ValueAt(i));
          }
          now->SetKeyAt(0, sibling->KeyAt(sibling->GetSize() - 1));
          now->SetValueAt(0, sibling->ValueAt(sibling->GetSize() - 1));
          now->IncreaseSize(1);
          sibling->IncreaseSize(-1);
          parent_page->SetKeyAt(internal_id, now->KeyAt(0));
        }
      } else {
        // 右兄弟
        if (!now_page->IsLeafPage()) {
          auto *sibling = reinterpret_cast<InternalPage *>(sibling_page);
          auto *now = reinterpret_cast<InternalPage *>(now_page);
          // 先给now后面添加一个kv,internalkey和sibling的第一个v
          now->SetKeyAt(now->GetSize(), internal_key);
          now->SetValueAt(now->GetSize(), sibling->ValueAt(0));
          for (int i = 1; i < sibling->GetSize(); i++) {
            sibling->SetKeyAt(i - 1, sibling->KeyAt(i));
            sibling->SetValueAt(i - 1, sibling->ValueAt(i));
          }
          parent_page->SetKeyAt(internal_id, sibling->KeyAt(0));
          sibling->IncreaseSize(-1);
          now->IncreaseSize(1);
        } else {
          auto *sibling = reinterpret_cast<LeafPage *>(sibling_page);
          auto *now = reinterpret_cast<LeafPage *>(now_page);
          now->SetKeyAt(now->GetSize(), sibling->KeyAt(0));
          now->SetValueAt(now->GetSize(), sibling->ValueAt(0));
          for (int i = 1; i < sibling->GetSize(); i++) {
            sibling->SetKeyAt(i - 1, sibling->KeyAt(i));
            sibling->SetValueAt(i - 1, sibling->ValueAt(i));
          }
          parent_page->SetKeyAt(internal_id, sibling->KeyAt(0));
          sibling->IncreaseSize(-1);
          now->IncreaseSize(1);
        }
      }
    }
  }
  while (!ctx.write_set_.empty()) {
    ctx.write_set_.pop_back();
  }
}
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  // Declaration of context instance.
  Context ctx;
  ctx.header_page_.emplace(bpm_->FetchPageWrite(header_page_id_));
  ctx.root_page_id_ = ctx.header_page_->As<BPlusTreeHeaderPage>()->root_page_id_;
  page_id_t page_id = ctx.header_page_->AsMut<BPlusTreeHeaderPage>()->root_page_id_;
  if (page_id == INVALID_PAGE_ID) {
    return;
  }
  ctx.write_set_.push_back(bpm_->FetchPageWrite(page_id));
  auto *page = ctx.write_set_.back().AsMut<BPlusTreePage>();
  while (!page->IsLeafPage()) {
    auto *internal_page = reinterpret_cast<InternalPage *>(page);
    bool found = false;
    for (int i = 1; i < internal_page->GetSize(); ++i) {
      if (comparator_(key, internal_page->KeyAt(i)) < 0) {
        found = true;
        page_id = internal_page->ValueAt(i - 1);
        ctx.write_set_.push_back(bpm_->FetchPageWrite(page_id));
        page = ctx.write_set_.back().AsMut<BPlusTreePage>();
        if (page->GetSize() > page->GetMinSize()) {
          // 释放锁
          ReleaseLock(ctx);
        }
        break;
      }
    }
    if (!found) {
      found = true;
      page_id = internal_page->ValueAt(internal_page->GetSize() - 1);
      ctx.write_set_.push_back(bpm_->FetchPageWrite(page_id));
      page = ctx.write_set_.back().AsMut<BPlusTreePage>();
      if (page->GetSize() > page->GetMinSize()) {
        // 释放锁
        ReleaseLock(ctx);
      }
    }
  }
  // 叶子节点就是ctx.write_set_的最后一个
  DeleteEntry(ctx, key);
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
  auto header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_guard.As<BPlusTreeHeaderPage>();
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(bpm_, INVALID_PAGE_ID, -1);
  }
  auto page_id = header_page->root_page_id_;
  auto page_guard = bpm_->FetchPageRead(page_id);
  const auto *page = page_guard.As<BPlusTreePage>();
  while (!page->IsLeafPage()) {
    const auto *internal_page = reinterpret_cast<const InternalPage *>(page);
    page_id = internal_page->ValueAt(0);
    page_guard = bpm_->FetchPageRead(page_id);
    page = page_guard.As<BPlusTreePage>();
  }
  page_guard.Drop();
  return INDEXITERATOR_TYPE(bpm_, page_id, 0);
}
/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  auto header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_guard.As<BPlusTreeHeaderPage>();
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(bpm_, INVALID_PAGE_ID, -1);
  }
  auto page_id = header_page->root_page_id_;
  auto page_guard = bpm_->FetchPageRead(page_id);
  const auto *page = page_guard.As<BPlusTreePage>();
  while (!page->IsLeafPage()) {
    const auto *internal_page = reinterpret_cast<const InternalPage *>(page);
    bool found = false;
    for (int i = 1; i < internal_page->GetSize(); ++i) {
      if (comparator_(key, internal_page->KeyAt(i)) < 0) {
        found = true;
        page_guard = bpm_->FetchPageRead(internal_page->ValueAt(i - 1));
        page_id = page_guard.PageId();
        page = page_guard.As<BPlusTreePage>();
        break;
      }
    }
    if (!found) {
      found = true;
      page_guard = bpm_->FetchPageRead(internal_page->ValueAt(internal_page->GetSize() - 1));
      page_id = page_guard.PageId();
      page = page_guard.As<BPlusTreePage>();
    }
  }
  const auto *leaf_page = reinterpret_cast<const LeafPage *>(page);
  int pos;
  for (pos = 0; pos < leaf_page->GetSize(); pos++) {
    auto diff = comparator_(key, leaf_page->KeyAt(pos));
    if (diff == 0) {
      break;
    }
    if (diff < 0) {
      return INDEXITERATOR_TYPE(bpm_, INVALID_PAGE_ID, -1);
    }
  }
  page_guard.Drop();
  return INDEXITERATOR_TYPE(bpm_, page_id, pos);
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
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, std::ofstream &out) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

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
