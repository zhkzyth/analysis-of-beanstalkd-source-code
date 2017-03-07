/*
 * ms相关数据结构的操作函数
 */


#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include "dat.h"

// 初始化这个ms结构
void
ms_init(ms a, ms_event_fn oninsert, ms_event_fn onremove)
{
  a->used = a->cap = a->last = 0;
  a->items = NULL;
  a->oninsert = oninsert;
  a->onremove = onremove;
}

// 增长分配空间
static void
grow(ms a)
{
  void **nitems; // 数组数据，可能是job结构的数据，也可能是其他？
  size_t ncap = (a->cap << 1) ? : 1; // 两倍的增长速度

  nitems = malloc(ncap * sizeof(void *));
  if (!nitems) return;

  memcpy(nitems, a->items, a->used * sizeof(void *));  // 这里的拷贝算法不太优雅，这样一下内存会扩展到 1 * base  + 2 * base的大小
  free(a->items);
  a->items = nitems;
  a->cap = ncap;
}

int
ms_append(ms a, void *item)
{
  // cap记录当前分配的空间最大值
  if (a->used >= a->cap) grow(a);

  // opps，分配失败了，直接返回，可能是空间不够了
  if (a->used >= a->cap) return 0;

  // items记录当前所有的item记录
  a->items[a->used++] = item;

  // trigger观察oninsert事件的观察者
  if (a->oninsert) a->oninsert(a, item, a->used - 1);

  return 1;
}

static int
ms_delete(ms a, size_t i)
{
  void *item;

  if (i >= a->used) return 0;
  item = a->items[i];
  a->items[i] = a->items[--a->used];

  /* it has already been removed now */
  if (a->onremove) a->onremove(a, item, i);
  return 1;
}

void
ms_clear(ms a)
{
  while (ms_delete(a, 0));
  free(a->items);
  ms_init(a, a->oninsert, a->onremove);
}

int
ms_remove(ms a, void *item)
{
  size_t i;

  for (i = 0; i < a->used; i++) {
    if (a->items[i] == item) return ms_delete(a, i);
  }
  return 0;
}

int
ms_contains(ms a, void *item)
{
  size_t i;

  for (i = 0; i < a->used; i++) {
    if (a->items[i] == item) return 1;
  }
  return 0;
}

// 从ms结构里面，拿出item
void *
ms_take(ms a)
{
  void *item;

  if (!a->used) return NULL;

  a->last = a->last % a->used;

  item = a->items[a->last];

  ms_delete(a, a->last);

  ++a->last;

  return item;
}
