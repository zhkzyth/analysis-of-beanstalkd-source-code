/*
 * tube数据结构相关的操作
*/

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include "dat.h"

struct ms tubes;

// 创建tube
tube
make_tube(const char *name)
{
    tube t;

    // 生成一个tube结构
    t = new(struct tube);
    if (!t) return NULL;

    // tube的名字
    t->name[MAX_TUBE_NAME_LEN - 1] = '\0';
    strncpy(t->name, name, MAX_TUBE_NAME_LEN - 1);
    if (t->name[MAX_TUBE_NAME_LEN - 1] != '\0') twarnx("truncating tube name");

    // 初始化tube的ready和delay堆
    t->ready.less = job_pri_less;
    t->delay.less = job_delay_less;
    t->ready.rec = job_setheappos;
    t->delay.rec = job_setheappos;

    // 创建一个blank head结构
    t->buried = (struct job) { }; // 创建一个空头
    t->buried.prev = t->buried.next = &t->buried; // 指向自己的空头

    // 初始化这个tube的waiting结构
    ms_init(&t->waiting, NULL, NULL);

    return t;
}

// 释放掉tube
static void
tube_free(tube t)
{
    prot_remove_tube(t);
    free(t->ready.data);
    free(t->delay.data);
    ms_clear(&t->waiting);
    free(t);
}

void
tube_dref(tube t)
{
    if (!t) return;
    if (t->refs < 1) return twarnx("refs is zero for tube: %s", t->name);

    --t->refs;
    if (t->refs < 1) tube_free(t);
}

void
tube_iref(tube t)
{
    if (!t) return;
    ++t->refs;
}

// 创建并插入tube
static tube
make_and_insert_tube(const char *name)
{
    int r;
    tube t = NULL;

    t = make_tube(name); // 创建tube
    if (!t) return NULL;

    /* We want this global tube list to behave like "weak" refs, so don't
     * increment the ref count. */
    r = ms_append(&tubes, t);

    if (!r) return tube_dref(t), (tube) 0;

    return t;
}

// 查找tube
tube
tube_find(const char *name)
{
    tube t;
    size_t i;

    for (i = 0; i < tubes.used; i++) {
        t = tubes.items[i];
        if (strncmp(t->name, name, MAX_TUBE_NAME_LEN) == 0) return t; // 直接查相关的名字
    }

    return NULL;
}

// 查找或者创建
tube
tube_find_or_make(const char *name)
{
    return tube_find(name) ? : make_and_insert_tube(name);
}
