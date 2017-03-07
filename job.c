/*
 * job结构相关的操作
 * 至少熟悉以下算法：
 *   - 双向链表 http://www.cnblogs.com/skywang12345/p/3561803.html
*/

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include "dat.h"

// 全局统一的next_id，用来id计数的
static uint64 next_id = 1;

//
static int cur_prime = 0;

//
static job all_jobs_init[12289] = {0};
static job *all_jobs = all_jobs_init;
static size_t all_jobs_cap = 12289; /* == primes[0] */
static size_t all_jobs_used = 0;

//
static int hash_table_was_oom = 0;

//
static void rehash();

//
static int
_get_job_hash_index(uint64 job_id)
{
    return job_id % all_jobs_cap;
}

// 保存job
static void
store_job(job j)
{
    int index = 0;

    index = _get_job_hash_index(j->r.id);

    j->ht_next = all_jobs[index];
    all_jobs[index] = j;
    all_jobs_used++;

    /* accept a load factor of 4 */
    if (all_jobs_used > (all_jobs_cap << 2)) rehash(1);

}

// 分配空间
static void
rehash(int is_upscaling)
{
    job *old = all_jobs;
    size_t old_cap = all_jobs_cap, old_used = all_jobs_used, i;
    int old_prime = cur_prime;
    int d = is_upscaling ? 1 : -1;

    if (cur_prime + d >= NUM_PRIMES) return;
    if (cur_prime + d < 0) return;
    if (is_upscaling && hash_table_was_oom) return;

    cur_prime += d;

    all_jobs_cap = primes[cur_prime];
    all_jobs = calloc(all_jobs_cap, sizeof(job));
    if (!all_jobs) {
        twarnx("Failed to allocate %zu new hash buckets", all_jobs_cap);
        hash_table_was_oom = 1;
        cur_prime = old_prime;
        all_jobs = old;
        all_jobs_cap = old_cap;
        all_jobs_used = old_used;
        return;
    }
    all_jobs_used = 0;
    hash_table_was_oom = 0;

    for (i = 0; i < old_cap; i++) {
        while (old[i]) {
            job j = old[i];
            old[i] = j->ht_next;
            j->ht_next = NULL;
            store_job(j);
        }
    }
    if (old != all_jobs_init) {
        free(old);
    }
}

job
job_find(uint64 job_id)
{
    job jh = NULL;
    int index = _get_job_hash_index(job_id);

    for (jh = all_jobs[index]; jh && jh->r.id != job_id; jh = jh->ht_next);

    return jh;
}

// 分配空间给job
job
allocate_job(int body_size)
{
    job j;

    j = malloc(sizeof(struct job) + body_size);
    if (!j) return twarnx("OOM"), (job) 0;

    memset(j, 0, sizeof(struct job));

    j->r.created_at = nanoseconds();
    j->r.body_size = body_size;
    j->next = j->prev = j; /* not in a linked list */

    return j;
}

// 创建job，带id信息
// 这个工厂函数，应该是给从bin log恢复的数据用的
job
make_job_with_id(uint pri, int64 delay, int64 ttr,
                 int body_size, tube tube, uint64 id)
{
    job j;

    // 分配job需要的内存空间
    j = allocate_job(body_size);
    if (!j) return twarnx("OOM"), (job) 0;

    // 貌似next_id是全局的
    if (id) {
        j->r.id = id;
        if (id >= next_id) next_id = id + 1;
    } else {
        j->r.id = next_id++;
    }

    // 填充record记录的信息
    j->r.pri = pri;
    j->r.delay = delay;
    j->r.ttr = ttr;

    // 保存job
    store_job(j);

    TUBE_ASSIGN(j->tube, tube);

    return j;
}

static void
job_hash_free(job j)
{
    job *slot;

    slot = &all_jobs[_get_job_hash_index(j->r.id)];
    while (*slot && *slot != j) slot = &(*slot)->ht_next;
    if (*slot) {
        *slot = (*slot)->ht_next;
        --all_jobs_used;
    }

    // Downscale when the hashmap is too sparse
    if (all_jobs_used < (all_jobs_cap >> 4)) rehash(0);
}

// 释放掉job
void
job_free(job j)
{
    if (j) {
      TUBE_ASSIGN(j->tube, NULL); // 清掉对tube的引用计数

      if (j->r.state != Copy) job_hash_free(j); // ?

    }

    free(j);
}

// 设置堆的位置
void
job_setheappos(void *j, int pos)
{
    ((job)j)->heap_index = pos;
}

// job权重，找出小的
// 如果权重一样，就根据id来选
int
job_pri_less(void *ax, void *bx)
{
    job a = ax, b = bx;

    if (a->r.pri < b->r.pri) return 1;
    if (a->r.pri > b->r.pri) return 0;

    return a->r.id < b->r.id;
}

// 找出delay最小的
int
job_delay_less(void *ax, void *bx)
{
    job a = ax, b = bx;

    if (a->r.deadline_at < b->r.deadline_at) return 1;
    if (a->r.deadline_at > b->r.deadline_at) return 0;

    return a->r.id < b->r.id;
}

job
job_copy(job j)
{
    job n;

    if (!j) return NULL;

    n = malloc(sizeof(struct job) + j->r.body_size);
    if (!n) return twarnx("OOM"), (job) 0;

    memcpy(n, j, sizeof(struct job) + j->r.body_size);
    n->next = n->prev = n; /* not in a linked list */

    n->file = NULL; /* copies do not have refcnt on the wal */

    n->tube = 0; /* Don't use memcpy for the tube, which we must refcount. */
    TUBE_ASSIGN(n->tube, j->tube);

    /* Mark this job as a copy so it can be appropriately freed later on */
    n->r.state = Copy;

    return n;
}

const char *
job_state(job j)
{
    if (j->r.state == Ready) return "ready";
    if (j->r.state == Reserved) return "reserved";
    if (j->r.state == Buried) return "buried";
    if (j->r.state == Delayed) return "delayed";
    return "invalid";
}

// 判断这个job是否存在一个双向链表中
int
job_list_any_p(job head)
{
    return head->next != head || head->prev != head;
}

// 把job从内部的双向链表结构移除，这个双向链表和tube的那个双向链表不冲突，不要混淆了
job
job_remove(job j)
{
    if (!j) return NULL;
    if (!job_list_any_p(j)) return NULL; /* not in a doubly-linked list */

    j->next->prev = j->prev;
    j->prev->next = j->next;

    j->prev = j->next = j;

    return j;
}

// 把job插入到一个双向链表结构中，每次插到链表的最后一个位置
void
job_insert(job head, job j)
{

    if (job_list_any_p(j)) return; /* already in a linked list */

    // 插入到双向链表里面
    j->prev = head->prev;
    j->next = head;
    head->prev->next = j;
    head->prev = j;
}

uint64
total_jobs()
{
    return next_id - 1;
}

/* for unit tests */
size_t
get_all_jobs_used()
{
    return all_jobs_used;
}
