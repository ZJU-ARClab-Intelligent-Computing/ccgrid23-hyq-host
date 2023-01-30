#include <linux/init.h>
#include <linux/module.h>
#include <linux/sizes.h>
#include <linux/types.h>
#include <linux/sbitmap.h>

#include "rdma.h"

static unsigned int offload_op = REQ_OP_READ;
module_param(offload_op, uint, 0644);
MODULE_PARM_DESC(offload_op, "Operation to offload: read=0, write=1.");

static unsigned int offload_qds[2] = {8, 8};
module_param_array(offload_qds, uint, NULL, 0644);
MODULE_PARM_DESC(offload_qds, "Queue depth of operations.");

struct qd_rw_based_queue_data {
    unsigned int depth;
    unsigned int max_depth;
    struct sbitmap tags;
};

static void qd_rw_based_prepare_req(struct nvme_rdma_request *req)
{
    req->scheduler_data.val = (u64)-1;
}

/**
 * @brief Offload requests with offload_op, limit the queue depth to
 * offload_qds[req_op(rq)]
 * 
 * @param req 
 * @return true 
 * @return false 
 */
static inline bool qd_rw_based_should_offload(struct nvme_rdma_request *req)
{
    struct qd_rw_based_queue_data *queue_data = req->queue->scheduler_data.ptr;
	struct request *rq = blk_mq_rq_from_pdu(req);
    enum req_opf op = req_op(rq);
    int tag;

    if (op != offload_op || crossing_128k_boundary(rq))
        return false;

    if (unlikely(queue_data->depth != offload_qds[op])) {
        queue_data->depth = min_t(unsigned int, queue_data->max_depth, offload_qds[op]);
        sbitmap_resize(&queue_data->tags, queue_data->depth);
    }

    tag = sbitmap_get(&queue_data->tags, 0, false);
    if (tag < 0)
        return false;

    req->scheduler_data.val = tag;
    return true;
}

static void qd_rw_based_finish_req(struct nvme_rdma_request *req)
{
    int tag = (int)req->scheduler_data.val;

    if (tag >= 0) {
        struct qd_rw_based_queue_data *queue_data = req->queue->scheduler_data.ptr;
        sbitmap_clear_bit(&queue_data->tags, (unsigned int)tag);
    }
}

static int qd_rw_based_init_queue_data(struct nvme_rdma_queue *queue)
{
    struct qd_rw_based_queue_data *queue_data;
    int ret;

    queue_data = kmalloc(sizeof(*queue_data), GFP_KERNEL);
    if (!queue_data)
        return -ENOMEM;

    queue_data->depth = 0;
    queue_data->max_depth = queue->queue_size;

    /*
     * Only use one bit per word to prevent conflicts.
     */
    ret = sbitmap_init_node(&queue_data->tags,
            queue_data->max_depth, 0, GFP_KERNEL, NUMA_NO_NODE);
    if (ret) {
        kfree(queue_data);
        return ret;
    }

    queue->scheduler_data.ptr = queue_data;
    return 0;
}

static void qd_rw_based_free_queue_data(struct nvme_rdma_queue *queue)
{
    struct qd_rw_based_queue_data *queue_data = queue->scheduler_data.ptr;
    queue->scheduler_data.ptr = NULL;

    sbitmap_free(&queue_data->tags);
    kfree(queue_data);
}

struct nvme_rdma_scheduler qd_rw_based_scheduler = {
    .name = "qd_rw_based",
    .should_offload = qd_rw_based_should_offload,
    .prepare_req = qd_rw_based_prepare_req,
    .abort_req = qd_rw_based_finish_req,
    .finish_req = qd_rw_based_finish_req,
    .init_queue_data = qd_rw_based_init_queue_data,
    .free_queue_data = qd_rw_based_free_queue_data,
};

static int __init qd_rw_based_init(void)
{
    BUILD_BUG_ON(REQ_OP_READ != 0);
    BUILD_BUG_ON(REQ_OP_WRITE != 1);
	return nvme_rdma_register_scheduler(&qd_rw_based_scheduler);
}

static void __exit qd_rw_based_exit(void)
{
	nvme_rdma_unregister_scheduler(&qd_rw_based_scheduler);
}

module_init(qd_rw_based_init);
module_exit(qd_rw_based_exit);

MODULE_LICENSE("GPL v2");