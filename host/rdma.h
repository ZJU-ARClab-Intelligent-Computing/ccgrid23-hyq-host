#ifndef __NVME_RDMA_H
#define __NVME_RDMA_H

#include <linux/blk-mq.h>
#include <linux/list.h>
#include <linux/nvme.h>
#include <linux/nvme-rdma.h>
#include <linux/types.h>

#include <rdma/ib_verbs.h>
#include <rdma/rdma_cm.h>

#include <linux/miscdevice.h>

#include "nvme.h"
#include "fabrics.h"

#define NVME_RDMA_MAX_INLINE_SEGMENTS 4

typedef union {
    u64 val;
    void *ptr;
} scheduler_data_t;

struct nvme_rdma_device
{
    struct ib_device *dev;
    struct ib_pd *pd;
    struct kref ref;
    struct list_head entry;
    unsigned int num_inline_segments;
};

struct nvme_rdma_qe
{
    struct ib_cqe cqe;
    void *data;
    u64 dma;
};

struct nvme_rdma_sgl
{
    int nents;
    struct sg_table sg_table;
};

struct nvme_rdma_path;
struct nvme_rdma_queue;
struct nvme_rdma_request
{
    struct nvme_request req;
    struct ib_mr *mr;
    struct nvme_rdma_qe sqe;
    union nvme_result result;
    __le16 status;
    refcount_t ref;
    struct ib_sge sge[1 + NVME_RDMA_MAX_INLINE_SEGMENTS];
    u32 num_sge;
    struct ib_reg_wr reg_wr;
    struct ib_cqe reg_cqe;

    /**
     * SCHED: The path the request goes.
     */
    struct nvme_rdma_path *path;

    struct nvme_rdma_queue *queue;
    struct nvme_rdma_sgl data_sgl;
    struct nvme_rdma_sgl *metadata_sgl;
    bool use_sig_mr;

    /**
     * SCHED: Request specific data for scheduling.
     */
    scheduler_data_t scheduler_data;
};

enum nvme_rdma_path_flags
{
    NVME_RDMA_PATH_ALLOCATED = 0,
    NVME_RDMA_PATH_LIVE = 1,
    NVME_RDMA_PATH_TR_READY = 2,
};

/**
 * SCHED: The new path structure.
 *
 * Each path represents an RDMA connection. The connection may target at the host
 * CPU or the smart NIC in the target end.
 */
struct nvme_rdma_path
{
    struct nvme_rdma_qe *rsp_ring;
    struct nvme_rdma_device *device;
    struct ib_cq *ib_cq;
    int cq_size;
    struct ib_qp *qp;
    unsigned long flags;
    struct rdma_cm_id *cm_id;

    // RDMA connection results
    int cm_error;
    struct completion cm_done;

    // NVMeoF fabrics connection results
    int fc_error;
    struct completion fc_done;

    struct mutex path_lock;
    bool offload;
};

/**
 * SCHED: The modified queue structure.
 *
 * Each queue contains two paths, one offloaded and the other non-offloaded.
 * Note that only the non-offloaded path is alive for admin queue.
 */
struct nvme_rdma_queue
{
    int queue_size;
    size_t cmnd_capsule_len;
    struct nvme_rdma_ctrl *ctrl;

    // Kernel path
    struct nvme_rdma_path knl_path;

    // Offload path
    struct nvme_rdma_path ofd_path;

    bool pi_support;

    /**
     * SCHED: Queue specific data for scheduling.
     */
    scheduler_data_t scheduler_data;
};

struct nvme_rdma_scheduler;
struct nvme_rdma_ctrl
{
    /* read only in the hot path */
    struct nvme_rdma_queue *queues;

    /* other member variables */
    struct blk_mq_tag_set tag_set;
    struct work_struct err_work;

    struct nvme_rdma_qe async_event_sqe;

    struct delayed_work reconnect_work;

    struct list_head list;

    struct blk_mq_tag_set admin_tag_set;
    struct nvme_rdma_device *device;

    u32 max_fr_pages;

    struct sockaddr_storage addr;
    struct sockaddr_storage src_addr;

    struct nvme_ctrl ctrl;
    bool use_inline_data;
    u32 io_queues[HCTX_MAX_TYPES];

    /**
     * SCHED: Controller specific data for scheduling.
     */
    scheduler_data_t scheduler_data;

    /**
     * SCHED: The scheduler assigned to this controller.
     */
    struct nvme_rdma_scheduler *scheduler;

    /**
     * SCHED: The misc device for managing the scheduler of the controller.
     */
    struct miscdevice mdevice;
};

struct nvme_rdma_scheduler
{
    struct list_head entry;

    char *name;

    /**
     * SCHED: The core scheduling function.
     *
     * The scheduler determines wether the request should be offloaded in this functon.
     */
    bool (*should_offload)(struct nvme_rdma_request *req);

    /**
     * SCHED: Prepare the request for scheduling.
     */
    void (*prepare_req)(struct nvme_rdma_request *req);

    /**
     * SCHED: Abort the request, cleanup data structures created by prepare_req.
     */
    void (*abort_req)(struct nvme_rdma_request *req);

    /**
     * SCHED: Mark the begaining of the request.
     */
    void (*start_req)(struct nvme_rdma_request *req);

    /**
     * SCHED: Mark the finishing the request and clean up request
     * data structures created by the scheduler.
     */
    void (*finish_req)(struct nvme_rdma_request *req);

    /**
     * SCHED: Init and free controller specific data structures.
     */
    int (*init_ctrl_data)(struct nvme_rdma_ctrl *ctrl);
    void (*free_ctrl_data)(struct nvme_rdma_ctrl *ctrl);

    /**
     * SCHED: Init and free queue specific data structures.
     */
    int (*init_queue_data)(struct nvme_rdma_queue *queue);
    void (*free_queue_data)(struct nvme_rdma_queue *queue);
};

static inline int nvme_rdma_queue_idx(struct nvme_rdma_queue *queue)
{
	return queue - queue->ctrl->queues;
}

/**
 * SCHED: Scheduler hook: clean up on request finishing.
 */
static inline void nvme_rdma_end_request(struct nvme_rdma_request *req)
{
    struct nvme_rdma_scheduler *scheduler = req->queue->ctrl->scheduler;
    if (nvme_rdma_queue_idx(req->queue) && scheduler && scheduler->finish_req)
        scheduler->finish_req(req);

    // Finish original process first.
    nvme_end_request(blk_mq_rq_from_pdu(req), req->status, req->result);
}

/**
 * SCHED: Register a scheduler.
 *
 * @param scheduler the scheduler
 * @return int 0 on succeed, error code for fail
 */
int nvme_rdma_register_scheduler(struct nvme_rdma_scheduler *scheduler);

/**
 * SCHED: Unregister a scheduler.
 *
 * @param scheduler the scheduler
 */
void nvme_rdma_unregister_scheduler(struct nvme_rdma_scheduler *scheduler);

/**
 * @brief Check whether a request is crossing the 128k boundary.
 * 
 * @param rq the request
 * @return true it is crossing the boundary
 * @return false it is not crossing the boundary
 */
static inline bool crossing_128k_boundary(struct request *rq) {
	static const u64 mask = ~((SZ_128K >> SECTOR_SHIFT) - 1);

    sector_t addr = blk_rq_pos(rq);
    sector_t size = blk_rq_sectors(rq);

	return size > 0 && (addr & mask) != ((addr + size - 1) & mask);
}

#endif