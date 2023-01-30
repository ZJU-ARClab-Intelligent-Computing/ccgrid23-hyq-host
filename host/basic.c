#include <linux/init.h>
#include <linux/module.h>

#include "rdma.h"

static bool offload = false;
module_param(offload, bool, 0644);
MODULE_PARM_DESC(offload, "Wether to enable offloading.");

static inline bool basic_should_offload(struct nvme_rdma_request *req)
{
	return offload;
}

struct nvme_rdma_scheduler basic_scheduler = {
    .name = "basic",
    .should_offload = basic_should_offload,
};

static int __init basic_init(void)
{
	return nvme_rdma_register_scheduler(&basic_scheduler);
}

static void __exit basic_exit(void)
{
	nvme_rdma_unregister_scheduler(&basic_scheduler);
}

module_init(basic_init);
module_exit(basic_exit);

MODULE_LICENSE("GPL v2");