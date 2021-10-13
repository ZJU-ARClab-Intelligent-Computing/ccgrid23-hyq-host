# SPDX-License-Identifier: GPL-2.0-only

obj-y		+= host/
obj-y		+= target/

PWD=$(shell pwd)
KHEADERS=$(shell uname -r)

default:
	make -C /lib/modules/$(KHEADERS)/build M=$(PWD) modules -j40

install: default
	make -C /lib/modules/$(KHEADERS)/build M=$(PWD) INSTALL_MOD_DIR=updates/drivers/nvme modules_install -j40

clean:
	make -C /lib/modules/$(KHEADERS)/build M=$(PWD) clean