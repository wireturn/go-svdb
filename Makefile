.PYONY:all
all:
	make -C block_crawler
	make -C output_crawler
	make -C input_crawler
	make -C monitored_addr_crawler
	make -C api
	make -C tx_listener
	make -C sync_center
	make -C test
