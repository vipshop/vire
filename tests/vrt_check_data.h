#ifndef _VRT_CHECK_DATA_H_
#define _VRT_CHECK_DATA_H_

int vrt_data_checker_init(char *checker, char *test_target_groups);
void vrt_data_checker_deinit(void);

int vrt_start_data_checker(void);
int vrt_wait_data_checker(void);

int test_if_need_pause(void);
void test_can_continue(void);
void test_need_to_pause(void);

void one_produce_thread_paused(void);
void one_dispatch_thread_paused(void);
void one_backend_thread_paused(void);

int all_produce_threads_paused(void);
int all_dispatch_threads_paused(void);
int all_backend_threads_paused(void);
int all_threads_paused(void);

#endif
