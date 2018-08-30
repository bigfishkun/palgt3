# coding=utf-8

from __future__ import unicode_literals, print_function
from pprint import pprint
from time import time, sleep

from threadpool.mpms import MPMS, Meta
from proxy.proxy import proxy
import basic_crawler as bc

@proxy
def worker(index, t=None):
    bc.crawl_basic('2018-04-17', '2018-12-31')

def collector(meta, result):
    pass

def main():

    processes = 1
    threads_per_process = 1

    # Init the poll  # 初始化
    m = MPMS(
        worker,
        collector,
        processes=processes,  # optional, how many processes, default value is your cpu core number
        threads=threads_per_process,  # optional, how many threads per process, default is 2
        meta={"any": 1, "dict": "you", "want": {"pass": "to"}, "worker": 0.5},
    )
    m.start()  # start and fork subprocess
    start_time = time()  # when we started  # 记录开始时间

    # put task parameters into the task jobqueue, 2000 total tasks
    # 把任务加入任务队列,一共2000次
    for i in range(2000):
        m.put(i, t=time())

    # optional, close the task jobqueue. jobqueue will be auto closed when join()
    # 关闭任务队列,可选. 在join()的时候会自动关闭
    # m.close()

    # close task jobqueue and wait all workers and handler to finish
    # 等待全部任务及全部结果处理完成
    m.join()

    print('sleeping 5s before next')
    sleep(5)


if __name__ == '__main__':
    main()