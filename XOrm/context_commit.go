// Copyright (c) 2025 EFramework Innovation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package XOrm

import (
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/eframework-io/Go.Utility/XLog"
	"github.com/eframework-io/Go.Utility/XLoom"
	"github.com/eframework-io/Go.Utility/XPrefs"
	"github.com/eframework-io/Go.Utility/XTime"
	"github.com/illumitacit/gostd/quit"
	"github.com/petermattis/goid"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	// preferencesContextCommitQueueCount 定义了提交队列的数量的偏好设置键。
	preferencesContextCommitQueueCount = "XOrm/Context/Commit/Queue"

	// preferencesContextCommitQueueCapacity 定义了单个队列的容量的偏好设置键。
	preferencesContextCommitQueueCapacity = "XOrm/Context/Commit/Capacity"
)

var (
	// contextCommitQueueCount 定义了提交队列的数量，默认为 CPU 核心数。
	contextCommitQueueCount int = runtime.NumCPU()

	// contextCommitQueueCapacity 定义了单个队列的容量，当超过此容量时，新的批次将被丢弃。
	contextCommitQueueCapacity int = 100000

	// contextCommitQueues 定义了提交队列的切片，用于缓冲待处理的批次数据。
	contextCommitQueues []chan *commitBatch

	// contextCommitGauge 定义了提交队列的计数器，用于统计所有队列等待提交的对象数量。
	contextCommitGauge prometheus.Gauge

	// contextCommitCounter 定义了提交队列的计数器，用于统计所有队列已经提交的对象总数。
	contextCommitCounter prometheus.Counter

	// contextCommitGauges 定义了提交队列的计数器，用于统计指定队列等待提交的对象数量。
	contextCommitGauges []prometheus.Gauge

	// contextCommitCounters 定义了提交队列的计数器，用于统计指定队列已经提交的对象总数。
	contextCommitCounters []prometheus.Counter

	// contextCommitSetupSig 定义了提交队列的信号通道，用于接收退出信号。
	contextCommitSetupSig []chan os.Signal

	// contextCommitFlushWait 定义了提交队列的等待通道，用于等待批次处理完成。
	contextCommitFlushWait []chan *sync.WaitGroup

	// contextCommitCloseWait 定义了提交队列的关闭通道，用于等待所有队列关闭完成。
	contextCommitCloseWait sync.WaitGroup

	// contextCommitFlushSig 定义了提交批次是否已刷新的标志，用于控制批次的状态。
	contextCommitFlushSig int32

	// contextCommitCloseSig 定义了提交队列是否已关闭的标志，用于控制队列的状态。
	contextCommitCloseSig int32

	// contextCommitBatchPool 定义了批次对象的对象池，用于重用已创建的批次对象。
	contextCommitBatchPool sync.Pool = sync.Pool{
		New: func() any {
			obj := new(commitBatch)
			obj.reset()
			return obj
		},
	}
)

func init() { initContextCommit(XPrefs.Asset()) }

// initContextCommit 初始化提交队列。
// 该函数会从 preferences 中获取提交队列的数量和批次大小，并启动提交队列循环。
func initContextCommit(preferences XPrefs.IBase) {
	Close()

	contextCommitQueueCount = preferences.GetInt(preferencesContextCommitQueueCount, runtime.NumCPU())
	if contextCommitQueueCount < 0 {
		XLog.Notice("XOrm.Context.Commit: ignore to setup commit queue.")
		return
	}

	contextCommitQueueCapacity = preferences.GetInt(preferencesContextCommitQueueCapacity, 100000)
	if contextCommitQueueCapacity <= 0 {
		XLog.Panic("XOrm.Context.Commit: capacity of queue is negative or zero: %v.", contextCommitQueueCapacity)
		return
	}

	contextCommitQueues = make([]chan *commitBatch, contextCommitQueueCount)
	contextCommitGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "xorm_context_commit_queue",
		Help: "The total number of pending commit objects.",
	})
	prometheus.MustRegister(contextCommitGauge)
	contextCommitCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "xorm_context_commit_total",
		Help: "The total number of committed objects.",
	})
	prometheus.MustRegister(contextCommitCounter)
	contextCommitGauges = make([]prometheus.Gauge, contextCommitQueueCount)
	contextCommitCounters = make([]prometheus.Counter, contextCommitQueueCount)
	contextCommitSetupSig = make([]chan os.Signal, contextCommitQueueCount)
	contextCommitFlushWait = make([]chan *sync.WaitGroup, contextCommitQueueCount)
	for i := range contextCommitQueueCount {
		contextCommitQueues[i] = make(chan *commitBatch, contextCommitQueueCapacity)
		contextCommitSetupSig[i] = make(chan os.Signal, 1)
		contextCommitFlushWait[i] = make(chan *sync.WaitGroup, 1)
	}

	contextCommitCloseWait = sync.WaitGroup{}
	atomic.StoreInt32(&contextCommitFlushSig, 0)
	atomic.StoreInt32(&contextCommitCloseSig, 0)

	// 启动提交队列线程
	wg := sync.WaitGroup{}
	for i := range contextCommitQueueCount {
		wg.Add(1)

		contextCommitGauges[i] = prometheus.NewGauge(prometheus.GaugeOpts{
			Name: fmt.Sprintf("xorm_context_commit_queue_%v", i),
			Help: fmt.Sprintf("The total number of pending commit objects in queue %v.", i),
		})
		prometheus.MustRegister(contextCommitGauges[i])

		contextCommitCounters[i] = prometheus.NewCounter(prometheus.CounterOpts{
			Name: fmt.Sprintf("xorm_context_commit_total_%v", i),
			Help: fmt.Sprintf("The total number of committed objects in queue %v.", i),
		})
		prometheus.MustRegister(contextCommitCounters[i])

		XLoom.RunAsyncT2(func(queueID int, doneOnce *sync.Once) {
			setupSig := contextCommitSetupSig[queueID]
			signal.Notify(setupSig, syscall.SIGTERM, syscall.SIGINT)

			quit.GetWaiter().Add(1)
			contextCommitCloseWait.Add(1)
			doneOnce.Do(func() { // 确保只调用一次，否则recover后会重复调用
				wg.Done() // 确保线程启动完成
			})

			flushSig := contextCommitFlushWait[queueID]
			queue := contextCommitQueues[queueID] // 提交队列，用于接收批次数据

			defer func() {
				// 处理剩余的批次
				for {
					if len(queue) > 0 {
						batch := <-queue
						batch.push(queueID)
						continue
					} else {
						break
					}
				}
				quit.GetWaiter().Done()
				contextCommitCloseWait.Done()
			}()

			for {
				select {
				case batch := <-queue:
					if batch == nil {
						return
					}
					batch.push(queueID)
				case fwg := <-flushSig:
					for {
						if len(queue) > 0 {
							batch := <-queue
							batch.push(queueID)
							continue
						} else {
							break
						}
					}
					fwg.Done()
				case sig, ok := <-setupSig:
					if ok {
						XLog.Notice("XOrm.Context.Commit(%v): receive signal of %v.", queueID, sig.String())
					} else {
						XLog.Notice("XOrm.Context.Commit(%v): channel of signal is closed.", queueID)
					}
					return
				case <-quit.GetQuitChannel():
					XLog.Notice("XOrm.Context.Commit(%v): receive signal of quit.", queueID)
					return
				}
			}
		}, i, &sync.Once{}, true)
	}
	wg.Wait()

	XLog.Notice("XOrm.Context.Commit: commit queue size is %v, and each queue has a capacity of %v.", contextCommitQueueCount, contextCommitQueueCapacity)
}

// commitBatch 定义了一批需要处理的数据对象，用于批量异步处理数据变更。
type commitBatch struct {
	tag         *XLog.LogTag                                  // 日志标签，用于追踪批次处理
	stime       int                                           // 批次提交时间
	objects     []*sessionObject                              // 待处理的对象列表
	prehandler  func(batch *commitBatch, sobj *sessionObject) // 预处理函数，在处理对象前调用
	posthandler func(batch *commitBatch, sobj *sessionObject) // 后处理函数，在处理对象后调用
}

// reset 重置批次对象的状态，在批次被放回对象池前调用。
func (cb *commitBatch) reset() {
	cb.tag = nil
	cb.stime = 0
	cb.objects = nil
	cb.prehandler = nil
	cb.posthandler = nil
}

// submit 提交批次对象至队列中，等待被处理。
func (cb *commitBatch) submit(gid ...int64) {
	if atomic.LoadInt32(&contextCommitCloseSig) > 0 {
		return
	}

	cb.stime = XTime.GetMicrosecond()

	var ggid int64
	if len(gid) > 0 {
		ggid = gid[0]
	} else {
		ggid = goid.Get()
	}

	// 确保 queue ID 在 0 到 commitQueueCount 之间，相同的 goroutine ID 会被分配到同一个队列。
	queueID := max(int(ggid)%contextCommitQueueCount, 0)
	queue := contextCommitQueues[queueID]

	select {
	case queue <- cb:
		// 更新数据度量。
		delta := float64(len(cb.objects))
		contextCommitGauge.Add(delta)
		contextCommitGauges[queueID].Add(delta)
	default:
		XLog.Critical("XOrm.Commit.Submit: too many data to submit.")
	}
}

// push 推送批次对象至远端数据库或缓存中。
// queueID 是批次对象所属的队列 ID。
func (cb *commitBatch) push(queueID int) {
	if cb.tag != nil {
		XLog.Watch(cb.tag)
	}
	pendingTime := XTime.GetMicrosecond() - cb.stime
	nowTime := XTime.GetMicrosecond()

	// 优先处理清除操作，尽早释放全局锁，提高效率
	// 其次处理删除操作，尽早释放全局锁，提高效率
	// 最后处理写入操作
	sort.Slice(cb.objects, func(i, j int) bool {
		oi := cb.objects[i]
		oj := cb.objects[j]

		var pi, pj int
		if oi.clear != nil {
			pi = 0
		} else if oi.delete {
			pi = 1
		} else {
			pi = 2
		}

		if oj.clear != nil {
			pj = 0
		} else if oj.delete {
			pj = 1
		} else {
			pj = 2
		}

		return pi < pj
	})

	for _, cobj := range cb.objects {
		cb.handle(cobj, queueID)
	}

	costTime := XTime.GetMicrosecond() - nowTime
	XLog.Notice("XOrm.Commit.Push: processed %v object(s), cost %.2fms, pending %.2fms.", len(cb.objects), float64(costTime)/1e3, float64(pendingTime)/1e3)

	if cb.tag != nil {
		XLog.Defer()
	}

	cb.reset()
	contextCommitBatchPool.Put(cb)
}

// handle 处理批次对象中的单个数据对象。
// queueID 是批次对象所属的队列 ID。
func (cb *commitBatch) handle(sobj *sessionObject, queueID int) {
	startTime := XTime.GetMicrosecond()
	obj := sobj.ptr
	key := obj.DataUnique()

	// 回调预处理函数。
	if cb.prehandler != nil {
		cb.prehandler(cb, sobj)
	}

	action := ""
	if sobj.create {
		obj.Write()
		action = "create"
	} else if sobj.delete {
		obj.Delete()
		action = "delete"
	} else if sobj.clear != nil {
		obj.Clear(sobj.clear)
		action = "clear"
	} else {
		obj.Write()
		action = "update"
	}

	// 回调后处理函数。
	if cb.posthandler != nil {
		cb.posthandler(cb, sobj)
	}

	// 更新数据度量。
	contextCommitCounter.Inc()
	contextCommitCounters[queueID].Inc()
	contextCommitGauge.Dec()
	contextCommitGauges[queueID].Dec()

	if action != "" {
		t2 := XTime.GetMicrosecond()
		XLog.Notice("XOrm.Commit.Push: %v %v cost %.2fms, object: %v.", action, key, float64(t2-startTime)/1e3, obj.Json())
	}
}

// Flush 将等待指定的队列提交完成。
// gid 参数为 goroutine ID，若未指定，则使用当前 goroutine ID，
// 若 gid 为 -1，则表示等待所有的队列提交完成。
func Flush(gid ...int64) {
	if atomic.LoadInt32(&contextCommitCloseSig) == 0 {
		var ggid int64
		if len(gid) > 0 {
			ggid = gid[0]
		} else {
			ggid = goid.Get()
		}
		if ggid == -1 {
			if atomic.CompareAndSwapInt32(&contextCommitFlushSig, 0, 1) {
				for index := range contextCommitQueues {
					sig := contextCommitFlushWait[index]
					if sig != nil {
						wg := &sync.WaitGroup{}
						wg.Add(1)
						sig <- wg
						wg.Wait()
						XLog.Notice("XOrm.Flush: batches of commit queue-%v has been flushed.", index)
					}
				}
				atomic.CompareAndSwapInt32(&contextCommitFlushSig, 1, 0)
				XLog.Notice("XOrm.Flush: batches of all commit queue has been flushed.")
			}
		} else {
			queueID := max(int(ggid)%contextCommitQueueCount, 0)
			sig := contextCommitFlushWait[queueID]
			if sig != nil {
				wg := &sync.WaitGroup{}
				wg.Add(1)
				sig <- wg
				wg.Wait()
				XLog.Notice("XOrm.Flush: batches of commit queue-%v has been flushed.", queueID)
			}
		}
	}
}

// Close 关闭所有的提交队列并等待所有未完成的批次处理完成。
// 此函数会发送退出信号并等待所有队列完成当前工作。
func Close() {
	if atomic.CompareAndSwapInt32(&contextCommitCloseSig, 0, 1) {
		for _, sig := range contextCommitSetupSig {
			signal.Stop(sig)
			close(sig)
		}
		// 等待所有队列完成。
		contextCommitCloseWait.Wait()

		// 注销数据度量。
		if contextCommitGauge != nil {
			prometheus.Unregister(contextCommitGauge)
		}
		if contextCommitCounter != nil {
			prometheus.Unregister(contextCommitCounter)
		}
		if len(contextCommitGauges) != 0 {
			for _, gauge := range contextCommitGauges {
				prometheus.Unregister(gauge)
			}
		}
		if len(contextCommitCounters) != 0 {
			for _, counter := range contextCommitCounters {
				prometheus.Unregister(counter)
			}
		}
	}
}
