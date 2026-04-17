#include "duckdb/parallel/task_scheduler.hpp"

#include "duckdb/common/chrono.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/storage/block_allocator.hpp"
#ifndef DUCKDB_NO_THREADS
#include "concurrentqueue.h"
#include "duckdb/common/thread.hpp"
#include "lightweightsemaphore.h"

#include <cstdio>
#include <deque>
#include <thread>
#else
#include <queue>
#endif

#if defined(_WIN32)
#include <windows.h>
#elif defined(__GNUC__)
#include <sched.h>
#include <unistd.h>
#if defined(__GLIBC__)
#include <pthread.h>
#endif
#endif

namespace duckdb {
struct SchedulerThread {
#ifndef DUCKDB_NO_THREADS
	explicit SchedulerThread(unique_ptr<thread> thread_p) : internal_thread(std::move(thread_p)) {
	}

	unique_ptr<thread> internal_thread;
#endif
};

#ifndef DUCKDB_NO_THREADS
typedef duckdb_moodycamel::LightweightSemaphore lightweight_semaphore_t;

struct ConcurrentQueue {
	ConcurrentQueue() : tasks_in_queue(0), dequeue_count(0) {
	}

	lightweight_semaphore_t semaphore;

	// MLFQ: three priority levels
	// Q0 = high priority (new queries), Q1 = medium, Q2 = low (long-running)
	struct TaskEntry {
		ProducerToken *producer;
		shared_ptr<Task> task;
		std::chrono::steady_clock::time_point enqueue_time;
	};
	std::deque<TaskEntry> q0;
	std::deque<TaskEntry> q1;
	std::deque<TaskEntry> q2;

	// Separate locks to reduce contention:
	// queue_lock: protects q0/q1/q2 deques
	// state_lock: protects morsel counts and priority levels
	mutable mutex queue_lock;
	mutable mutex state_lock;

	// Demotion thresholds (completed tasks before moving to next level)
	static constexpr idx_t Q0_THRESHOLD = 8;
	static constexpr idx_t Q1_THRESHOLD = 40;

	// Aging threshold: promote a task if it has waited longer than this
	static constexpr int64_t AGING_THRESHOLD_MS = 500;

	// Only run aging check every N dequeues to reduce overhead
	static constexpr idx_t AGING_CHECK_INTERVAL = 100;

	// Per-query state: morsel counts and current priority level
	unordered_map<uint64_t, idx_t> query_morsel_counts;
	unordered_map<uint64_t, int> query_priority_levels;

	void Enqueue(ProducerToken &token, shared_ptr<Task> task);
	void EnqueueBulk(ProducerToken &token, vector<shared_ptr<Task>> &tasks);
	bool DequeueFromProducer(ProducerToken &token, shared_ptr<Task> &task);
	bool Dequeue(shared_ptr<Task> &task);
	void NotifyTaskComplete(uint64_t query_id);
	idx_t GetTasksInQueue() const;
	idx_t GetApproxSize() const;
	idx_t GetProducerCount() const;
	idx_t GetTaskCountForProducer(ProducerToken &token) const;

private:
	// Must be called with state_lock held
	int GetQueryLevelLocked(uint64_t qid) {
		auto it = query_priority_levels.find(qid);
		return (it != query_priority_levels.end()) ? it->second : 0;
	}

	// Aging: promote only the specific tasks that have waited too long.
	// Must be called with queue_lock held.
	void AgeTasks() {
		auto now = std::chrono::steady_clock::now();
		// Check front of Q1
		if (!q1.empty()) {
			auto wait = std::chrono::duration_cast<std::chrono::milliseconds>(now - q1.front().enqueue_time).count();
			if (wait >= AGING_THRESHOLD_MS) {
				uint64_t qid = q1.front().task->query_id;
				std::fprintf(stderr, "[MLFQ] AGING: query %llu promoted Q1 -> Q0 (waited %lldms)\n",
				             (unsigned long long)qid, (long long)wait);
				q1.front().task->priority_level = 0;
				q0.push_back(std::move(q1.front()));
				q1.pop_front();
			}
		}
		// Check front of Q2
		if (!q2.empty()) {
			auto wait = std::chrono::duration_cast<std::chrono::milliseconds>(now - q2.front().enqueue_time).count();
			if (wait >= AGING_THRESHOLD_MS) {
				uint64_t qid = q2.front().task->query_id;
				std::fprintf(stderr, "[MLFQ] AGING: query %llu promoted Q2 -> Q0 (waited %lldms)\n",
				             (unsigned long long)qid, (long long)wait);
				q2.front().task->priority_level = 0;
				q0.push_back(std::move(q2.front()));
				q2.pop_front();
			}
		}
	}

	atomic<idx_t> tasks_in_queue;
	atomic<idx_t> dequeue_count;
};

struct QueueProducerToken {
	explicit QueueProducerToken(ConcurrentQueue &queue) {
		// No per-producer lock-free token needed in MLFQ mode
	}
};

void ConcurrentQueue::Enqueue(ProducerToken &token, shared_ptr<Task> task) {
	if (task->query_id == 0) {
		task->query_id = reinterpret_cast<uint64_t>(&token);
	}
	task->token = token;
	uint64_t qid = task->query_id;

	// Read priority level under state_lock (separate from queue_lock)
	int level;
	{
		lock_guard<mutex> sl(state_lock);
		level = GetQueryLevelLocked(qid);
	}
	task->priority_level = level;

	{
		lock_guard<mutex> ql(queue_lock);
		TaskEntry entry = {&token, std::move(task), std::chrono::steady_clock::now()};
		if (level == 0) {
			q0.push_back(std::move(entry));
		} else if (level == 1) {
			q1.push_back(std::move(entry));
		} else {
			q2.push_back(std::move(entry));
		}
		++tasks_in_queue;
	}
	semaphore.signal();
}

void ConcurrentQueue::EnqueueBulk(ProducerToken &token, vector<shared_ptr<Task>> &tasks) {
	typedef std::make_signed<std::size_t>::type ssize_t;

	// Read all levels under state_lock first, then enqueue under queue_lock
	vector<int> levels(tasks.size());
	{
		lock_guard<mutex> sl(state_lock);
		for (idx_t i = 0; i < tasks.size(); i++) {
			if (tasks[i]->query_id == 0) {
				tasks[i]->query_id = reinterpret_cast<uint64_t>(&token);
			}
			levels[i] = GetQueryLevelLocked(tasks[i]->query_id);
		}
	}

	{
		lock_guard<mutex> ql(queue_lock);
		for (idx_t i = 0; i < tasks.size(); i++) {
			auto &task = tasks[i];
			task->token = token;
			task->priority_level = levels[i];
			TaskEntry entry = {&token, std::move(task), std::chrono::steady_clock::now()};
			if (levels[i] == 0) {
				q0.push_back(std::move(entry));
			} else if (levels[i] == 1) {
				q1.push_back(std::move(entry));
			} else {
				q2.push_back(std::move(entry));
			}
		}
		tasks_in_queue += tasks.size();
	}
	semaphore.signal(NumericCast<ssize_t>(tasks.size()));
}

bool ConcurrentQueue::DequeueFromProducer(ProducerToken &token, shared_ptr<Task> &task) {
	lock_guard<mutex> lock(queue_lock);
	// Search priority queues in order, returning only tasks from this producer
	for (auto *q : {&q0, &q1, &q2}) {
		for (auto it = q->begin(); it != q->end(); ++it) {
			if (it->producer == &token) {
				task = std::move(it->task);
				q->erase(it);
				--tasks_in_queue;
				return true;
			}
		}
	}
	return false;
}

bool ConcurrentQueue::Dequeue(shared_ptr<Task> &task) {
	lock_guard<mutex> lock(queue_lock);
	// Only run aging every AGING_CHECK_INTERVAL dequeues to reduce overhead
	if (dequeue_count.fetch_add(1, std::memory_order_relaxed) % AGING_CHECK_INTERVAL == 0) {
		AgeTasks();
	}
	// Always serve highest priority queue first
	if (!q0.empty()) {
		task = std::move(q0.front().task);
		q0.pop_front();
		--tasks_in_queue;
		return true;
	}
	if (!q1.empty()) {
		task = std::move(q1.front().task);
		q1.pop_front();
		--tasks_in_queue;
		return true;
	}
	if (!q2.empty()) {
		task = std::move(q2.front().task);
		q2.pop_front();
		--tasks_in_queue;
		return true;
	}
	return false;
}

void ConcurrentQueue::NotifyTaskComplete(uint64_t query_id) {
	// Uses state_lock only — does not block Dequeue/Enqueue
	lock_guard<mutex> sl(state_lock);
	auto &count = query_morsel_counts[query_id];
	count++;
	auto &level = query_priority_levels[query_id];
	// Log morsel progress every 10 completions
	if (count % 10 == 0) {
		std::fprintf(stderr, "[MLFQ] PROGRESS: query %llu morsels=%llu level=Q%d\n",
		             (unsigned long long)query_id, (unsigned long long)count, level);
	}
	if (level == 0 && count >= Q0_THRESHOLD) {
		level = 1;
		std::fprintf(stderr, "[MLFQ] DEMOTE: query %llu Q0 -> Q1 (morsels: %llu)\n",
		             (unsigned long long)query_id, (unsigned long long)count);
	} else if (level == 1 && count >= Q1_THRESHOLD) {
		level = 2;
		std::fprintf(stderr, "[MLFQ] DEMOTE: query %llu Q1 -> Q2 (morsels: %llu)\n",
		             (unsigned long long)query_id, (unsigned long long)count);
	}
}

idx_t ConcurrentQueue::GetTasksInQueue() const {
	return tasks_in_queue;
}

idx_t ConcurrentQueue::GetApproxSize() const {
	lock_guard<mutex> lock(queue_lock);
	return q0.size() + q1.size() + q2.size();
}

idx_t ConcurrentQueue::GetProducerCount() const {
	return 0;
}

idx_t ConcurrentQueue::GetTaskCountForProducer(ProducerToken &token) const {
	lock_guard<mutex> lock(queue_lock);
	idx_t count = 0;
	for (auto *q : {&q0, &q1, &q2}) {
		for (auto &entry : *q) {
			if (entry.producer == &token) {
				count++;
			}
		}
	}
	return count;
}

#else
struct ConcurrentQueue {
	reference_map_t<QueueProducerToken, std::queue<shared_ptr<Task>>> q;
	mutable mutex qlock;

	void Enqueue(ProducerToken &token, shared_ptr<Task> task);
	void EnqueueBulk(ProducerToken &token, vector<shared_ptr<Task>> &tasks);
	bool DequeueFromProducer(ProducerToken &token, shared_ptr<Task> &task);
	bool Dequeue(shared_ptr<Task> &task);
	idx_t GetTasksInQueue() const;
	idx_t GetApproxSize() const;
	idx_t GetProducerCount() const;
	idx_t GetTaskCountForProducer(ProducerToken &token) const;
};

void ConcurrentQueue::Enqueue(ProducerToken &token, shared_ptr<Task> task) {
	lock_guard<mutex> lock(qlock);
	task->token = token;
	q[std::ref(*token.token)].push(std::move(task));
}

void ConcurrentQueue::EnqueueBulk(ProducerToken &token, vector<shared_ptr<Task>> &tasks) {
	lock_guard<mutex> lock(qlock);
	for (auto &task : tasks) {
		task->token = token;
		q[std::ref(*token.token)].push(std::move(task));
	}
}

bool ConcurrentQueue::DequeueFromProducer(ProducerToken &token, shared_ptr<Task> &task) {
	lock_guard<mutex> lock(qlock);
	D_ASSERT(!q.empty());

	const auto it = q.find(std::ref(*token.token));
	if (it == q.end() || it->second.empty()) {
		return false;
	}

	task = std::move(it->second.front());
	it->second.pop();

	return true;
}

bool ConcurrentQueue::Dequeue(shared_ptr<Task> &task) {
	throw InternalException("Global dequeue not supported for no threads queue");
}

idx_t ConcurrentQueue::GetTasksInQueue() const {
	lock_guard<mutex> lock(qlock);
	idx_t task_count = 0;
	for (auto &producer : q) {
		task_count += producer.second.size();
	}
	return task_count;
}

idx_t ConcurrentQueue::GetApproxSize() const {
	return GetTasksInQueue();
}

idx_t ConcurrentQueue::GetProducerCount() const {
	lock_guard<mutex> lock(qlock);
	return q.size();
}

idx_t ConcurrentQueue::GetTaskCountForProducer(ProducerToken &token) const {
	lock_guard<mutex> lock(qlock);
	const auto it = q.find(std::ref(*token.token));
	if (it == q.end()) {
		return 0;
	}
	return it->second.size();
}

struct QueueProducerToken {
	explicit QueueProducerToken(ConcurrentQueue &queue) : queue(&queue) {
	}

	~QueueProducerToken() {
		lock_guard<mutex> lock(queue->qlock);
		queue->q.erase(*this);
	}

private:
	ConcurrentQueue *queue;
};
#endif

ProducerToken::ProducerToken(TaskScheduler &scheduler, unique_ptr<QueueProducerToken> token)
    : scheduler(scheduler), token(std::move(token)) {
}

ProducerToken::~ProducerToken() {
}

TaskScheduler::TaskScheduler(DatabaseInstance &db)
    : db(db), queue(make_uniq<ConcurrentQueue>()),
      allocator_flush_threshold(db.config.options.allocator_flush_threshold),
      allocator_background_threads(Settings::Get<AllocatorBackgroundThreadsSetting>(db)), requested_thread_count(0),
      current_thread_count(1) {
	SetAllocatorBackgroundThreads(allocator_background_threads);
}

TaskScheduler::~TaskScheduler() {
#ifndef DUCKDB_NO_THREADS
	try {
		RelaunchThreadsInternal(0, true);
	} catch (...) {
		// nothing we can do in the destructor if this fails
	}
#endif
}

TaskScheduler &TaskScheduler::GetScheduler(ClientContext &context) {
	return TaskScheduler::GetScheduler(DatabaseInstance::GetDatabase(context));
}

TaskScheduler &TaskScheduler::GetScheduler(DatabaseInstance &db) {
	return db.GetScheduler();
}

unique_ptr<ProducerToken> TaskScheduler::CreateProducer() {
	auto token = make_uniq<QueueProducerToken>(*queue);
	return make_uniq<ProducerToken>(*this, std::move(token));
}

void TaskScheduler::ScheduleTask(ProducerToken &token, shared_ptr<Task> task) {
	// Enqueue a task for the given producer token and signal any sleeping threads
	queue->Enqueue(token, std::move(task));
}

void TaskScheduler::ScheduleTasks(ProducerToken &producer, vector<shared_ptr<Task>> &tasks) {
	queue->EnqueueBulk(producer, tasks);
}

bool TaskScheduler::GetTaskFromProducer(ProducerToken &token, shared_ptr<Task> &task) {
	return queue->DequeueFromProducer(token, task);
}

void TaskScheduler::ExecuteForever(atomic<bool> *marker) {
#ifndef DUCKDB_NO_THREADS
	static constexpr const int64_t INITIAL_FLUSH_WAIT = 500000; // initial wait time of 0.5s (in mus) before flushing

	const auto &block_allocator = BlockAllocator::Get(db);
	const auto &config = DBConfig::GetConfig(db);

	shared_ptr<Task> task;
	// loop until the marker is set to false
	while (*marker) {
		if (!block_allocator.SupportsFlush()) {
			// allocator can't flush, just start an untimed wait
			queue->semaphore.wait();
		} else if (!queue->semaphore.wait(INITIAL_FLUSH_WAIT)) {
			// allocator can flush, we flush this threads outstanding allocations after it was idle for 0.5s
			block_allocator.ThreadFlush(allocator_background_threads, allocator_flush_threshold,
			                            NumericCast<idx_t>(requested_thread_count.load()));
			auto decay_delay = Allocator::DecayDelay();
			if (!decay_delay.IsValid()) {
				// no decay delay specified - just wait
				queue->semaphore.wait();
			} else {
				if (!queue->semaphore.wait(UnsafeNumericCast<int64_t>(decay_delay.GetIndex()) * 1000000 -
				                           INITIAL_FLUSH_WAIT)) {
					// in total, the thread was idle for the entire decay delay (note: seconds converted to mus)
					// mark it as idle and start an untimed wait
					Allocator::ThreadIdle();
					queue->semaphore.wait();
				}
			}
		}
		if (queue->Dequeue(task)) {
			auto process_mode = TaskExecutionMode::PROCESS_ALL;
			if (Settings::Get<SchedulerProcessPartialSetting>(config)) {
				process_mode = TaskExecutionMode::PROCESS_PARTIAL;
			}
			auto execute_result = task->Execute(process_mode);

			switch (execute_result) {
			case TaskExecutionResult::TASK_FINISHED: {
				// Update morsel count; may demote query to lower priority queue
				uint64_t qid = task->query_id;
				task.reset();
				if (qid != 0) {
					queue->NotifyTaskComplete(qid);
				}
				break;
			}
			case TaskExecutionResult::TASK_ERROR:
				task.reset();
				break;
			case TaskExecutionResult::TASK_NOT_FINISHED: {
				// Partial morsel done — update count (demotion applies to re-enqueue)
				uint64_t qid = task->query_id;
				if (qid != 0) {
					queue->NotifyTaskComplete(qid);
				}
				auto &token = *task->token;
				queue->Enqueue(token, std::move(task));
				break;
			}
			case TaskExecutionResult::TASK_BLOCKED:
				task->Deschedule();
				task.reset();
				break;
			}
		} else if (queue->GetTasksInQueue() > 0) {
			// failed to dequeue but there are still tasks remaining - signal again to retry
			queue->semaphore.signal(1);
		}
	}
	// this thread will exit, flush all of its outstanding allocations
	if (block_allocator.SupportsFlush()) {
		block_allocator.ThreadFlush(allocator_background_threads, 0, NumericCast<idx_t>(requested_thread_count.load()));
		Allocator::ThreadIdle();
	}
#else
	throw NotImplementedException("DuckDB was compiled without threads! Background thread loop is not allowed.");
#endif
}

idx_t TaskScheduler::ExecuteTasks(atomic<bool> *marker, idx_t max_tasks) {
#ifndef DUCKDB_NO_THREADS
	idx_t completed_tasks = 0;
	// loop until the marker is set to false
	while (*marker && completed_tasks < max_tasks) {
		shared_ptr<Task> task;
		if (!queue->Dequeue(task)) {
			return completed_tasks;
		}
		auto execute_result = task->Execute(TaskExecutionMode::PROCESS_ALL);

		switch (execute_result) {
		case TaskExecutionResult::TASK_FINISHED:
		case TaskExecutionResult::TASK_ERROR:
			task.reset();
			completed_tasks++;
			break;
		case TaskExecutionResult::TASK_NOT_FINISHED:
			throw InternalException("Task should not return TASK_NOT_FINISHED in PROCESS_ALL mode");
		case TaskExecutionResult::TASK_BLOCKED:
			task->Deschedule();
			task.reset();
			break;
		}
	}
	return completed_tasks;
#else
	throw NotImplementedException("DuckDB was compiled without threads! Background thread loop is not allowed.");
#endif
}

void TaskScheduler::ExecuteTasks(idx_t max_tasks) {
#ifndef DUCKDB_NO_THREADS
	shared_ptr<Task> task;
	for (idx_t i = 0; i < max_tasks; i++) {
		queue->semaphore.wait(TASK_TIMEOUT_USECS);
		if (!queue->Dequeue(task)) {
			return;
		}
		try {
			auto execute_result = task->Execute(TaskExecutionMode::PROCESS_ALL);
			switch (execute_result) {
			case TaskExecutionResult::TASK_FINISHED:
			case TaskExecutionResult::TASK_ERROR:
				task.reset();
				break;
			case TaskExecutionResult::TASK_NOT_FINISHED:
				throw InternalException("Task should not return TASK_NOT_FINISHED in PROCESS_ALL mode");
			case TaskExecutionResult::TASK_BLOCKED:
				task->Deschedule();
				task.reset();
				break;
			}
		} catch (...) {
			return;
		}
	}
#else
	throw NotImplementedException("DuckDB was compiled without threads! Background thread loop is not allowed.");
#endif
}

#ifndef DUCKDB_NO_THREADS
static void ThreadExecuteTasks(TaskScheduler *scheduler, atomic<bool> *marker) {
	scheduler->ExecuteForever(marker);
}
#endif

int32_t TaskScheduler::NumberOfThreads() {
	return current_thread_count.load();
}

idx_t TaskScheduler::GetNumberOfTasks() const {
	return queue->GetTasksInQueue();
}

idx_t TaskScheduler::GetProducerCount() const {
	return queue->GetProducerCount();
}

idx_t TaskScheduler::GetTaskCountForProducer(ProducerToken &token) const {
	return queue->GetTaskCountForProducer(token);
}

void TaskScheduler::SetThreads(idx_t total_threads, idx_t external_threads) {
	if (total_threads == 0) {
		throw SyntaxException("Number of threads must be positive!");
	}
#ifndef DUCKDB_NO_THREADS
	if (total_threads < external_threads) {
		throw SyntaxException("Number of threads can't be smaller than number of external threads!");
	}
#else
	if (total_threads != external_threads) {
		throw NotImplementedException(
		    "DuckDB was compiled without threads! Setting total_threads != external_threads is not allowed.");
	}
#endif
	requested_thread_count = NumericCast<int32_t>(total_threads - external_threads);
}

void TaskScheduler::SetAllocatorFlushTreshold(idx_t threshold) {
	allocator_flush_threshold = threshold;
}

void TaskScheduler::SetAllocatorBackgroundThreads(bool enable) {
	allocator_background_threads = enable;
	Allocator::SetBackgroundThreads(enable);
}

void TaskScheduler::Signal(idx_t n) {
#ifndef DUCKDB_NO_THREADS
	typedef std::make_signed<std::size_t>::type ssize_t;
	queue->semaphore.signal(NumericCast<ssize_t>(n));
#endif
}

void TaskScheduler::YieldThread() {
#ifndef DUCKDB_NO_THREADS
	std::this_thread::yield();
#endif
}

idx_t TaskScheduler::GetEstimatedCPUId() {
#if defined(__EMSCRIPTEN__)
	// FIXME: Wasm + multithreads can likely be implemented as
	//   return return (idx_t)std::hash<std::thread::id>()(std::this_thread::get_id());
	return 0;
#else
	// this code comes from jemalloc
#if defined(_WIN32)
	return (idx_t)GetCurrentProcessorNumber();
#elif defined(_GNU_SOURCE)
	auto cpu = sched_getcpu();
	if (cpu < 0) {
#ifndef DUCKDB_NO_THREADS
		// fallback to thread id
		return (idx_t)std::hash<std::thread::id>()(std::this_thread::get_id());
#else

		return 0;
#endif
	}
	return (idx_t)cpu;
#elif defined(__aarch64__) && defined(__APPLE__)
	/* Other oses most likely use tpidr_el0 instead */
	uintptr_t c;
	asm volatile("mrs %x0, tpidrro_el0" : "=r"(c)::"memory");
	return (idx_t)(c & ((1 << 3) - 1));
#else
#ifndef DUCKDB_NO_THREADS
	// fallback to thread id
	return (idx_t)std::hash<std::thread::id>()(std::this_thread::get_id());
#else
	return 0;
#endif
#endif
#endif
}

void TaskScheduler::RelaunchThreads() {
	lock_guard<mutex> t(thread_lock);
	auto n = requested_thread_count.load();
	RelaunchThreadsInternal(n, false);
}

#ifndef DUCKDB_NO_THREADS
static vector<int> GetProcessCPUMask() {
#if defined(__GLIBC__)
	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);
	if (sched_getaffinity(0, sizeof(cpu_set_t), &cpuset) != 0) {
		return {};
	}
	vector<int> available_cpus;
	for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
		if (CPU_ISSET(cpu, &cpuset)) {
			available_cpus.push_back(cpu);
		}
	}
	return available_cpus;
#else
	return {};
#endif
}

static void SetThreadAffinity(thread &thread, const vector<int> &available_cpus, idx_t thread_idx) {
#if defined(__GLIBC__)
	if (thread_idx < available_cpus.size()) {
		const auto cpu_id = available_cpus[thread_idx];
		cpu_set_t cpuset;
		CPU_ZERO(&cpuset);
		CPU_SET(cpu_id, &cpuset);

		// note that we don't care about the return value here
		// if we did not manage to set affinity, the thread just does not have affinity, which is OK
		pthread_setaffinity_np(thread.native_handle(), sizeof(cpu_set_t), &cpuset);
	}
#endif
}
#endif

void TaskScheduler::RelaunchThreadsInternal(int32_t n, bool destroy) {
#ifndef DUCKDB_NO_THREADS
	auto &config = DBConfig::GetConfig(db);
	auto new_thread_count = NumericCast<idx_t>(n);

	idx_t external_threads = 0;
	ThreadPinMode pin_thread_mode = ThreadPinMode::AUTO;
	if (!destroy) {
		// If we are destroying, i.e., calling ~TaskScheduler, we don't want to read the settings
		external_threads = Settings::Get<ExternalThreadsSetting>(config);
		pin_thread_mode = Settings::Get<PinThreadsSetting>(db);
	}

	if (threads.size() == new_thread_count) {
		current_thread_count = NumericCast<int32_t>(threads.size() + external_threads);
		return;
	}
	if (threads.size() != new_thread_count) {
		// we are changing the number of threads: clear all threads first
		// we do this even when increasing the number of threads to make sure that all threads follow the current
		// affinity mask
		for (idx_t i = 0; i < threads.size(); i++) {
			*markers[i] = false;
		}
		Signal(threads.size());
		// now join the threads to ensure they are fully stopped before erasing them
		for (idx_t i = 0; i < threads.size(); i++) {
			threads[i]->internal_thread->join();
		}
		// erase the threads/markers
		threads.clear();
		markers.clear();
	}
	if (threads.size() < new_thread_count) {
		// we are increasing the number of threads: launch them and run tasks on them
		idx_t create_new_threads = new_thread_count - threads.size();

		// Whether to pin threads to cores
		static constexpr idx_t THREAD_PIN_THRESHOLD = 64;
		const auto pin_threads =
		    pin_thread_mode == ThreadPinMode::ON ||
		    (pin_thread_mode == ThreadPinMode::AUTO && std::thread::hardware_concurrency() > THREAD_PIN_THRESHOLD);
		const auto available_cpus = pin_threads ? GetProcessCPUMask() : vector<int>();
		// If we have fewer available cores than threads, do not pin and let OS scheduler handle it
		const auto can_pin = pin_threads && new_thread_count <= available_cpus.size();
		for (idx_t i = 0; i < create_new_threads; i++) {
			// launch a thread and assign it a cancellation marker
			auto marker = unique_ptr<atomic<bool>>(new atomic<bool>(true));
			unique_ptr<thread> worker_thread;
			try {
				worker_thread = make_uniq<thread>(ThreadExecuteTasks, this, marker.get());
				if (can_pin) {
					SetThreadAffinity(*worker_thread, available_cpus, threads.size());
				}
			} catch (std::exception &ex) {
				// thread constructor failed - this can happen when the system has too many threads allocated
				// in this case we cannot allocate more threads - stop launching them
				break;
			}
			auto thread_wrapper = make_uniq<SchedulerThread>(std::move(worker_thread));

			threads.push_back(std::move(thread_wrapper));
			markers.push_back(std::move(marker));
		}
	}
	current_thread_count = NumericCast<int32_t>(threads.size() + external_threads);
	BlockAllocator::Get(db).FlushAll();
#endif
}

} // namespace duckdb
