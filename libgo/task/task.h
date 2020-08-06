#pragma once
#include "../common/config.h"
#include "../common/ts_queue.h"
#include "../common/anys.h"
#include "../context/context.h"
#include "../debug/debugger.h"

namespace co
{

enum class TaskState
{
    runnable,
    block,
    done,
};

const char* GetTaskStateName(TaskState state);

typedef std::function<void()> TaskF;

struct TaskGroupKey {};
typedef Anys<TaskGroupKey> TaskAnys;

class IProcesser;

struct ITask {
    uint64_t id_;
    TaskAnys anys_;
    Context ctx_;
    uint64_t yieldCount_ = 0;


    ITask(uint64_t id, fn_t fn, intptr_t vp, std::size_t stackSize) : id_(id), ctx_(fn, vp, stackSize) {
    }

    uint64_t getId() {
        return id_;
    }
    ALWAYS_INLINE void SwapIn()
    {
        ctx_.SwapIn();
    }
    //ALWAYS_INLINE void SwapTo(Task* other)
    //{
    //    ctx_.SwapTo(other->ctx_);
    //}
    ALWAYS_INLINE void SwapOut()
    {
        ctx_.SwapOut();
    }
    virtual const char* DebugInfo();
    TaskState state_ = TaskState::runnable;


};

struct Task : public ITask, public TSQueueHook, public SharedRefObject, public CoDebugger::DebuggerBase<Task>
{
    IProcesser* proc_ = nullptr;
    TaskF fn_;
    std::exception_ptr eptr_;           // 保存exception的指针

    atomic_t<uint64_t> suspendId_ {0};

    Task(TaskF const& fn, std::size_t stack_size);
    ~Task();
    const char* DebugInfo() override;


private:
    void Run();

    static void FCONTEXT_CALL StaticRun(intptr_t vp);

    Task(Task const&) = delete;
    Task(Task &&) = delete;
    Task& operator=(Task const&) = delete;
    Task& operator=(Task &&) = delete;
};

#define TaskInitPtr reinterpret_cast<ITask*>(0x1)
#define TaskRefDefine(type, name) \
    ALWAYS_INLINE type& TaskRef ## name(ITask *tk) \
    { \
        typedef type T; \
        static int idx = -1; \
        if (UNLIKELY(tk == TaskInitPtr)) { \
            if (idx == -1) \
                idx = TaskAnys::Register<T>(); \
            static T ignore{}; \
            return ignore; \
        } \
        return tk->anys_.get<T>(idx); \
    }
#define TaskRefInit(name) do { TaskRef ## name(TaskInitPtr); } while(0)

} //namespace co
