#pragma once

#include<condition_variable>
#include<functional>
#include<future>
#include<mutex>
#include<queue>
#include<memory>
#include<stdexcept>
#include<thread>
#include<type_traits>
#include<utility>
#include<vector>

class ThreadPool
{
private:
    std::vector<std::thread>work;
    std::queue<std::function<void()>>q;
    std::mutex m;
    std::condition_variable cv;
    bool stop{false};

public:
    explicit ThreadPool(std::size_t count)
    {
        if(count==0)count=1;
        for(std::size_t i=0;i<count;i++)
        {
            work.emplace_back([this]
            {
                for(;;)
                {
                    std::function<void()>task;
                    {
                        std::unique_lock<std::mutex>lock(m);
                        cv.wait(lock,[this]{return stop||!q.empty();});
                        if(stop&&q.empty())return;
                        task=std::move(q.front());
                        q.pop();
                    }
                    task();
                }
            });
        }
    }

    ~ThreadPool()
    {
        {
            std::lock_guard<std::mutex>lock(m);
            stop=true;
        }
        cv.notify_all();
        for(auto &thread:work)
        {
            if(thread.joinable())
            {thread.join();}
        }
    }

    template<typename F,typename...Args>
    auto add(F &&func,Args &&...args)->std::future<std::invoke_result_t<F,Args...>>
    {
        using ReturnType=std::invoke_result_t<F,Args...>;
        auto task=std::make_shared<std::packaged_task<ReturnType()>>
                 (std::bind(std::forward<F>(func),std::forward<Args>(args)...));
        auto result=task->get_future();
        {
            std::lock_guard<std::mutex>lock(m);
            if(stop){throw std::runtime_error("thread pool is stopped");}
            q.emplace([task]{(*task)();});
        }
        cv.notify_one();
        return result;
    }
};