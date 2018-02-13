// Copyright Glen Knowles 2018.
// Distributed under the Boost Software License, Version 1.0.
//
// intern.h - tismet eval


/****************************************************************************
*
*   Declarations
*
***/

namespace Eval {

class SourceNode;

struct SampleList {
    Dim::TimePoint first;
    Dim::Duration interval;
    uint32_t count{0};
    uint32_t metricId{0};

    // EXTENDS BEYOND END OF STRUCT
    double samples[1];

    static std::shared_ptr<SampleList> alloc(
        Dim::TimePoint first,
        Dim::Duration interval,
        size_t count
    );
    static std::shared_ptr<SampleList> alloc(const SampleList & samples);
    static std::shared_ptr<SampleList> dup(const SampleList & samples);
};

struct ResultInfo {
    std::shared_ptr<char[]> target;
    std::shared_ptr<char[]> name;
    std::shared_ptr<SampleList> samples;
    bool more{false};
};

class ResultNode : public Dim::ITaskNotify {
public:
    enum class Apply {
        kForward,
        kSkip,
        kFinished,
        kDestroy
    };

public:
    virtual ~ResultNode();

    virtual void onResult(int resultId, const ResultInfo & info);
    void onTask() override;

    int m_unfinished{0};
    std::vector<std::shared_ptr<SourceNode>> m_sources;

protected:
    virtual Apply onResultTask(ResultInfo & info);

    void stopSources();

    std::mutex m_resMut;
    std::deque<ResultInfo> m_results;
};

class SampleReader : public IDbDataNotify {
public:
    SampleReader(SourceNode * node);
    void read(
        std::shared_ptr<char[]> target,
        Dim::TimePoint first,
        Dim::TimePoint last
    );

private:
    void readMore();

    bool onDbSeriesStart(const DbSeriesInfo & info) override;
    bool onDbSample(uint32_t id, Dim::TimePoint time, double value) override;
    void onDbSeriesEnd(uint32_t id) override;

    SourceNode & m_node;
    std::thread::id m_tid;
    ResultInfo m_result;
    Dim::TimePoint m_first;
    Dim::TimePoint m_last;
    Dim::UnsignedSet m_unfinished;

    size_t m_pos{0};
    Dim::TimePoint m_time;
};

class SourceNode : public Dim::ITaskNotify {
public:
    struct SourceRange {
        Dim::TimePoint first;
        Dim::TimePoint last;

        Dim::TimePoint lastUsed;
    };
    struct ResultRange {
        ResultNode * rn{nullptr};
        int resultId{0};
        Dim::TimePoint first;
        Dim::TimePoint last;
        Dim::Duration minInterval;
    };

public:
    SourceNode();

    bool outputRange(Dim::TimePoint * first, Dim::TimePoint * last) const;

    // Returns false when !info.more
    bool outputResult(const ResultInfo & info);

    virtual void onStart();
    void onTask() override;

    std::shared_ptr<char[]> m_source;
    std::vector<SourceRange> m_ranges;

    mutable std::mutex m_outMut;
    std::vector<ResultRange> m_outputs;

private:
    SampleReader m_reader;
};

struct FuncArg {
    std::shared_ptr<char[]> string;
    double number;
};

class FuncNode : public ResultNode, public SourceNode {
public:
    ~FuncNode();

    void forwardResult(ResultInfo & info, bool unfinished = false);

    void onStart() override;
    Apply onResultTask(ResultInfo & info) override;

    // validate and/or process arguments
    virtual bool onFuncBind();
    virtual Apply onFuncApply(ResultInfo & info);

    Query::Function::Type m_type;
    std::vector<FuncArg> m_args;
};

void initializeFuncs();

void registerFunc(Query::Function::Type type, Dim::IFactory<FuncNode> * fact);

} // namespace
