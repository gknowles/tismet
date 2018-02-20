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
    Dim::Duration interval{};
    unsigned count{0};
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
    void onTask() override = 0;

    int m_unfinished{0};
    std::vector<std::shared_ptr<SourceNode>> m_sources;

protected:
    void stopSources();

    std::mutex m_resMut;
    std::deque<ResultInfo> m_results;
};

class SourceNode {
public:
    struct ResultRange {
        ResultNode * rn{};
        int resultId{};
        Dim::Duration minInterval{};
        Dim::TimePoint first;
        Dim::TimePoint last;

        // "pre" is a request for samples from before the start of the result
        // range that are needed to make it consistent. These are requested by
        // functions such as movingAverage and derivative.
        Dim::Duration pretime{};
        unsigned presamples{0};
    };

public:
    virtual ~SourceNode();
    void init(std::shared_ptr<char[]> name);

    void addOutput(const ResultRange & rr);
    void removeOutput(ResultNode * rn);

protected:
    std::shared_ptr<char[]> sourceName() const { return m_source; }

    // Sets first, last, pretime, and presamples
    // Returns false if outputs and pendingOutputs are empty
    bool outputRange(ResultRange * rr);

    struct OutputResultReturn {
        bool more;      // more series expected for this set of outputs
        bool pending;   // another set of outputs is pending
    } outputResult(const ResultInfo & info);

private:
    virtual void onSourceStart() = 0;

    std::shared_ptr<char[]> m_source;

    mutable std::mutex m_outMut;
    std::vector<ResultRange> m_outputs;
    std::vector<ResultRange> m_pendingOutputs;
};

struct FuncArg {
    std::shared_ptr<char[]> string;
    double number;
};

class FuncNode : public ResultNode, public SourceNode {
public:
    void init(std::shared_ptr<char[]> sourceName);
    bool bind(std::vector<FuncArg> && args);

protected:
    void onSourceStart() override;
    void onTask() override;

    // validate and/or process arguments
    virtual bool onFuncBind();
    virtual void onFuncAdjustRange(
        Dim::TimePoint * first,
        Dim::TimePoint * last,
        Dim::Duration * pretime,
        unsigned * presamples
    );

    // Perform the function, outputResult() must be called for each result
    // that should be published.
    //
    // Return false to stop receiving results for the current outputs, only
    // required if the function is to be aborted midstream.
    virtual bool onFuncApply(ResultInfo & info) = 0;

    virtual Query::Function::Type type() const = 0;

    std::vector<FuncArg> m_args;
};

void initializeFuncs();

void registerFunc(Query::Function::Type type, Dim::IFactory<FuncNode> * fact);

} // namespace
