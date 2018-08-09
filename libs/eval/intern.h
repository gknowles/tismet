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

class ResultNode
    : public Dim::RefCount
    , public Dim::ITaskNotify
{
public:
    enum class Apply {
        kForward,
        kSkip,
        kFinished,
        kDestroy
    };

public:
    virtual ~ResultNode();

    void onResult(const ResultInfo & info);
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
        Dim::Duration minInterval{};
        Dim::TimePoint first;
        Dim::TimePoint last;

        // "pre" is a request for samples from before the start of the result
        // range that are needed to make the first values meaningful. These are
        // requested by functions such as movingAverage and derivative.
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
    };
    OutputResultReturn outputResult(const ResultInfo & info);

private:
    void outputResultImpl(const ResultInfo & info);
    virtual void onSourceStart() = 0;

    std::shared_ptr<char[]> m_source;

    mutable std::mutex m_outMut;
    std::vector<ResultRange> m_outputs;
    std::vector<ResultRange> m_pendingOutputs;
};

class FuncNode : public ResultNode, public SourceNode, public IFuncNotify {
public:
    void init(
        std::shared_ptr<char[]> sourceName,
        std::unique_ptr<IFuncInstance> instance
    );
    bool bind(std::vector<FuncArg> && args);

protected:
    void onSourceStart() override;
    void onTask() override;
    void onFuncOutput(ResultInfo & info) override;

    std::unique_ptr<IFuncInstance> m_instance;
};

} // namespace
