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

    void onResult(ResultInfo const & info);
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
    struct SourceContext : FuncContext {
        ResultNode * rn{};
    };

public:
    virtual ~SourceNode();
    void init(std::shared_ptr<char[]> name);

    void addOutput(SourceContext const & context);
    void removeOutput(ResultNode * rn);

    std::shared_ptr<char[]> sourceName() const { return m_source; }

protected:
    // Sets first, last, pretime, and presamples
    // Returns false if outputs and pendingOutputs are empty
    bool outputContext(SourceContext * context);

    void outputResult(ResultInfo const & info);

private:
    void outputResultImpl_LK(ResultInfo const & info);
    virtual void onSourceStart() = 0;

    std::shared_ptr<char[]> m_source;

    mutable std::mutex m_outMut;
    std::vector<SourceContext> m_outputs;
    std::vector<SourceContext> m_pendingOutputs;
};

class FuncNode : public ResultNode, public SourceNode, public IFuncNotify {
public:
    void init(
        std::shared_ptr<char[]> sourceName,
        std::unique_ptr<IFuncInstance> instance
    );
    bool bind(std::vector<Query::Node const *> & args);

protected:
    void onSourceStart() override;
    void onTask() override;
    bool onFuncSource(Query::Node const & node) override;
    void onFuncOutput(ResultInfo & info) override;

    std::unique_ptr<IFuncInstance> m_instance;
};

} // namespace
