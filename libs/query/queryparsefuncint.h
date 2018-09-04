// queryparsefuncint.h - tismet query
// Generated by genfuncs 1.0.0
// clang-format off
#pragma once


/****************************************************************************
*
*   Query functions
*
***/

//===========================================================================
inline bool QueryParser::onFnAbsoluteStart () {
    return startFunc(Eval::Function::kAbsolute);
}

//===========================================================================
inline bool QueryParser::onFnAggregateStart () {
    return startFunc(Eval::Function::kAggregate);
}

//===========================================================================
inline bool QueryParser::onFnAliasStart () {
    return startFunc(Eval::Function::kAlias);
}

//===========================================================================
inline bool QueryParser::onFnAliasSubStart () {
    return startFunc(Eval::Function::kAliasSub);
}

//===========================================================================
inline bool QueryParser::onFnAverageAboveStart () {
    return startFunc(Eval::Function::kAverageAbove);
}

//===========================================================================
inline bool QueryParser::onFnAverageBelowStart () {
    return startFunc(Eval::Function::kAverageBelow);
}

//===========================================================================
inline bool QueryParser::onFnAverageSeriesStart () {
    return startFunc(Eval::Function::kAverageSeries);
}

//===========================================================================
inline bool QueryParser::onFnColorStart () {
    return startFunc(Eval::Function::kColor);
}

//===========================================================================
inline bool QueryParser::onFnConsolidateByStart () {
    return startFunc(Eval::Function::kConsolidateBy);
}

//===========================================================================
inline bool QueryParser::onFnCountSeriesStart () {
    return startFunc(Eval::Function::kCountSeries);
}

//===========================================================================
inline bool QueryParser::onFnCurrentAboveStart () {
    return startFunc(Eval::Function::kCurrentAbove);
}

//===========================================================================
inline bool QueryParser::onFnCurrentBelowStart () {
    return startFunc(Eval::Function::kCurrentBelow);
}

//===========================================================================
inline bool QueryParser::onFnDerivativeStart () {
    return startFunc(Eval::Function::kDerivative);
}

//===========================================================================
inline bool QueryParser::onFnDiffSeriesStart () {
    return startFunc(Eval::Function::kDiffSeries);
}

//===========================================================================
inline bool QueryParser::onFnDrawAsInfiniteStart () {
    return startFunc(Eval::Function::kDrawAsInfinite);
}

//===========================================================================
inline bool QueryParser::onFnFilterSeriesStart () {
    return startFunc(Eval::Function::kFilterSeries);
}

//===========================================================================
inline bool QueryParser::onFnFirstSeriesStart () {
    return startFunc(Eval::Function::kFirstSeries);
}

//===========================================================================
inline bool QueryParser::onFnHighestCurrentStart () {
    return startFunc(Eval::Function::kHighestCurrent);
}

//===========================================================================
inline bool QueryParser::onFnHighestMaxStart () {
    return startFunc(Eval::Function::kHighestMax);
}

//===========================================================================
inline bool QueryParser::onFnKeepLastValueStart () {
    return startFunc(Eval::Function::kKeepLastValue);
}

//===========================================================================
inline bool QueryParser::onFnLastSeriesStart () {
    return startFunc(Eval::Function::kLastSeries);
}

//===========================================================================
inline bool QueryParser::onFnLegendValueStart () {
    return startFunc(Eval::Function::kLegendValue);
}

//===========================================================================
inline bool QueryParser::onFnLineWidthStart () {
    return startFunc(Eval::Function::kLineWidth);
}

//===========================================================================
inline bool QueryParser::onFnMaxSeriesStart () {
    return startFunc(Eval::Function::kMaxSeries);
}

//===========================================================================
inline bool QueryParser::onFnMaximumAboveStart () {
    return startFunc(Eval::Function::kMaximumAbove);
}

//===========================================================================
inline bool QueryParser::onFnMaximumBelowStart () {
    return startFunc(Eval::Function::kMaximumBelow);
}

//===========================================================================
inline bool QueryParser::onFnMedianSeriesStart () {
    return startFunc(Eval::Function::kMedianSeries);
}

//===========================================================================
inline bool QueryParser::onFnMinSeriesStart () {
    return startFunc(Eval::Function::kMinSeries);
}

//===========================================================================
inline bool QueryParser::onFnMinimumAboveStart () {
    return startFunc(Eval::Function::kMinimumAbove);
}

//===========================================================================
inline bool QueryParser::onFnMinimumBelowStart () {
    return startFunc(Eval::Function::kMinimumBelow);
}

//===========================================================================
inline bool QueryParser::onFnMovingAverageStart () {
    return startFunc(Eval::Function::kMovingAverage);
}

//===========================================================================
inline bool QueryParser::onFnMultiplySeriesStart () {
    return startFunc(Eval::Function::kMultiplySeries);
}

//===========================================================================
inline bool QueryParser::onFnNonNegativeDerivativeStart () {
    return startFunc(Eval::Function::kNonNegativeDerivative);
}

//===========================================================================
inline bool QueryParser::onFnRangeSeriesStart () {
    return startFunc(Eval::Function::kRangeSeries);
}

//===========================================================================
inline bool QueryParser::onFnRemoveAboveValueStart () {
    return startFunc(Eval::Function::kRemoveAboveValue);
}

//===========================================================================
inline bool QueryParser::onFnRemoveBelowValueStart () {
    return startFunc(Eval::Function::kRemoveBelowValue);
}

//===========================================================================
inline bool QueryParser::onFnScaleStart () {
    return startFunc(Eval::Function::kScale);
}

//===========================================================================
inline bool QueryParser::onFnScaleToSecondsStart () {
    return startFunc(Eval::Function::kScaleToSeconds);
}

//===========================================================================
inline bool QueryParser::onFnStddevSeriesStart () {
    return startFunc(Eval::Function::kStddevSeries);
}

//===========================================================================
inline bool QueryParser::onFnSumSeriesStart () {
    return startFunc(Eval::Function::kSumSeries);
}

//===========================================================================
inline bool QueryParser::onFnTimeShiftStart () {
    return startFunc(Eval::Function::kTimeShift);
}
