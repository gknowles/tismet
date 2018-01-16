// Copyright Glen Knowles 2015 - 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// intern.h - tismet
#pragma once

// Carbon
void tsCarbonInitialize();

// Data
void tsDataInitialize();
DbHandle tsDataHandle();
bool tsDataInsertMetric(uint32_t * id, std::string_view name);
void tsDataUpdateMetric(uint32_t id, const MetricInfo & info);
void tsDataUpdateSample(uint32_t id, Dim::TimePoint time, double value);

// Graphite
void tsGraphiteInitialize();

// Performance counters
void tsPerfInitialize();
