// Copyright Glen Knowles 2015 - 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// intern.h - tismet
#pragma once

// Performance counters
void tsPerfInitialize();

// Data
void tsDataInitialize();
DbHandle tsDataHandle();
void tsDataBackup(IDbProgressNotify * notify);
bool tsDataInsertMetric(uint32_t * id, std::string_view name);
void tsDataUpdateMetric(uint32_t id, const MetricInfo & info);
void tsDataUpdateSample(uint32_t id, Dim::TimePoint time, double value);

// Backup
void tsBackupInitialize();
void tsBackupStart();

// Carbon
void tsCarbonInitialize();

// Graphite Web API
void tsGraphiteInitialize();
