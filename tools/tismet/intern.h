// Copyright Glen Knowles 2015 - 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// intern.h - tismet
#pragma once

// Performance counters
void tsPerfInitialize();

// Data
void tsDataInitialize();
void tsDataBackup(IDbProgressNotify * notify);
DbHandle tsDataHandle();
DbContextHandle tsDataOpenContext();

// Returns false if the metric is not being stored
bool tsDataInsertMetric(
    uint32_t * id,
    DbHandle h,
    std::string_view name
);

// Backup
void tsBackupInitialize();
void tsBackupStart();

// Carbon
void tsCarbonInitialize();

// Graphite Web API
void tsGraphiteInitialize();
