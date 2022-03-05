// Copyright Glen Knowles 2015 - 2021.
// Distributed under the Boost Software License, Version 1.0.
//
// intern.h - tismet
#pragma once

// Application
std::string_view tsProductVersion();

// Performance counters
void tsPerfInitialize();

// Data
void tsDataInitialize();
void tsDataBackup(IDbProgressNotify * notify);
DbHandle tsDataHandle();
const Dim::Path & tsDataPath();

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

// Web API
void tsWebInitialize();
void tsGraphiteInitialize();

std::string_view resWebSiteContent();
