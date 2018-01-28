<!--
Copyright Glen Knowles 2016 - 2018.
Distributed under the Boost Software License, Version 1.0.
-->

[![Build status](https://ci.appveyor.com/api/projects/status/nlcftmonh607sv3a?svg=true)
    ](https://ci.appveyor.com/project/gknowles/tismet "msvc 2017")

# Tismet

Time series metric collection

- Windows server
- Metric database with transactional write-ahead logging.
- Alternative storage provider for Graphite
  - Takes the place of both whisper and carbon-cache
- Collection protocols supported
  - Graphite plaintext protocol

#### Limitations
- Requires HTTP/2
- Graphite must be version 1.1 or later


## Building
#### Prerequisites
  - CMake >= 3.6
  - Visual Studio 2017 (should include "Git for Windows")

#### Steps
~~~ batch
git clone https://github.com/gknowles/tismet.git
cd tismet
git submodule update --init
configure.bat
msbuild /p:Configuration=Release /m
~~~
Instead of running msbuild you can open the tismet.sln solution file in
Visual Studio and build it there.

Binaries are placed in tismet/bin, and can be run from there for simple
testing.


## Running
Tismet can run from the command line or as a service. The easiest way to
install it as a service is:
  - sc create Tismet binPath= \<installPath\>\tismet.exe depend= NetworkProvider
    start= auto

When run, Tismet accesses directories relative to the executable:
  - tismet.exe
  - conf/
  - data/
  - data/backup/


## Configuring
Because Graphite doesn't understand HTTP/2 the simplest thing is to have it go
though a proxy to talk to Tismet. I use nghttpx because it's simple, but
anything that can proxy http clients to http/2 servers should work.

For https you must have a certificate installed into the Windows certificate
store and configure Tismet to use it. You tell Tismet which cert to use by
putting the Subject Key Identifier (which should be a long hex string) of the
cert in the configuration file.

Https isn't required, but it can make it easier for some proxies to detect that
Tismet wants HTTP/2.

### Certificate
One simple way to make a cert is outlined below, although since it uses the
Test Root cert (which is known to all those folks on the internet) it's not
exactly safe:
1. Generate the cert signed by the windows test certificate "CertReq Test Root"
(Friendly name for your new cert is optional)
~~~ powershell
powershell New-SelfSignedCertificate
        -CertStoreLocation cert:\CurrentUser\MY
        -DnsName MyComputerName
        -TestRoot
        -FriendlyName "Example Test Site"
~~~

2. Make "CertReq Test Root" trusted - it was created by the previous step if
it didn't already exist.

   1. Run "certmgr.msc", the "Manage user certificates" management snap-in.
   2. Copy/move "CertReq Test Root" from Intermediate Certification Authorities
to Trusted Root Certification Authorities.


## Configuring Graphite

Change Graphite's local_settings.py:
1. to use the RemoteFinder:
~~~ python
STORAGE_FINDERS = (
    'graphite.finders.remote.RemoteFinder',
)
~~~

2. and reference Tismet (via the proxy) as cluster servers:
~~~ python
CLUSTER_SERVERS = ["proxy-to-tismet.example.com"]
~~~


## Making backups

Use the tsm utility to trigger a backup and, optionally, wait for it to finish.
Run "tsm help" for more information about using tsm.

~~~ batch
tsm backup <server address>
~~~

It's not safe to just copy the data/metrics.* files while the server is running,
the tsd must be copied before the tsl and even if you do that you can still end
up with a corrupt copy if an internal checkpoint was in progress.
