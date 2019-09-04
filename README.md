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
- Collection protocols
  - Graphite plaintext protocol
- Query protocols
  - Alternative storage provider for Graphite (Takes the place of both whisper
    and carbon-cache).
  - Subset of Graphite Render API (Works as a standalone backend for Grafana).

#### Limitations
- Requires HTTP/2, does not work with HTTP/1.x
- When used as alternative storage provider
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
The Tismet server can run from the command line or as a service. The easiest
way to install it as a service is:
~~~ batch
tismet install
~~~

When run, Tismet accesses directories relative to the executable:
  - tismet.exe
  - conf/
  - crash/
  - data/
  - data/backup/
  - log/


## Configuring
Because Graphite doesn't understand HTTP/2 the simplest thing is to have it go
though a reverse proxy to talk to Tismet. I use Apache, but anything that can
proxy http clients to http/2 servers should work. Notably IIS and Nginx do not,
at this time, support http/2 connections to back-end servers.

For https you must have a certificate installed into the Windows certificate
store and configure Tismet to use it. You tell Tismet which cert to use by
putting the Subject Key Identifier (which should be a long hex string) of the
cert in the configuration file.

Https isn't required, but it can make it easier for some proxies to detect that
Tismet wants HTTP/2.

### Certificate
One way to setup a certificate on Windows is outlined below.

1. Generate a signing certificate that is used to sign server authentication
certificates. The subject name doesn't have to be "Local Test Root", it could
be anything. Make a note of the resulting Thumbprint, it will be used in the
next step.
~~~ powershell
powershell New-SelfSignedCertificate
    -CertStoreLocation cert:\CurrentUser\My
    -Subject "Local Test Root"
    -Type Custom
    -KeyUsage CertSign
~~~

2. Generate a signed certificate for the domain, using the signing certificate.
The friendly name is optional. Repeat this step as often as you like for as
many domains as you need.
~~~ powershell
powershell New-SelfSignedCertificate
    -CertStoreLocation cert:\CurrentUser\My
    -DnsName example.com, example.net
    -Signer cert:\CurrentUser\My\<Thumbprint of Local Test Root>
    -FriendlyName "Example Test Site"
~~~

3. Make "Local Test Root" trusted. Note that a copy of the certificate must be
left in Personal for step #2 to sign additional certificates in the future.
   1. Run "certmgr.msc", the "Manage user certificates" management snap-in.
   2. Copy "Local Test Root" from the Personal folder to Trusted Root
Certification Authorities.


## Configuring Graphite

Change Graphite's local_settings.py:
1. To use the RemoteFinder:
~~~ python
STORAGE_FINDERS = (
    'graphite.finders.remote.RemoteFinder',
)
~~~

2. And to reference Tismet (via the proxy) as a cluster server:
~~~ python
CLUSTER_SERVERS = ["proxy-to-tismet.example.com"]
~~~


## Configuring Grafana

Create a data source.
  - Set Type to 'Graphite'
  - Set URL under "HTTP settings" to the reverse proxy's address
  - Set Version under "Graphite details" to '1.1.x'


## Making backups

Use the tsm utility to trigger a backup and, optionally, wait for it to finish.
Run "tsm --help" for more information about using tsm.

~~~ batch
tsm backup <server address>
~~~

It's not safe to just copy the data/metrics.* files while the server is running,
the tsd must be copied before the tsl and even if you do that you can still end
up with a corrupt copy if an internal checkpoint was in progress.


## Other time series database projects
- [VictoriaMetrics](https://victoriametrics.com)
- [prometheus](https://prometheus.io)
- [Hawkular](https://www.hawkular.org)
- [graphite](https://grahite.readthedocs.io)
- [InfluxDb](https://influxdata.com)
- [akumuli](https://akumuli.org)
- [Amazon Timestream](https://aws.amazon.com/timestream)
- [Timescale](https://www.timescale.com)
