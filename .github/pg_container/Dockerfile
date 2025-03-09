ARG PG_VERSION
FROM docker.io/postgres:${PG_VERSION}-bookworm

RUN apt-get update \
      && apt-cache showpkg postgresql-$PG_MAJOR-postgis-$POSTGIS_MAJOR \
      && apt-get install -y --no-install-recommends \
           ca-certificates \
           postgresql-$PG_MAJOR-postgis-3 \
           postgresql-$PG_MAJOR-postgis-3-scripts \
           postgresql-$PG_MAJOR-pgvector \
           postgresql-$PG_MAJOR-age \
           postgresql-$PG_MAJOR-rational


# Other available Debian packages for PG extensions:
#    postgresql-16-age - Graph database optimized for fast analysis and real-time data processing
#    postgresql-16-asn1oid - ASN.1 OID data type for PostgreSQL
#    postgresql-16-auto-failover - Postgres high availability support
#    postgresql-16-bgw-replstatus - report whether PostgreSQL node is master or standby
#    postgresql-16-credcheck - PostgreSQL username/password checks
#    postgresql-16-cron - Run periodic jobs in PostgreSQL
#    postgresql-16-debversion - Debian version number type for PostgreSQL
#    postgresql-16-decoderbufs - logical decoder output plugin to deliver data as Protocol Buffers
#    postgresql-16-dirtyread - Read dead but unvacuumed tuples from a PostgreSQL relation
#    postgresql-16-extra-window-functions - Extra Window Functions for PostgreSQL
#    postgresql-16-first-last-agg - PostgreSQL extension providing first and last aggregate functions
#    postgresql-16-h3 - PostgreSQL bindings for H3, a hierarchical hexagonal geospatial indexing system
#    postgresql-16-hll - HyperLogLog extension for PostgreSQL
#    postgresql-16-http - HTTP client for PostgreSQL, retrieve a web page from inside the database
#    postgresql-16-hypopg - PostgreSQL extension adding support for hypothetical indexes.
#    postgresql-16-icu-ext - PostgreSQL extension exposing functionality from the ICU library
#    postgresql-16-ip4r - IPv4 and IPv6 types for PostgreSQL 16
#    postgresql-16-jsquery - PostgreSQL JSON query language with GIN indexing support
#    postgresql-16-londiste-sql - SQL infrastructure for Londiste
#    postgresql-16-mimeo - specialized, per-table replication between PostgreSQL instances
#    postgresql-16-mobilitydb - Open source geospatial trajectory data management & analysis platform
#    postgresql-16-mysql-fdw - Postgres 16 Foreign Data Wrapper for MySQL
#    postgresql-16-numeral - numeral datatypes for PostgreSQL
#    postgresql-16-ogr-fdw - PostgreSQL foreign data wrapper for OGR
#    postgresql-16-omnidb - PostgreSQL PL/pgSQL debugger extension for OmniDB
#    postgresql-16-oracle-fdw - PostgreSQL Foreign Data Wrapper for Oracle
#    postgresql-16-orafce - Oracle support functions for PostgreSQL 16
#    postgresql-16-partman - PostgreSQL Partition Manager
#    postgresql-16-periods - PERIODs and SYSTEM VERSIONING for PostgreSQL
#    postgresql-16-pg-catcheck - Postgres system catalog checker
#    postgresql-16-pg-checksums - Activate/deactivate/verify PostgreSQL data checksums
#    postgresql-16-pg-fact-loader - Build fact tables asynchronously with Postgres
#    postgresql-16-pg-failover-slots - High-availability support for PostgreSQL logical replication
#    postgresql-16-pg-hint-plan - support for optimizer hints in PostgreSQL
#    postgresql-16-pg-qualstats - PostgreSQL extension to gather statistics about predicates.
#    postgresql-16-pg-rrule - RRULE data type for PostgreSQL
#    postgresql-16-pg-stat-kcache - PostgreSQL extension to gather per-query kernel statistics.
#    postgresql-16-pg-track-settings - PostgreSQL extension tracking of configuration settings
#    postgresql-16-pg-wait-sampling - Extension providing statistics about PostgreSQL wait events
#    postgresql-16-pgaudit - PostgreSQL Audit Extension
#    postgresql-16-pgauditlogtofile - PostgreSQL pgAudit Add-On to redirect audit logs
#    postgresql-16-pgextwlist - PostgreSQL Extension Whitelisting
#    postgresql-16-pgfaceting - Faceted query acceleration for PostgreSQL using roaring bitmaps
#    postgresql-16-pgfincore - set of PostgreSQL functions to manage blocks in memory
#    postgresql-16-pgl-ddl-deploy - Transparent DDL replication for PostgreSQL
#    postgresql-16-pglogical - Logical Replication Extension for PostgreSQL
#    postgresql-16-pglogical-ticker - Have time-based replication delay for pglogical
#    postgresql-16-pgmemcache - PostgreSQL interface to memcached
#    postgresql-16-pgmp - arbitrary precision integers and rationals for PostgreSQL 16
#    postgresql-16-pgpcre - Perl Compatible Regular Expressions (PCRE) extension for PostgreSQL
#    postgresql-16-pgpool2 - connection pool server and replication proxy for PostgreSQL - modules
#    postgresql-16-pgq-node - Cascaded queueing on top of PgQ
#    postgresql-16-pgq3 - Generic queue for PostgreSQL
#    postgresql-16-pgrouting - Routing functionality support for PostgreSQL/PostGIS
#    postgresql-16-pgrouting-doc - Routing functionality support for PostgreSQL/PostGIS (Documentation)
#    postgresql-16-pgrouting-scripts - Routing functionality support for PostgreSQL/PostGIS - SQL scripts
#    postgresql-16-pgsphere - Spherical data types for PostgreSQL
#    postgresql-16-pgtap - Unit testing framework extension for PostgreSQL 16
#    postgresql-16-pgvector - Open-source vector similarity search for Postgres
#    postgresql-16-pldebugger - PostgreSQL pl/pgsql Debugger API
#    postgresql-16-pljava - Java procedural language for PostgreSQL 16
#    postgresql-16-pllua - Lua procedural language for PostgreSQL 16
#    postgresql-16-plpgsql-check - plpgsql_check extension for PostgreSQL
#    postgresql-16-plprofiler - PostgreSQL PL/pgSQL functions performance profiler
#    postgresql-16-plproxy - database partitioning system for PostgreSQL 16
#    postgresql-16-plr - Procedural language interface between PostgreSQL and R
#    postgresql-16-plsh - PL/sh procedural language for PostgreSQL 16
#    postgresql-16-pointcloud - PostgreSQL extension for storing point cloud (LIDAR) data
#    postgresql-16-postgis-3 - Geographic objects support for PostgreSQL 16
#    postgresql-16-postgis-3-scripts - Geographic objects support for PostgreSQL 16 -- SQL scripts
#    postgresql-16-powa - PostgreSQL Workload Analyzer -- PostgreSQL 16 extension
#    postgresql-16-prefix - Prefix Range module for PostgreSQL
#    postgresql-16-preprepare - pre prepare your PostgreSQL statements server side
#    postgresql-16-prioritize - Get and set the nice priorities of PostgreSQL backends
#    postgresql-16-q3c - PostgreSQL 16 extension used for indexing the sky
#    postgresql-16-rational - Precise fractional arithmetic for PostgreSQL
#    postgresql-16-rdkit - Cheminformatics and machine-learning software (PostgreSQL Cartridge)
#    postgresql-16-repack - reorganize tables in PostgreSQL databases with minimal locks
#    postgresql-16-repmgr - replication manager for PostgreSQL 16
#    postgresql-16-roaringbitmap - RoaringBitmap extension for PostgreSQL
#    postgresql-16-rum - PostgreSQL RUM access method
#    postgresql-16-semver - Semantic version number type for PostgreSQL
#    postgresql-16-set-user - PostgreSQL privilege escalation with enhanced logging and control
#    postgresql-16-show-plans - Show query plans of currently running PostgreSQL statements
#    postgresql-16-similarity - PostgreSQL similarity functions extension
#    postgresql-16-slony1-2 - replication system for PostgreSQL: PostgreSQL 16 server plug-in
#    postgresql-16-snakeoil - PostgreSQL anti-virus scanner based on ClamAV
#    postgresql-16-squeeze - PostgreSQL extension for automatic bloat cleanup
#    postgresql-16-statviz - PostgreSQL internal statistics analysis and visualization
#    postgresql-16-tablelog - log changes on tables and restore tables to point in time
#    postgresql-16-tdigest - t-digest algorithm for on-line accumulation of rank-based statistics
#    postgresql-16-tds-fdw - PostgreSQL foreign data wrapper for TDS databases
#    postgresql-16-toastinfo - Show storage structure of varlena datatypes in PostgreSQL
#    postgresql-16-unit - SI Units for PostgreSQL
#    postgresql-16-wal2json - PostgreSQL logical decoding JSON output plugin
