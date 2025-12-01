---
title: OpenRiak Riak Key-Value Store
layout: home
---

# OpenRiak Riak Key-Value Store

Riak is a distributed key-value store, designed to provide high-availability with predictable response times in the presence of complex failure scenarios. It can be configured to provide assurance against data loss, even where individual nodes have ephemeral storage, and groups of nodes can be concurrently impacted by failure events. It is a reliable system whilst running on simple, low-cost, commodity components - remaining highly available without the need for urgent operator intervention.

Riak is commonly used as a schema-free database for the storage and indexing of records, documents, objects or binaries with minimal constraints imposed by the database. In functional terms, Riak can be considered to be a hybrid combination of some of the features available within S3 and DynamoDB.

Riak fully supports multi-cluster environments (within and across physical locations), where open replication is possible not just between Riak clusters, but between Riak clusters and other database services. With Riak, reconciliation is considered as important as replication. Clusters may come in different shapes and sizes - but it is important that as well as replicating data between clusters, it is possible to do rapid and continuous verification that clusters remain synchronised.

Riak users have been running large-scale production databases in mission-critical environments on commodity hardware with more than a decade of continuous uptime. These environments are noted not just for their high availability, but for their low operator-intervention rates. Riak is often preferred in organisations where technology choices need to be long-lasting, and ongoing operational costs are a more important consideration than up-front developer costs.

Riak is built almost entirely using BEAM technology, a platform designed from the start to support the next generation of reliable systems. Over the past few years Riak has been evolved to make better use of the BEAM platform, and is now supported on an ongoing basis by a Working Group of the Erlang Ecosystem Foundation.

For further information, browse Riak QuickDocs

- [Initial Design Decisions](./InitialDesignDecisions.md)
- [Install and Start](./InstallAndStartGuide.md)
- [Build and Scale a Cluster](./BuildAndScaleClusterGuide.md)
- [Object API](./ObjectAPI.md)
- [Query API](./QueryAPI.md)
- [Other APIs](./OtherAPI.md)
- [Replication and Reconciliation](./ReplicationGuide.md)
- [Operations and Troubleshooting](./OperationsAndTroubleshootingGuide.md)
- [Riak Theory](./RiakTheoryGuide.md)
