---
title: Scheduler architecture
---

# Scheduler


The scheduler code can be seen as having 3 main sub-parts as depicted below (class names start with capital letter, directory names in parentheses, arrow pointing to class members). These 3 main components relate to the scheduler logic, the metadata handling and the backend API. 

- **Scheduler Logic**: 
  
  The Scheduler class implementing a tape resource scheduler. This class is the main entry point for most of the operations on both the tape file catalogue and the object store for queues. An exception is although used for operations that would trivially map to catalogue operations. In the same directory you can find all the different classes with methods relevant for the scheduling workflow itself. Contains an abstract `SchedulerDatabase` class providing an interface to the metadata handling side of the Scheduler. 

- **Metadata Handling**: 
  
  Contains a concrete implementation for a particular metadata handling use-case. One use-case is a Catalogue serving as a permanent metadata data store. Second use case is a scheduler backend related to the either an ObjectStore (OStoreDB, used in production) or a RelationalDB (PostgreSQL, pre-production deployment 2026).

- **Backend API**: 
  
  Low level interface specific to the SchedulerDB. For example, contains classes handling of the objects in the ObjectStore, or handling of the statements and SQL queries for the relational databases in use.


```mermaid
stateDiagram-v2
    direction LR
    classDef whitesystem fill:white,stroke:black
    classDef indevelopment fill:#ff8c1a,stroke:black
    classDef blueok fill:#66b3ff,stroke:black
    Scheduler:::blueok --> SchedulerDatabase:::blueok
    Scheduler:::blueok --> Catalogue\n(cta/catalogue):::blueok
    OStoreDB\n(cta/scheduler/OStoreDB):::blueok --> BackendRados\n(cta/objectstore):::blueok
    SchedulerDatabase --> OStoreDB\n(cta/scheduler/OStoreDB)
    RelationalDB\n(cta/scheduler/rdbms):::indevelopment --> rbms\n(cta/rdbms):::blueok
    SchedulerDatabase --> RelationalDB\n(cta/scheduler/rdbms):::indevelopment
    scheduler&nbsp;logic\n(cta/scheduler):::whitesystem
    metadata&nbsp;handling:::whitesystem
    backend&nbsp;API:::whitesystem
    state scheduler&nbsp;logic\n(cta/scheduler){
       Archive/Retrieve&#45Mount --> Catalogue\n(cta/catalogue)
       Archive/Retrieve&#45Mount --> SchedulerDatabase
       Archive/Retrieve&#45Job --> Archive/Retrieve&#45Mount
       Archive/Retrieve&#45Job --> Catalogue\n(cta/catalogue)
       [...&nbsp;and&nbsp;more&nbsp;...]
       Scheduler
       SchedulerDatabase       
    }
    SchedulerDB:::whitesystem
    state metadata&nbsp;handling{
       state SchedulerDB{
          OStoreDB\n(cta/scheduler/OStoreDB) 
          RelationalDB\n(cta/scheduler/rdbms)
       }
       Catalogue\n(cta/catalogue) --> rbms\n(cta/rdbms)
       Catalogue\n(cta/catalogue) --> rbms\n(cta/rdbms)
    }
    state backend&nbsp;API{
         rbms\n(cta/rdbms)
         BackendRados\n(cta/objectstore)
    }
    Archive/Retrieve&#45Mount :::blueok
    Archive/Retrieve&#45Job:::blueok
    [...&nbsp;and&nbsp;more&nbsp;...]:::whitesystem
```


The [Scheduling Workflow](../workflows/scheduling.md) is documented under the Workflows section. 
The code itself can be found [here](https://gitlab.cern.ch/cta/CTA). 


