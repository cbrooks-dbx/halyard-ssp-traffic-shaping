# Task `lakeflow_ingest` (Lakeflow pipeline d88fa974-f1d1-4680-81fd-b54e1e2b6239)

- result: **SUCCESS**, 103s

Most recent pipeline events (newest first):

```text
2026-10-20T18:14:02.160Z  INFO  update_progress        Update c0338a is COMPLETED.
2026-10-20T18:14:02.119Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.gold_segment_daily' has COMPLETED.
2026-10-20T18:13:52.228Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.gold_exchange_daily' has COMPLETED.
2026-10-20T18:13:52.164Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.gold_dsp_daily' has COMPLETED.
2026-10-20T18:13:46.752Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.gold_segment_daily' is RUNNING.
2026-10-20T18:13:46.444Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.gold_segment_daily' is STARTING.
2026-10-20T18:13:46.437Z  INFO  planning_information   Flow 'cmegdemos_catalog.ssp_traffic_shaping.gold_segment_daily' has been planned to be executed as GROUP_AGGREGATE.
2026-10-20T18:13:46.212Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.gold_exchange_daily' is RUNNING.
2026-10-20T18:13:46.204Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.gold_exchange_daily' is STARTING.
2026-10-20T18:13:46.201Z  INFO  planning_information   Flow 'cmegdemos_catalog.ssp_traffic_shaping.gold_exchange_daily' has been planned to be executed as GENERIC_AGGREGATE.
2026-10-20T18:13:45.985Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.gold_dsp_daily' is RUNNING.
2026-10-20T18:13:45.954Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.gold_dsp_daily' is STARTING.
2026-10-20T18:13:45.933Z  INFO  planning_information   Flow 'cmegdemos_catalog.ssp_traffic_shaping.gold_dsp_daily' has been planned to be executed as GENERIC_AGGREGATE.
2026-10-20T18:13:41.150Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.gold_dsp_daily' is PLANNING.
2026-10-20T18:13:41.148Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.gold_exchange_daily' is PLANNING.
2026-10-20T18:13:41.147Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.gold_segment_daily' is PLANNING.
2026-10-20T18:13:41.134Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.silver_dsp_calls' has COMPLETED.
2026-10-20T18:13:30.073Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.silver_dsp_calls' is RUNNING.
2026-10-20T18:13:30.053Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.silver_dsp_calls' is STARTING.
2026-10-20T18:13:30.036Z  INFO  planning_information   Flow 'cmegdemos_catalog.ssp_traffic_shaping.silver_dsp_calls' has been planned to be executed as COMPLETE_RECOMPUTE. Another option is available:ROW_BASED. COMPLETE_RECOMPUTE was chosen in the current run for its optimal
2026-10-20T18:13:10.561Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.silver_dsp_calls' is PLANNING.
2026-10-20T18:13:10.529Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.silver_auctions' has COMPLETED.
2026-10-20T18:13:08.496Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.silver_auctions' is RUNNING.
2026-10-20T18:13:07.980Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.silver_auctions' is STARTING.
2026-10-20T18:13:07.707Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.bronze_auctions' has COMPLETED.
2026-10-20T18:13:05.937Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.dim_publishers' has COMPLETED.
2026-10-20T18:13:05.900Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.dim_dsp_partners' has COMPLETED.
2026-10-20T18:13:02.121Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.dim_publishers' is RUNNING.
2026-10-20T18:13:02.119Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.dim_dsp_partners' is RUNNING.
2026-10-20T18:13:02.091Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.dim_publishers' is STARTING.
2026-10-20T18:13:02.080Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.dim_dsp_partners' is STARTING.
2026-10-20T18:13:02.077Z  INFO  planning_information   Flow 'cmegdemos_catalog.ssp_traffic_shaping.dim_publishers' has been planned to be executed as COMPLETE_RECOMPUTE.
2026-10-20T18:13:02.063Z  INFO  planning_information   Flow 'cmegdemos_catalog.ssp_traffic_shaping.dim_dsp_partners' has been planned to be executed as COMPLETE_RECOMPUTE.
2026-10-20T18:13:01.634Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.dim_publishers' is PLANNING.
2026-10-20T18:13:01.632Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.dim_dsp_partners' is PLANNING.
2026-10-20T18:13:01.198Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.bronze_auctions' is RUNNING.
2026-10-20T18:12:54.856Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.bronze_auctions' is STARTING.
2026-10-20T18:12:54.629Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.dim_publishers' is QUEUED.
2026-10-20T18:12:54.627Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.silver_dsp_calls' is QUEUED.
2026-10-20T18:12:54.626Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.gold_dsp_daily' is QUEUED.
2026-10-20T18:12:54.623Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.dim_dsp_partners' is QUEUED.
2026-10-20T18:12:54.622Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.gold_segment_daily' is QUEUED.
2026-10-20T18:12:54.619Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.gold_exchange_daily' is QUEUED.
2026-10-20T18:12:54.617Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.silver_auctions' is QUEUED.
2026-10-20T18:12:54.610Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.bronze_auctions' is QUEUED.
2026-10-20T18:12:54.596Z  INFO  update_progress        Update c0338a is RUNNING.
2026-10-20T18:12:54.588Z  INFO  graph_created          Dataflow graph successfully loaded.
2026-10-20T18:12:54.514Z  INFO  dataset_definition     Dataset `cmegdemos_catalog`.`ssp_traffic_shaping`.`dim_publishers` defined as MATERIALIZED_VIEW.
2026-10-20T18:12:54.503Z  INFO  dataset_definition     Dataset `cmegdemos_catalog`.`ssp_traffic_shaping`.`gold_dsp_daily` defined as MATERIALIZED_VIEW.
2026-10-20T18:12:54.490Z  INFO  dataset_definition     Dataset `cmegdemos_catalog`.`ssp_traffic_shaping`.`silver_auctions` defined as STREAMING_TABLE.
2026-10-20T18:12:54.480Z  INFO  dataset_definition     Dataset `cmegdemos_catalog`.`ssp_traffic_shaping`.`bronze_auctions` defined as STREAMING_TABLE.
2026-10-20T18:12:54.469Z  INFO  dataset_definition     Dataset `cmegdemos_catalog`.`ssp_traffic_shaping`.`dim_dsp_partners` defined as MATERIALIZED_VIEW.
2026-10-20T18:12:54.447Z  INFO  dataset_definition     Dataset `cmegdemos_catalog`.`ssp_traffic_shaping`.`silver_dsp_calls` defined as MATERIALIZED_VIEW.
2026-10-20T18:12:54.436Z  INFO  dataset_definition     Dataset `cmegdemos_catalog`.`ssp_traffic_shaping`.`gold_segment_daily` defined as MATERIALIZED_VIEW.
2026-10-20T18:12:54.414Z  INFO  dataset_definition     Dataset `cmegdemos_catalog`.`ssp_traffic_shaping`.`gold_exchange_daily` defined as MATERIALIZED_VIEW.
2026-10-20T18:12:54.392Z  INFO  flow_definition        Flow `cmegdemos_catalog`.`ssp_traffic_shaping`.`gold_exchange_daily` defined as MATERIALIZED_VIEW.
2026-10-20T18:12:54.387Z  INFO  flow_definition        Flow `cmegdemos_catalog`.`ssp_traffic_shaping`.`gold_segment_daily` defined as MATERIALIZED_VIEW.
2026-10-20T18:12:54.382Z  INFO  flow_definition        Flow `cmegdemos_catalog`.`ssp_traffic_shaping`.`gold_dsp_daily` defined as MATERIALIZED_VIEW.
2026-10-20T18:12:54.374Z  INFO  flow_definition        Flow `cmegdemos_catalog`.`ssp_traffic_shaping`.`silver_dsp_calls` defined as MATERIALIZED_VIEW.
2026-10-20T18:12:54.364Z  INFO  flow_definition        Flow `cmegdemos_catalog`.`ssp_traffic_shaping`.`silver_auctions` defined as APPEND.
2026-10-20T18:12:54.359Z  INFO  flow_definition        Flow `cmegdemos_catalog`.`ssp_traffic_shaping`.`dim_publishers` defined as MATERIALIZED_VIEW.
2026-10-20T18:12:54.343Z  INFO  flow_definition        Flow `cmegdemos_catalog`.`ssp_traffic_shaping`.`bronze_auctions` defined as APPEND.
2026-10-20T18:12:54.288Z  INFO  flow_definition        Flow `cmegdemos_catalog`.`ssp_traffic_shaping`.`dim_dsp_partners` defined as MATERIALIZED_VIEW.
2026-10-20T18:12:52.911Z  INFO  update_progress        Update c0338a is SETTING_UP_TABLES.
2026-10-20T18:12:36.822Z  INFO  update_progress        Update c0338a is INITIALIZING.
2026-10-20T18:12:31.050Z  INFO  runtime_details        Resolved runtime environment details for update c0338a.
2026-10-20T18:12:25.897Z  INFO  update_progress        Update c0338a is WAITING_FOR_RESOURCES.
2026-10-20T18:12:23.214Z  INFO  create_update          Update c0338a started by JOB_TASK.
2026-10-20T18:12:23.205Z  INFO  user_action            Principal 'chandler.brooks@databricks.com' started an update.
2026-10-20T17:50:36.724Z  INFO  update_progress        Update 9f39d7 is COMPLETED.
2026-10-20T17:50:36.680Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.gold_segment_daily' has COMPLETED.
2026-10-20T17:50:27.694Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.gold_dsp_daily' has COMPLETED.
2026-10-20T17:50:27.638Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.gold_exchange_daily' has COMPLETED.
2026-10-20T17:50:22.236Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.gold_segment_daily' is RUNNING.
2026-10-20T17:50:21.893Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.gold_segment_daily' is STARTING.
2026-10-20T17:50:21.868Z  INFO  planning_information   Flow 'cmegdemos_catalog.ssp_traffic_shaping.gold_segment_daily' has been planned to be executed as GROUP_AGGREGATE.
2026-10-20T17:50:21.563Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.gold_exchange_daily' is RUNNING.
2026-10-20T17:50:21.552Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.gold_exchange_daily' is STARTING.
2026-10-20T17:50:21.549Z  INFO  planning_information   Flow 'cmegdemos_catalog.ssp_traffic_shaping.gold_exchange_daily' has been planned to be executed as GENERIC_AGGREGATE.
2026-10-20T17:50:21.254Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.gold_dsp_daily' is RUNNING.
2026-10-20T17:50:21.233Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.gold_dsp_daily' is STARTING.
2026-10-20T17:50:21.222Z  INFO  planning_information   Flow 'cmegdemos_catalog.ssp_traffic_shaping.gold_dsp_daily' has been planned to be executed as GENERIC_AGGREGATE.
2026-10-20T17:50:16.851Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.gold_dsp_daily' is PLANNING.
2026-10-20T17:50:16.849Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.gold_exchange_daily' is PLANNING.
2026-10-20T17:50:16.848Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.gold_segment_daily' is PLANNING.
2026-10-20T17:50:16.835Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.silver_dsp_calls' has COMPLETED.
2026-10-20T17:50:05.562Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.silver_dsp_calls' is RUNNING.
2026-10-20T17:50:05.545Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.silver_dsp_calls' is STARTING.
2026-10-20T17:50:05.530Z  INFO  planning_information   Flow 'cmegdemos_catalog.ssp_traffic_shaping.silver_dsp_calls' has been planned to be executed as COMPLETE_RECOMPUTE. Another option is available:ROW_BASED. COMPLETE_RECOMPUTE was chosen in the current run for its optimal
2026-10-20T17:49:45.435Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.silver_dsp_calls' is PLANNING.
2026-10-20T17:49:45.404Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.silver_auctions' has COMPLETED.
2026-10-20T17:49:42.966Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.silver_auctions' is RUNNING.
2026-10-20T17:49:42.538Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.silver_auctions' is STARTING.
2026-10-20T17:49:42.313Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.bronze_auctions' has COMPLETED.
2026-10-20T17:49:41.599Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.dim_publishers' has COMPLETED.
2026-10-20T17:49:41.543Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.dim_dsp_partners' has COMPLETED.
2026-10-20T17:49:37.689Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.dim_dsp_partners' is RUNNING.
2026-10-20T17:49:37.687Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.dim_publishers' is RUNNING.
2026-10-20T17:49:37.653Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.dim_dsp_partners' is STARTING.
2026-10-20T17:49:37.644Z  INFO  flow_progress          Flow 'cmegdemos_catalog.ssp_traffic_shaping.dim_publishers' is STARTING.
```