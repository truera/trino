# TruEra Trino Extensions
This module is a custom Trino plugin for TruEra. Currently it just contains one new function (ROC-AUC).

## How do I add to the plugin?
To get started, read the Trino [dev guide](https://trino.io/docs/current/develop/spi-overview.html#)

## How do I test the package?
1. Make sure that you have set the env variable $AILENS_DEPENDENCIES_DIR_OVERRIDE
2. Install the plugin to local trino: `bash install_plugin_local.sh` 
3. Restart Trino (`./service.sh stop trino && ./service.sh start trino`)

You can test the "roc_auc" function with a command like:
```sql
SELECT auc_roc(__truera_label__, __truera_prediction__) 
FROM "iceberg"."tablestore"."a83db5d335ab494590cd7ada132707ad_predictions_probits_score" as pred
JOIN "iceberg"."tablestore"."a83db5d335ab494590cd7ada132707ad_label" 
AS label ON pred.__truera_id__ = label.__truera_id__;
```

## Release
Use this Pipeline: https://dev.azure.com/ailens-io/truera/_build?definitionId=124
