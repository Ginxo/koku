-- First we'll store the data in a "temp" table to do our grouping against
CREATE TABLE IF NOT EXISTS {{schema | sqlsafe}}.azure_openshift_daily_resource_matched_temp
(
    uuid varchar,
    usage_start timestamp,
    resource_id varchar,
    service_name varchar,
    data_transfer_direction varchar,
    instance_type varchar,
    subscription_guid varchar,
    subscription_name varchar,
    resource_location varchar,
    unit_of_measure varchar,
    usage_quantity double,
    currency varchar,
    pretax_cost double,
    tags varchar,
    resource_id_matched boolean,
    ocp_source varchar,
    year varchar,
    month varchar
) WITH(format = 'PARQUET', partitioned_by=ARRAY['ocp_source', 'year', 'month'])
;

CREATE TABLE IF NOT EXISTS {{schema | sqlsafe}}.azure_openshift_daily_tag_matched_temp
(
    uuid varchar,
    usage_start timestamp,
    resource_id varchar,
    service_name varchar,
    instance_type varchar,
    subscription_guid varchar,
    subscription_name varchar,
    resource_location varchar,
    unit_of_measure varchar,
    usage_quantity double,
    currency varchar,
    pretax_cost double,
    tags varchar,
    matched_tag varchar,
    ocp_source varchar,
    year varchar,
    month varchar
) WITH(format = 'PARQUET', partitioned_by=ARRAY['ocp_source', 'year', 'month'])
;

CREATE TABLE IF NOT EXISTS hive.{{schema | sqlsafe}}.reporting_ocpazurecostlineitem_project_daily_summary_temp
(
    azure_uuid varchar,
    cluster_id varchar,
    cluster_alias varchar,
    data_source varchar,
    namespace varchar,
    node varchar,
    persistentvolumeclaim varchar,
    persistentvolume varchar,
    storageclass varchar,
    resource_id varchar,
    usage_start timestamp,
    usage_end timestamp,
    service_name varchar,
    instance_type varchar,
    subscription_guid varchar,
    subscription_name varchar,
    resource_location varchar,
    unit_of_measure varchar,
    usage_quantity double,
    currency varchar,
    pretax_cost double,
    markup_cost double,
    pod_cost double,
    project_markup_cost double,
    pod_usage_cpu_core_hours double,
    pod_request_cpu_core_hours double,
    pod_effective_usage_cpu_core_hours double,
    pod_limit_cpu_core_hours double,
    pod_usage_memory_gigabyte_hours double,
    pod_request_memory_gigabyte_hours double,
    pod_effective_usage_memory_gigabyte_hours double,
    node_capacity_cpu_core_hours double,
    node_capacity_memory_gigabyte_hours double,
    cluster_capacity_cpu_core_hours double,
    cluster_capacity_memory_gigabyte_hours double,
    pod_labels varchar,
    volume_labels varchar,
    tags varchar,
    project_rank integer,
    data_source_rank integer,
    resource_id_matched boolean,
    cost_category_id int,
    ocp_source varchar,
    year varchar,
    month varchar
) WITH(format = 'PARQUET', partitioned_by=ARRAY['ocp_source', 'year', 'month'])
;

-- Now create our proper table if it does not exist
CREATE TABLE IF NOT EXISTS hive.{{schema | sqlsafe}}.reporting_ocpazurecostlineitem_project_daily_summary
(
    azure_uuid varchar,
    cluster_id varchar,
    cluster_alias varchar,
    data_source varchar,
    namespace varchar,
    node varchar,
    persistentvolumeclaim varchar,
    persistentvolume varchar,
    storageclass varchar,
    resource_id varchar,
    usage_start timestamp,
    usage_end timestamp,
    service_name varchar,
    data_transfer_direction varchar,
    instance_type varchar,
    subscription_guid varchar,
    subscription_name varchar,
    resource_location varchar,
    unit_of_measure varchar,
    usage_quantity double,
    currency varchar,
    pretax_cost double,
    markup_cost double,
    pod_cost double,
    project_markup_cost double,
    pod_labels varchar,
    tags varchar,
    project_rank integer,
    data_source_rank integer,
    cost_category_id int,
    azure_source varchar,
    ocp_source varchar,
    year varchar,
    month varchar,
    day varchar
) WITH(format = 'PARQUET', partitioned_by=ARRAY['azure_source', 'ocp_source', 'year', 'month', 'day'])
;

INSERT INTO hive.{{schema | sqlsafe}}.azure_openshift_daily_resource_matched_temp (
    uuid,
    usage_start,
    resource_id,
    service_name,
    data_transfer_direction,
    instance_type,
    subscription_guid,
    subscription_name,
    resource_location,
    unit_of_measure,
    usage_quantity,
    currency,
    pretax_cost,
    tags,
    resource_id_matched,
    ocp_source,
    year,
    month
)
SELECT cast(uuid() as varchar) as uuid,
    coalesce(azure.date, azure.usagedatetime) as usage_start,
    split_part(coalesce(nullif(resourceid, ''), instanceid), '/', 9) as resource_id,
    coalesce(nullif(servicename, ''), metercategory) as service_name,
    CASE
        WHEN coalesce(nullif(servicename, ''), metercategory) = 'Virtual Network' AND lower(consumedservice)='microsoft.compute' AND json_exists(lower(additionalinfo), 'strict $.datatransferdirection')
            THEN json_extract_scalar(lower(additionalinfo), '$.datatransferdirection')
        ELSE NULL
    END as data_transfer_direction,
    max(json_extract_scalar(json_parse(azure.additionalinfo), '$.ServiceType')) as instance_type,
    coalesce(nullif(azure.subscriptionid, ''), azure.subscriptionguid) as subscription_guid,
    max(azure.subscriptionname) as subscription_name,
    azure.resourcelocation as resource_location,
    max(CASE
        WHEN split_part(unitofmeasure, ' ', 2) = 'Hours'
            THEN  'Hrs'
        WHEN split_part(unitofmeasure, ' ', 2) = 'GB/Month'
            THEN  'GB-Mo'
        WHEN split_part(unitofmeasure, ' ', 2) != ''
            THEN  split_part(unitofmeasure, ' ', 2)
        ELSE unitofmeasure
    END) as unit_of_measure,
    sum(coalesce(nullif(azure.quantity, 0), azure.usagequantity)) as usage_quantity,
    coalesce(nullif(azure.billingcurrencycode, ''), nullif(azure.currency, ''), azure.billingcurrency) as currency,
    sum(coalesce(nullif(azure.costinbillingcurrency, 0), azure.pretaxcost)) as pretax_cost,
    azure.tags,
    max(azure.resource_id_matched) as resource_id_matched,
    {{ocp_source_uuid}} as ocp_source,
    max(azure.year) as year,
    max(azure.month) as month
FROM hive.{{schema | sqlsafe}}.azure_openshift_daily as azure
WHERE azure.source = {{azure_source_uuid}}
    AND azure.year = {{year}}
    AND azure.month = {{month}}
    AND coalesce(azure.date, azure.usagedatetime) >= {{start_date}}
    AND coalesce(azure.date, azure.usagedatetime) < date_add('day', 1, {{end_date}})
    AND azure.resource_id_matched = TRUE
GROUP BY coalesce(azure.date, azure.usagedatetime),
    split_part(coalesce(nullif(resourceid, ''), instanceid), '/', 9),
    lower(azure.consumedservice),
    5, -- data transfer direction
    coalesce(nullif(servicename, ''), metercategory),
    coalesce(nullif(subscriptionid, ''), subscriptionguid),
    azure.subscriptionname,
    azure.resourcelocation,
    coalesce(nullif(azure.billingcurrencycode, ''), nullif(azure.currency, ''), azure.billingcurrency),
    azure.tags
;


INSERT INTO hive.{{schema | sqlsafe}}.azure_openshift_daily_tag_matched_temp (
    uuid,
    usage_start,
    resource_id,
    service_name,
    instance_type,
    subscription_guid,
    subscription_name,
    resource_location,
    unit_of_measure,
    usage_quantity,
    currency,
    pretax_cost,
    tags,
    matched_tag,
    ocp_source,
    year,
    month
)
WITH cte_enabled_tag_keys AS (
    SELECT
    CASE WHEN array_agg(key) IS NOT NULL
        THEN array_union(ARRAY['openshift_cluster', 'openshift_node', 'openshift_project'], array_agg(key))
        ELSE ARRAY['openshift_cluster', 'openshift_node', 'openshift_project']
    END as enabled_keys
    FROM postgres.{{schema | sqlsafe}}.reporting_enabledtagkeys
    WHERE enabled = TRUE
        AND provider_type = 'Azure'
)
SELECT cast(uuid() as varchar) as uuid,
    coalesce(azure.date, azure.usagedatetime) as usage_start,
    split_part(coalesce(resourceid, instanceid), '/', 9) as resource_id,
    coalesce(nullif(servicename, ''), metercategory) as service_name,
    max(json_extract_scalar(json_parse(azure.additionalinfo), '$.ServiceType')) as instance_type,
    coalesce(nullif(azure.subscriptionid, ''), azure.subscriptionguid) as subscription_guid,
    max(azure.subscriptionname) as subscription_name,
    azure.resourcelocation as resource_location,
    max(CASE
        WHEN split_part(unitofmeasure, ' ', 2) = 'Hours'
            THEN  'Hrs'
        WHEN split_part(unitofmeasure, ' ', 2) = 'GB/Month'
            THEN  'GB-Mo'
        WHEN split_part(unitofmeasure, ' ', 2) != ''
            THEN  split_part(unitofmeasure, ' ', 2)
        ELSE unitofmeasure
    END) as unit_of_measure,
    sum(coalesce(nullif(azure.quantity, 0), azure.usagequantity)) as usage_quantity,
    coalesce(nullif(azure.billingcurrencycode, ''), nullif(azure.currency, ''), azure.billingcurrency) as currency,
    sum(coalesce(nullif(azure.costinbillingcurrency, 0), azure.pretaxcost)) as pretax_cost,
    json_format(
        cast(
            map_filter(
                cast(json_parse(azure.tags) as map(varchar, varchar)),
                (k, v) -> contains(etk.enabled_keys, k)
            ) as json
        )
    ) as tags,
    azure.matched_tag,
    {{ocp_source_uuid}} as ocp_source,
    max(azure.year) as year,
    max(azure.month) as month
FROM hive.{{schema | sqlsafe}}.azure_openshift_daily as azure
CROSS JOIN cte_enabled_tag_keys as etk
WHERE azure.source = {{azure_source_uuid}}
    AND azure.year = {{year}}
    AND azure.month = {{month}}
    AND coalesce(azure.date, azure.usagedatetime) >= {{start_date}}
    AND coalesce(azure.date, azure.usagedatetime) < date_add('day', 1, {{end_date}})
    AND (azure.resource_id_matched = FALSE OR azure.resource_id_matched IS NULL)
GROUP BY coalesce(azure.date, azure.usagedatetime),
    split_part(coalesce(resourceid, instanceid), '/', 9),
    coalesce(nullif(servicename, ''), metercategory),
    coalesce(nullif(subscriptionid, ''), subscriptionguid),
    azure.subscriptionname,
    azure.resourcelocation,
    coalesce(nullif(azure.billingcurrencycode, ''), nullif(azure.currency, ''), azure.billingcurrency),
    13, -- tags
    azure.matched_tag
;

-- Claimless PVs Unattributed Storage
INSERT INTO hive.{{schema | sqlsafe}}.reporting_ocpazurecostlineitem_project_daily_summary_temp (
    azure_uuid,
    cluster_id,
    cluster_alias,
    data_source,
    namespace,
    node,
    persistentvolumeclaim,
    persistentvolume,
    storageclass,
    resource_id,
    usage_start,
    usage_end,
    service_name,
    instance_type,
    subscription_guid,
    subscription_name,
    resource_location,
    unit_of_measure,
    usage_quantity,
    currency,
    pretax_cost,
    markup_cost,
    pod_cost,
    project_markup_cost,
    pod_usage_cpu_core_hours,
    pod_request_cpu_core_hours,
    pod_effective_usage_cpu_core_hours,
    pod_limit_cpu_core_hours,
    pod_usage_memory_gigabyte_hours,
    pod_request_memory_gigabyte_hours,
    pod_effective_usage_memory_gigabyte_hours,
    node_capacity_cpu_core_hours,
    node_capacity_memory_gigabyte_hours,
    cluster_capacity_cpu_core_hours,
    cluster_capacity_memory_gigabyte_hours,
    pod_labels,
    volume_labels,
    tags,
    resource_id_matched,
    cost_category_id,
    ocp_source,
    year,
    month
)
WITH cte_ocp_filtered_resources as (
    SELECT
        distinct azure.resource_id as azure_partial_resource_id
    FROM hive.{{schema | sqlsafe}}.reporting_ocpusagelineitem_daily_summary as ocp
    JOIN hive.{{schema | sqlsafe}}.azure_openshift_daily_resource_matched_temp as azure
        ON (azure.usage_start = ocp.usage_start)
        AND (
                (strpos(azure.resource_id, ocp.persistentvolume) > 0 AND ocp.data_source = 'Storage')
            OR
                (lower(ocp.csi_volume_handle) = lower(azure.resource_id))
            )
    WHERE ocp.source = {{ocp_source_uuid}}
        AND ocp.year = {{year}}
        AND lpad(ocp.month, 2, '0') = {{month}}
        AND ocp.usage_start >= {{start_date}}
        AND ocp.usage_start < date_add('day', 1, {{end_date}})
        AND ocp.persistentvolume is not null
        AND persistentvolumeclaim = ''
        AND azure.ocp_source = {{ocp_source_uuid}}
        AND azure.year = {{year}}
        AND azure.month = {{month}}
),
cte_az_resource_to_disk_capacity as (
    SELECT
        ocp_filtered.azure_partial_resource_id as resource_id,
        max(az_disk_capacity.capacity) as capacity,
        sum(pretaxcost) as disk_cost,
        date(coalesce(date, usagedatetime)) as usage_start
    FROM azure_line_items as azure
    JOIN postgres.public.reporting_common_diskcapacity as az_disk_capacity
        ON azure.metername LIKE '%' || az_disk_capacity.product_substring || ' %' -- space here is important to avoid partial matching
        AND az_disk_capacity.provider_type = 'Azure'
    JOIN cte_ocp_filtered_resources as ocp_filtered
        ON split_part(coalesce(nullif(azure.resourceid, ''), azure.instanceid), '/', 9) = ocp_filtered.azure_partial_resource_id
    WHERE coalesce(azure.date, azure.usagedatetime) >= TIMESTAMP '{{start_date | sqlsafe}}'
        AND coalesce(azure.date, azure.usagedatetime) < date_add('day', 1, TIMESTAMP '{{end_date | sqlsafe}}')
        AND coalesce(nullif(azure.servicename, ''), azure.metercategory) LIKE '%Storage%'
        AND coalesce(nullif(azure.resourceid, ''), azure.instanceid) LIKE '%%Microsoft.Compute/disks/%%'
    GROUP BY ocp_filtered.azure_partial_resource_id, date(coalesce(date, usagedatetime))
),
cte_total_pv_capacity as (
    SELECT
        SUM(combined_requests.capacity) as total_pv_capacity
    FROM (
        SELECT
            ocp.persistentvolume,
            max(ocp.persistentvolumeclaim_capacity_gigabyte) as capacity
        FROM hive.{{schema | sqlsafe}}.reporting_ocpusagelineitem_daily_summary as ocp
        JOIN hive.{{schema | sqlsafe}}.azure_openshift_daily_resource_matched_temp as azure
        ON (azure.usage_start = ocp.usage_start)
        AND (
                (strpos(azure.resource_id, ocp.persistentvolume) > 0 AND ocp.data_source = 'Storage')
            OR
                (lower(ocp.csi_volume_handle) = lower(azure.resource_id))
            )
        WHERE ocp.year = {{year}}
            AND lpad(ocp.month, 2, '0') = {{month}}
            AND ocp.usage_start >= {{start_date}}
            AND ocp.usage_start < date_add('day', 1, {{end_date}})
        GROUP BY ocp.persistentvolume
    ) as combined_requests
)
SELECT azure.uuid as azure_uuid,
    cast(NULL as varchar) as cluster_id,
    cast(NULL as varchar) as cluster_alias,
    'Storage' as data_source,
    'Storage unattributed' as namespace,
    cast(NULL as varchar) as node,
    max(persistentvolumeclaim) as persistentvolumeclaim,
    max(persistentvolume) as persistentvolume,
    max(storageclass) as storageclass,
    max(azure.resource_id) as resource_id,
    max(azure.usage_start) as usage_start,
    max(azure.usage_start) as usage_end,
    max(nullif(azure.service_name, '')) as service_name,
    max(azure.instance_type) as instance_type,
    max(azure.subscription_guid) as subscription_guid,
    max(azure.subscription_name) as subscription_name,
    max(nullif(azure.resource_location, '')) as resource_location,
    cast(NULL as varchar) as unit_of_measure,
    cast(NULL as double) as usage_quantity,
    max(azure.currency) as currency,
    (max(az_disk.capacity) - max(persistentvolumeclaim_capacity_gigabyte)) / max(az_disk.capacity) * max(cast(azure.pretax_cost as decimal(24,9)))  as pretax_cost,
    ((max(az_disk.capacity) - max(persistentvolumeclaim_capacity_gigabyte)) / max(az_disk.capacity) * max(cast(azure.pretax_cost as decimal(24,9)))) * cast({{markup}} as decimal(24,9)) as markup_cost, -- pretax_cost x markup = markup_cost
    cast(NULL as double) as pod_cost,
    cast(NULL as double) as project_markup_cost,
    cast(NULL as double) as pod_usage_cpu_core_hours,
    cast(NULL as double) as pod_request_cpu_core_hours,
    cast(NULL as double) as pod_effective_usage_cpu_core_hours,
    cast(NULL as double) as pod_limit_cpu_core_hours,
    cast(NULL as double) as pod_usage_memory_gigabyte_hours,
    cast(NULL as double) as pod_request_memory_gigabyte_hours,
    cast(NULL as double) as pod_effective_usage_memory_gigabyte_hours,
    cast(NULL as double) as node_capacity_cpu_core_hours,
    cast(NULL as double) as node_capacity_memory_gigabyte_hours,
    cast(NULL as double) as cluster_capacity_cpu_core_hours,
    cast(NULL as double) as cluster_capacity_memory_gigabyte_hours,
    cast(NULL as varchar) as pod_labels,
    cast(NULL as varchar) as volume_labels,
    max(azure.tags) as tags,
    max(azure.resource_id_matched) as resource_id_matched,
    max(ocp.cost_category_id) as cost_category_id,
    {{ocp_source_uuid}} as ocp_source,
    max(azure.year) as year,
    max(azure.month) as month
    FROM hive.{{schema | sqlsafe}}.reporting_ocpusagelineitem_daily_summary as ocp
    JOIN hive.{{schema | sqlsafe}}.azure_openshift_daily_resource_matched_temp as azure
        ON (azure.usage_start = ocp.usage_start)
        AND (
                (strpos(azure.resource_id, ocp.persistentvolume) > 0 AND ocp.data_source = 'Storage')
            OR
                (lower(ocp.csi_volume_handle) = lower(azure.resource_id))
            )
    JOIN cte_az_resource_to_disk_capacity AS az_disk
        ON az_disk.usage_start = azure.usage_start
        AND az_disk.resource_id = azure.resource_id
    LEFT JOIN cte_total_pv_capacity as pv_cap
        ON 1 = 1
    WHERE ocp.source = {{ocp_source_uuid}}
        AND ocp.year = {{year}}
        AND lpad(ocp.month, 2, '0') = {{month}} -- Zero pad the month when fewer than 2 characters
        AND ocp.usage_start >= {{start_date}}
        AND ocp.usage_start < date_add('day', 1, {{end_date}})
        AND ocp.persistentvolume is not null
        AND persistentvolumeclaim = ''
        -- Filter out Node Network Costs because they cannot be tied to namespace level
        AND azure.data_transfer_direction IS NULL
        AND azure.ocp_source = {{ocp_source_uuid}}
        AND azure.year = {{year}}
        AND azure.month = {{month}}
    GROUP BY azure.uuid, ocp.namespace, ocp.data_source, ocp.pod_labels, ocp.volume_labels
;

-- Storage Disk resource_id matching
INSERT INTO hive.{{schema | sqlsafe}}.reporting_ocpazurecostlineitem_project_daily_summary_temp (
    azure_uuid,
    cluster_id,
    cluster_alias,
    data_source,
    namespace,
    node,
    persistentvolumeclaim,
    persistentvolume,
    storageclass,
    resource_id,
    usage_start,
    usage_end,
    service_name,
    instance_type,
    subscription_guid,
    subscription_name,
    resource_location,
    unit_of_measure,
    usage_quantity,
    currency,
    pretax_cost,
    markup_cost,
    pod_cost,
    project_markup_cost,
    pod_usage_cpu_core_hours,
    pod_request_cpu_core_hours,
    pod_effective_usage_cpu_core_hours,
    pod_limit_cpu_core_hours,
    pod_usage_memory_gigabyte_hours,
    pod_request_memory_gigabyte_hours,
    pod_effective_usage_memory_gigabyte_hours,
    node_capacity_cpu_core_hours,
    node_capacity_memory_gigabyte_hours,
    cluster_capacity_cpu_core_hours,
    cluster_capacity_memory_gigabyte_hours,
    pod_labels,
    volume_labels,
    tags,
    resource_id_matched,
    cost_category_id,
    ocp_source,
    year,
    month
)
WITH cte_ocp_filtered_resources as (
    SELECT
        distinct azure.resource_id as azure_partial_resource_id
    FROM hive.{{schema | sqlsafe}}.reporting_ocpusagelineitem_daily_summary as ocp
    JOIN hive.{{schema | sqlsafe}}.azure_openshift_daily_resource_matched_temp as azure
        ON (azure.usage_start = ocp.usage_start)
        AND (
                (strpos(azure.resource_id, ocp.persistentvolume) > 0 AND ocp.data_source = 'Storage')
            OR
                (lower(ocp.csi_volume_handle) = lower(azure.resource_id))
            )
    WHERE ocp.source = {{ocp_source_uuid}}
        AND ocp.year = {{year}}
        AND lpad(ocp.month, 2, '0') = {{month}}
        AND ocp.usage_start >= {{start_date}}
        AND ocp.usage_start < date_add('day', 1, {{end_date}})
        AND (ocp.resource_id IS NOT NULL AND ocp.resource_id != '')
        AND azure.ocp_source = {{ocp_source_uuid}}
        AND azure.year = {{year}}
        AND azure.month = {{month}}
),
cte_az_resource_to_disk_capacity as (
    SELECT
        ocp_filtered.azure_partial_resource_id as resource_id,
        max(az_disk_capacity.capacity) as capacity,
        sum(pretaxcost) as disk_cost,
        date(coalesce(date, usagedatetime)) as usage_start
    FROM azure_line_items as azure
    JOIN postgres.public.reporting_common_diskcapacity as az_disk_capacity
        ON azure.metername LIKE '%' || az_disk_capacity.product_substring || ' %' -- space here is important to avoid partial matching
        AND az_disk_capacity.provider_type = 'Azure'
    JOIN cte_ocp_filtered_resources as ocp_filtered
        ON split_part(coalesce(nullif(azure.resourceid, ''), azure.instanceid), '/', 9) = ocp_filtered.azure_partial_resource_id
    WHERE coalesce(azure.date, azure.usagedatetime) >= TIMESTAMP '{{start_date | sqlsafe}}'
        AND coalesce(azure.date, azure.usagedatetime) < date_add('day', 1, TIMESTAMP '{{end_date | sqlsafe}}')
        AND coalesce(nullif(azure.servicename, ''), azure.metercategory) LIKE '%Storage%'
        AND coalesce(nullif(azure.resourceid, ''), azure.instanceid) LIKE '%%Microsoft.Compute/disks/%%'
    GROUP BY ocp_filtered.azure_partial_resource_id, date(coalesce(date, usagedatetime))
),
cte_total_pv_capacity as (
    SELECT
        SUM(combined_requests.capacity) as total_pv_capacity
    FROM (
        SELECT
            ocp.persistentvolume,
            max(ocp.persistentvolumeclaim_capacity_gigabyte) as capacity
        FROM hive.{{schema | sqlsafe}}.reporting_ocpusagelineitem_daily_summary as ocp
        JOIN hive.{{schema | sqlsafe}}.azure_openshift_daily_resource_matched_temp as azure
        ON (azure.usage_start = ocp.usage_start)
        AND (
                (strpos(azure.resource_id, ocp.persistentvolume) > 0 AND ocp.data_source = 'Storage')
            OR
                (lower(ocp.csi_volume_handle) = lower(azure.resource_id))
            )
        WHERE ocp.year = {{year}}
            AND lpad(ocp.month, 2, '0') = {{month}}
            AND ocp.usage_start >= {{start_date}}
            AND ocp.usage_start < date_add('day', 1, {{end_date}})
        GROUP BY ocp.persistentvolume
    ) as combined_requests
)
SELECT
    disk_cost.azure_uuid as azure_disk,
    disk_cost.cluster_id as cluster_id,
    disk_cost.cluster_alias as cluster_alias,
    disk_cost.data_source as data_source,
    disk_cost.namespace as namespace,
    disk_cost.node as node,
    disk_cost.persistentvolumeclaim as persistentvolumeclaim,
    disk_cost.persistentvolume as persistentvolume,
    disk_cost.storageclass as storageclass,
    disk_cost.resource_id as resource_id,
    disk_cost.usage_start as usage_start,
    disk_cost.usage_end as usage_end,
    disk_cost.service_name as service_name,
    disk_cost.instance_type as instance_type,
    disk_cost.subscription_guid as subscription_guid,
    disk_cost.subscription_name as subscription_name,
    disk_cost.resource_location as resource_location,
    disk_cost.unit_of_measure as unit_of_measure,
    disk_cost.usage_quantity as usage_quantity,
    disk_cost.currency as currency,
    disk_cost.pretax_cost as pretax_cost,
    disk_cost.markup_cost as markup_cost,
    disk_cost.pod_cost as pod_cost,
    disk_cost.project_markup_cost as project_markup_cost,
    disk_cost.pod_usage_cpu_core_hours as pod_usage_cpu_core_hours,
    disk_cost.pod_request_cpu_core_hours as pod_request_cpu_core_hours,
    disk_cost.pod_effective_usage_cpu_core_hours as pod_effective_usage_cpu_core_hours,
    disk_cost.pod_limit_cpu_core_hours as pod_limit_cpu_core_hours,
    disk_cost.pod_usage_memory_gigabyte_hours as pod_usage_memory_gigabyte_hours,
    disk_cost.pod_request_memory_gigabyte_hours as pod_request_memory_gigabyte_hours,
    disk_cost.pod_effective_usage_memory_gigabyte_hours as pod_effective_usage_memory_gigabyte_hours,
    disk_cost.node_capacity_cpu_core_hours as node_capacity_cpu_core_hours,
    disk_cost.node_capacity_memory_gigabyte_hours as node_capacity_memory_gigabyte_hours,
    disk_cost.cluster_capacity_cpu_core_hours as cluster_capacity_cpu_core_hours,
    disk_cost.cluster_capacity_memory_gigabyte_hours as cluster_capacity_memory_gigabyte_hours,
    disk_cost.pod_labels as pod_labels,
    disk_cost.volume_labels as volume_labels,
    disk_cost.tags as tags,
    disk_cost.resource_id_matched as resource_id_matched,
    disk_cost.cost_category_id as cost_category_id,
    disk_cost.ocp_source as ocp_source,
    disk_cost.year as year,
    disk_cost.month as month
FROM (
    SELECT azure.uuid as azure_uuid,
        cast(NULL as varchar) as cluster_id,
        cast(NULL as varchar) as cluster_alias,
        'Storage' as data_source,
        'Storage unattributed' as namespace,
        cast(NULL as varchar) as node,
        cast(NULL as varchar) as persistentvolumeclaim,
        cast(NULL as varchar) as persistentvolume,
        cast(NULL as varchar) as storageclass,
        azure.resource_id as resource_id,
        max(azure.usage_start) as usage_start,
        max(azure.usage_start) as usage_end,
        max(nullif(azure.service_name, '')) as service_name,
        max(azure.instance_type) as instance_type,
        max(azure.subscription_guid) as subscription_guid,
        max(azure.subscription_name) as subscription_name,
        max(nullif(azure.resource_location, '')) as resource_location,
        cast(NULL as varchar) as unit_of_measure,
        cast(NULL as double) as usage_quantity,
        max(azure.currency) as currency,
        (max(az_disk.capacity) - max(persistentvolumeclaim_capacity_gigabyte)) / max(az_disk.capacity) * max(cast(azure.pretax_cost as decimal(24,9)))  as pretax_cost,
        ((max(az_disk.capacity) - max(persistentvolumeclaim_capacity_gigabyte)) / max(az_disk.capacity) * max(cast(azure.pretax_cost as decimal(24,9)))) * cast({{markup}} as decimal(24,9)) as markup_cost, -- pretax_cost x markup = markup_cost
        cast(NULL as double) as pod_cost,
        cast(NULL as double) as project_markup_cost,
        cast(NULL as double) as pod_usage_cpu_core_hours,
        cast(NULL as double) as pod_request_cpu_core_hours,
        cast(NULL as double) as pod_effective_usage_cpu_core_hours,
        cast(NULL as double) as pod_limit_cpu_core_hours,
        cast(NULL as double) as pod_usage_memory_gigabyte_hours,
        cast(NULL as double) as pod_request_memory_gigabyte_hours,
        cast(NULL as double) as pod_effective_usage_memory_gigabyte_hours,
        cast(NULL as double) as node_capacity_cpu_core_hours,
        cast(NULL as double) as node_capacity_memory_gigabyte_hours,
        cast(NULL as double) as cluster_capacity_cpu_core_hours,
        cast(NULL as double) as cluster_capacity_memory_gigabyte_hours,
        cast(NULL as varchar) as pod_labels,
        cast(NULL as varchar) as volume_labels,
        max(azure.tags) as tags,
        max(azure.resource_id_matched) as resource_id_matched,
        max(ocp.cost_category_id) as cost_category_id,
        {{ocp_source_uuid}} as ocp_source,
        max(azure.year) as year,
        max(azure.month) as month
    FROM hive.{{schema | sqlsafe}}.reporting_ocpusagelineitem_daily_summary as ocp
    JOIN hive.{{schema | sqlsafe}}.azure_openshift_daily_resource_matched_temp as azure
        ON (azure.usage_start = ocp.usage_start)
        AND (
                (strpos(azure.resource_id, ocp.persistentvolume) > 0 AND ocp.data_source = 'Storage')
            OR
                (lower(ocp.csi_volume_handle) = lower(azure.resource_id))
            )
    JOIN cte_az_resource_to_disk_capacity AS az_disk
        ON az_disk.usage_start = azure.usage_start
        AND az_disk.resource_id = azure.resource_id
    LEFT JOIN cte_total_pv_capacity as pv_cap
        ON 1 = 1
    WHERE ocp.source = {{ocp_source_uuid}}
        AND ocp.year = {{year}}
        AND lpad(ocp.month, 2, '0') = {{month}}
        AND ocp.usage_start >= {{start_date}}
        AND ocp.usage_start < date_add('day', 1, {{end_date}})
        AND (ocp.resource_id IS NOT NULL AND ocp.resource_id != '')
        AND azure.ocp_source = {{ocp_source_uuid}}
        AND azure.year = {{year}}
        AND azure.month = {{month}}
    GROUP BY azure.uuid, ocp.data_source, azure.resource_id
    UNION
    SELECT azure.uuid as azure_uuid,
        max(ocp.cluster_id) as cluster_id,
        max(ocp.cluster_alias) as cluster_alias,
        ocp.data_source,
        ocp.namespace as namespace,
        max(ocp.node) as node,
        max(persistentvolumeclaim) as persistentvolumeclaim,
        max(persistentvolume) as persistentvolume,
        max(storageclass) as storageclass,
        max(azure.resource_id) as resource_id,
        max(azure.usage_start) as usage_start,
        max(azure.usage_start) as usage_end,
        max(nullif(azure.service_name, '')) as service_name,
        max(azure.instance_type) as instance_type,
        max(azure.subscription_guid) as subscription_guid,
        max(azure.subscription_name) as subscription_name,
        max(nullif(azure.resource_location, '')) as resource_location,
        max(azure.unit_of_measure) as unit_of_measure,
        max(cast(azure.usage_quantity as decimal(24,9))) as usage_quantity,
        max(azure.currency) as currency,
        max(persistentvolumeclaim_capacity_gigabyte) / max(az_disk.capacity) * max(cast(azure.pretax_cost as decimal(24,9)))  as pretax_cost,
        (max(persistentvolumeclaim_capacity_gigabyte) / max(az_disk.capacity) * max(cast(azure.pretax_cost as decimal(24,9)))) * cast({{markup}} as decimal(24,9)) as markup_cost, -- pretax_cost x markup = markup_cost
        cast(NULL as double) as pod_cost,
        cast(NULL as double) as project_markup_cost,
        cast(NULL as double) as pod_usage_cpu_core_hours,
        cast(NULL as double) as pod_request_cpu_core_hours,
        cast(NULL as double) as pod_effective_usage_cpu_core_hours,
        cast(NULL as double) as pod_limit_cpu_core_hours,
        cast(NULL as double) as pod_usage_memory_gigabyte_hours,
        cast(NULL as double) as pod_request_memory_gigabyte_hours,
        cast(NULL as double) as pod_effective_usage_memory_gigabyte_hours,
        cast(NULL as double) as node_capacity_cpu_core_hours,
        cast(NULL as double) as node_capacity_memory_gigabyte_hours,
        cast(NULL as double) as cluster_capacity_cpu_core_hours,
        cast(NULL as double) as cluster_capacity_memory_gigabyte_hours,
        cast(NULL as varchar) as pod_labels,
        ocp.volume_labels as volume_labels,
        max(azure.tags) as tags,
        max(azure.resource_id_matched) as resource_id_matched,
        max(ocp.cost_category_id) as cost_category_id,
        {{ocp_source_uuid}} as ocp_source,
        max(azure.year) as year,
        max(azure.month) as month
    FROM hive.{{schema | sqlsafe}}.reporting_ocpusagelineitem_daily_summary as ocp
    JOIN hive.{{schema | sqlsafe}}.azure_openshift_daily_resource_matched_temp as azure
        ON (azure.usage_start = ocp.usage_start)
        AND (
                (strpos(azure.resource_id, ocp.persistentvolume) > 0 AND ocp.data_source = 'Storage')
            OR
                (lower(ocp.csi_volume_handle) = lower(azure.resource_id))
            )
    JOIN cte_az_resource_to_disk_capacity AS az_disk
        ON az_disk.usage_start = azure.usage_start
        AND az_disk.resource_id = azure.resource_id
    WHERE ocp.source = {{ocp_source_uuid}}
        AND ocp.year = {{year}}
        AND lpad(ocp.month, 2, '0') = {{month}}
        AND ocp.usage_start >= {{start_date}}
        AND ocp.usage_start < date_add('day', 1, {{end_date}})
        AND (ocp.resource_id IS NOT NULL AND ocp.resource_id != '')
        AND azure.ocp_source = {{ocp_source_uuid}}
        AND azure.year = {{year}}
        AND azure.month = {{month}}
    GROUP BY azure.uuid, ocp.namespace, ocp.data_source, ocp.pod_labels, ocp.volume_labels
) as disk_cost
;

-- Directly Pod resource_id matching
INSERT INTO hive.{{schema | sqlsafe}}.reporting_ocpazurecostlineitem_project_daily_summary_temp (
    azure_uuid,
    cluster_id,
    cluster_alias,
    data_source,
    namespace,
    node,
    persistentvolumeclaim,
    persistentvolume,
    storageclass,
    resource_id,
    usage_start,
    usage_end,
    service_name,
    instance_type,
    subscription_guid,
    subscription_name,
    resource_location,
    unit_of_measure,
    usage_quantity,
    currency,
    pretax_cost,
    markup_cost,
    pod_cost,
    project_markup_cost,
    pod_usage_cpu_core_hours,
    pod_request_cpu_core_hours,
    pod_effective_usage_cpu_core_hours,
    pod_limit_cpu_core_hours,
    pod_usage_memory_gigabyte_hours,
    pod_request_memory_gigabyte_hours,
    pod_effective_usage_memory_gigabyte_hours,
    node_capacity_cpu_core_hours,
    node_capacity_memory_gigabyte_hours,
    cluster_capacity_cpu_core_hours,
    cluster_capacity_memory_gigabyte_hours,
    pod_labels,
    volume_labels,
    tags,
    resource_id_matched,
    cost_category_id,
    ocp_source,
    year,
    month
)
SELECT azure.uuid as azure_uuid,
    max(ocp.cluster_id) as cluster_id,
    max(ocp.cluster_alias) as cluster_alias,
    ocp.data_source,
    ocp.namespace as namespace,
    max(ocp.node) as node,
    max(persistentvolumeclaim) as persistentvolumeclaim,
    max(persistentvolume) as persistentvolume,
    max(storageclass) as storageclass,
    max(azure.resource_id) as resource_id,
    max(azure.usage_start) as usage_start,
    max(azure.usage_start) as usage_end,
    max(nullif(azure.service_name, '')) as service_name,
    max(azure.instance_type) as instance_type,
    max(azure.subscription_guid) as subscription_guid,
    max(azure.subscription_name) as subscription_name,
    max(nullif(azure.resource_location, '')) as resource_location,
    max(azure.unit_of_measure) as unit_of_measure,
    max(cast(azure.usage_quantity as decimal(24,9))) as usage_quantity,
    max(azure.currency) as currency,
    max(cast(azure.pretax_cost as decimal(24,9))) as pretax_cost,
    max(cast(azure.pretax_cost as decimal(24,9))) * cast({{markup}} as decimal(24,9)) as markup_cost, -- pretax_cost x markup = markup_cost
    cast(NULL as double) as pod_cost,
    cast(NULL as double) as project_markup_cost,
    sum(ocp.pod_usage_cpu_core_hours) as pod_usage_cpu_core_hours,
    sum(ocp.pod_request_cpu_core_hours) as pod_request_cpu_core_hours,
    sum(ocp.pod_effective_usage_cpu_core_hours) as pod_effective_usage_cpu_core_hours,
    sum(ocp.pod_limit_cpu_core_hours) as pod_limit_cpu_core_hours,
    sum(ocp.pod_usage_memory_gigabyte_hours) as pod_usage_memory_gigabyte_hours,
    sum(ocp.pod_request_memory_gigabyte_hours) as pod_request_memory_gigabyte_hours,
    sum(ocp.pod_effective_usage_memory_gigabyte_hours) as pod_effective_usage_memory_gigabyte_hours,
    max(ocp.node_capacity_cpu_core_hours) as node_capacity_cpu_core_hours,
    max(ocp.node_capacity_memory_gigabyte_hours) as node_capacity_memory_gigabyte_hours,
    max(ocp.cluster_capacity_cpu_core_hours) as cluster_capacity_cpu_core_hours,
    max(ocp.cluster_capacity_memory_gigabyte_hours) as cluster_capacity_memory_gigabyte_hours,
    ocp.pod_labels,
    ocp.volume_labels,
    max(azure.tags) as tags,
    max(azure.resource_id_matched) as resource_id_matched,
    max(ocp.cost_category_id) as cost_category_id,
    {{ocp_source_uuid}} as ocp_source,
    max(azure.year) as year,
    max(azure.month) as month
    FROM hive.{{schema | sqlsafe}}.reporting_ocpusagelineitem_daily_summary as ocp
    JOIN hive.{{schema | sqlsafe}}.azure_openshift_daily_resource_matched_temp as azure
        ON (azure.usage_start = ocp.usage_start)
        AND (
                (replace(lower(azure.resource_id), '_osdisk', '') = lower(ocp.node) AND ocp.data_source = 'Pod')
            )
    WHERE ocp.source = {{ocp_source_uuid}}
        AND ocp.year = {{year}}
        AND lpad(ocp.month, 2, '0') = {{month}} -- Zero pad the month when fewer than 2 characters
        AND ocp.usage_start >= {{start_date}}
        AND ocp.usage_start < date_add('day', 1, {{end_date}})
        AND (ocp.resource_id IS NOT NULL AND ocp.resource_id != '') -- This excludes claimless PVs
        -- Filter out Node Network Costs because they cannot be tied to namespace level
        AND azure.data_transfer_direction IS NULL
        AND azure.ocp_source = {{ocp_source_uuid}}
        AND azure.year = {{year}}
        AND azure.month = {{month}}
    GROUP BY azure.uuid, ocp.namespace, ocp.data_source, ocp.pod_labels, ocp.volume_labels
;

-- Tag matching
INSERT INTO hive.{{schema | sqlsafe}}.reporting_ocpazurecostlineitem_project_daily_summary_temp (
    azure_uuid,
    cluster_id,
    cluster_alias,
    data_source,
    namespace,
    node,
    persistentvolumeclaim,
    persistentvolume,
    storageclass,
    resource_id,
    usage_start,
    usage_end,
    service_name,
    instance_type,
    subscription_guid,
    subscription_name,
    resource_location,
    unit_of_measure,
    usage_quantity,
    currency,
    pretax_cost,
    markup_cost,
    pod_cost,
    project_markup_cost,
    pod_usage_cpu_core_hours,
    pod_request_cpu_core_hours,
    pod_effective_usage_cpu_core_hours,
    pod_limit_cpu_core_hours,
    pod_usage_memory_gigabyte_hours,
    pod_request_memory_gigabyte_hours,
    pod_effective_usage_memory_gigabyte_hours,
    node_capacity_cpu_core_hours,
    node_capacity_memory_gigabyte_hours,
    cluster_capacity_cpu_core_hours,
    cluster_capacity_memory_gigabyte_hours,
    pod_labels,
    volume_labels,
    tags,
    resource_id_matched,
    cost_category_id,
    ocp_source,
    year,
    month
)
SELECT azure.uuid as azure_uuid,
    max(ocp.cluster_id) as cluster_id,
    max(ocp.cluster_alias) as cluster_alias,
    ocp.data_source,
    ocp.namespace,
    max(ocp.node) as node,
    max(nullif(ocp.persistentvolumeclaim, '')) as persistentvolumeclaim,
    max(nullif(ocp.persistentvolume, '')) as persistentvolume,
    max(nullif(ocp.storageclass, '')) as storageclass,
    max(azure.resource_id) as resource_id,
    max(azure.usage_start) as usage_start,
    max(azure.usage_start) as usage_end,
    max(nullif(azure.service_name, '')) as service_name,
    max(azure.instance_type) as instance_type,
    max(azure.subscription_guid) as subscription_guid,
    max(subscription_name) as subscription_name,
    max(nullif(azure.resource_location, '')) as resource_location,
    max(azure.unit_of_measure) as unit_of_measure,
    max(cast(azure.usage_quantity as decimal(24,9))) as usage_quantity,
    max(azure.currency) as currency,
    max(cast(azure.pretax_cost as decimal(24,9))) as pretax_cost,
    max(cast(azure.pretax_cost as decimal(24,9))) * cast({{markup}} as decimal(24,9)) as markup_cost, -- pretax_cost x markup = markup_cost
    cast(NULL as double) as pod_cost,
    cast(NULL as double) as project_markup_cost,
    cast(NULL as double) as pod_usage_cpu_core_hours,
    cast(NULL as double) as pod_request_cpu_core_hours,
    cast(NULL as double) as pod_effective_usage_cpu_core_hours,
    cast(NULL as double) as pod_limit_cpu_core_hours,
    cast(NULL as double) as pod_usage_memory_gigabyte_hours,
    cast(NULL as double) as pod_request_memory_gigabyte_hours,
    cast(NULL as double) as pod_effective_usage_memory_gigabyte_hours,
    cast(NULL as double) as node_capacity_cpu_core_hours,
    cast(NULL as double) as node_capacity_memory_gigabyte_hours,
    cast(NULL as double) as cluster_capacity_cpu_core_hours,
    cast(NULL as double) as cluster_capacity_memory_gigabyte_hours,
    max(ocp.pod_labels) as pod_labels,
    max(ocp.volume_labels) as volume_labels,
    max(azure.tags) as tags,
    FALSE as resource_id_matched,
    max(ocp.cost_category_id) as cost_category_id,
    {{ocp_source_uuid}} as ocp_source,
    max(azure.year) as year,
    max(azure.month) as month
    FROM hive.{{schema | sqlsafe}}.reporting_ocpusagelineitem_daily_summary as ocp
    JOIN hive.{{schema | sqlsafe}}.azure_openshift_daily_tag_matched_temp as azure
        ON azure.usage_start = ocp.usage_start
        AND (
                json_query(azure.tags, 'strict $.openshift_project' OMIT QUOTES) = ocp.namespace
                OR json_query(azure.tags, 'strict $.openshift_node' OMIT QUOTES) = ocp.node
                OR json_query(azure.tags, 'strict $.openshift_cluster' OMIT QUOTES) = ocp.cluster_alias
                OR json_query(azure.tags, 'strict $.openshift_cluster' OMIT QUOTES) = ocp.cluster_id
                OR (azure.matched_tag != '' AND any_match(split(azure.matched_tag, ','), x->strpos(ocp.pod_labels, replace(x, ' ')) != 0))
                OR (azure.matched_tag != '' AND any_match(split(azure.matched_tag, ','), x->strpos(ocp.volume_labels, replace(x, ' ')) != 0))
            )
        AND namespace != 'Worker unallocated'
        AND namespace != 'Platform unallocated'
        AND namespace != 'Storage unattributed'
        AND namespace != 'Network unattributed'
    WHERE ocp.source = {{ocp_source_uuid}}
        AND ocp.year = {{year}}
        AND lpad(ocp.month, 2, '0') = {{month}} -- Zero pad the month when fewer than 2 characters
        AND ocp.usage_start >= {{start_date}}
        AND ocp.usage_start < date_add('day', 1, {{end_date}})
        AND azure.ocp_source = {{ocp_source_uuid}}
        AND azure.year = {{year}}
        AND azure.month = {{month}}
    GROUP BY azure.uuid, ocp.namespace, ocp.data_source
;

-- Group by to calculate proper cost per project

INSERT INTO hive.{{schema | sqlsafe}}.reporting_ocpazurecostlineitem_project_daily_summary (
    azure_uuid,
    cluster_id,
    cluster_alias,
    data_source,
    namespace,
    node,
    persistentvolumeclaim,
    persistentvolume,
    storageclass,
    resource_id,
    usage_start,
    usage_end,
    service_name,
    data_transfer_direction,
    instance_type,
    subscription_guid,
    subscription_name,
    resource_location,
    unit_of_measure,
    usage_quantity,
    currency,
    pretax_cost,
    markup_cost,
    pod_cost,
    project_markup_cost,
    pod_labels,
    tags,
    cost_category_id,
    azure_source,
    ocp_source,
    year,
    month,
    day
)
WITH cte_rankings AS (
    SELECT pds.azure_uuid,
        count(*) as azure_uuid_count
    FROM hive.{{schema | sqlsafe}}.reporting_ocpazurecostlineitem_project_daily_summary_temp AS pds
    GROUP BY azure_uuid
)
SELECT pds.azure_uuid,
    cluster_id,
    cluster_alias,
    data_source,
    namespace,
    node,
    persistentvolumeclaim,
    persistentvolume,
    storageclass,
    resource_id,
    usage_start,
    usage_end,
    service_name,
    NULL as data_transfer_direction,
    instance_type,
    subscription_guid,
    subscription_name,
    resource_location,
    unit_of_measure,
    usage_quantity / r.azure_uuid_count as usage_quantity,
    currency,
    CASE WHEN resource_id_matched = TRUE AND data_source = 'Pod'
        THEN ({{pod_column | sqlsafe}} / {{node_column | sqlsafe}}) * pretax_cost
        ELSE pretax_cost / r.azure_uuid_count
    END as pretax_cost,
    CASE WHEN resource_id_matched = TRUE AND data_source = 'Pod'
        THEN ({{pod_column | sqlsafe}} / {{node_column | sqlsafe}}) * pretax_cost * cast({{markup}} as decimal(24,9))
        ELSE pretax_cost / r.azure_uuid_count * cast({{markup}} as decimal(24,9))
    END as markup_cost,
    CASE WHEN resource_id_matched = TRUE AND data_source = 'Pod'
        THEN ({{pod_column | sqlsafe}} / {{node_column | sqlsafe}}) * pretax_cost
        ELSE pretax_cost / r.azure_uuid_count
    END as pod_cost,
    CASE WHEN resource_id_matched = TRUE AND data_source = 'Pod'
        THEN ({{pod_column | sqlsafe}} / {{node_column | sqlsafe}}) * pretax_cost * cast({{markup}} as decimal(24,9))
        ELSE pretax_cost / r.azure_uuid_count * cast({{markup}} as decimal(24,9))
    END as project_markup_cost,
    pds.pod_labels,
    CASE WHEN pds.pod_labels IS NOT NULL
        THEN json_format(cast(
            map_concat(
                cast(json_parse(pds.pod_labels) as map(varchar, varchar)),
                cast(json_parse(pds.tags) as map(varchar, varchar))
            ) as JSON))
        ELSE json_format(cast(
            map_concat(
                cast(json_parse(pds.volume_labels) as map(varchar, varchar)),
                cast(json_parse(pds.tags) as map(varchar, varchar))
            ) as JSON))
    END as tags,
    cost_category_id,
    {{azure_source_uuid}} as azure_source,
    {{ocp_source_uuid}} as ocp_source,
    cast(year(usage_start) as varchar) as year,
    cast(month(usage_start) as varchar) as month,
    cast(day(usage_start) as varchar) as day
FROM hive.{{schema | sqlsafe}}.reporting_ocpazurecostlineitem_project_daily_summary_temp AS pds
JOIN cte_rankings as r
    ON pds.azure_uuid = r.azure_uuid
WHERE pds.ocp_source = {{ocp_source_uuid}} AND pds.year = {{year}} AND pds.month = {{month}}
;

-- Network costs are currently not mapped to pod metrics
-- and are filtered out of the above SQL since that is grouped by namespace
-- and costs are split out by pod metrics, this puts all network costs per node
-- into a "Network unattributed" project with no cost split and one record per
-- data direction
INSERT INTO hive.{{schema | sqlsafe}}.reporting_ocpazurecostlineitem_project_daily_summary (
    azure_uuid,
    cluster_id,
    cluster_alias,
    data_source,
    namespace,
    node,
    persistentvolumeclaim,
    persistentvolume,
    storageclass,
    resource_id,
    usage_start,
    usage_end,
    service_name,
    data_transfer_direction,
    instance_type,
    subscription_guid,
    subscription_name,
    resource_location,
    unit_of_measure,
    usage_quantity,
    currency,
    pretax_cost,
    markup_cost,
    pod_cost,
    project_markup_cost,
    tags,
    cost_category_id,
    azure_source,
    ocp_source,
    year,
    month,
    day
)
SELECT azure.uuid as azure_uuid,
    max(ocp.cluster_id) as cluster_id,
    max(ocp.cluster_alias) as cluster_alias,
    max(ocp.data_source) as data_source,
    'Network unattributed' as namespace,
    ocp.node as node,
    max(nullif(ocp.persistentvolumeclaim, '')) as persistentvolumeclaim,
    max(nullif(ocp.persistentvolume, '')) as persistentvolume,
    max(nullif(ocp.storageclass, '')) as storageclass,
    max(azure.resource_id) as resource_id,
    max(azure.usage_start) as usage_start,
    max(azure.usage_start) as usage_end,
    max(nullif(azure.service_name, '')) as service_name,
    max(data_transfer_direction) as data_transfer_direction,
    max(azure.instance_type) as instance_type,
    max(azure.subscription_guid) as subscription_guid,
    max(azure.subscription_name) as subscription_name,
    max(nullif(azure.resource_location, '')) as resource_location,
    max(azure.unit_of_measure) as unit_of_measure,
    max(cast(azure.usage_quantity as decimal(24,9))) as usage_quantity,
    max(azure.currency) as currency,
    max(cast(azure.pretax_cost as decimal(24,9))) as pretax_cost,
    max(cast(azure.pretax_cost as decimal(24,9))) * cast({{markup}} as decimal(24,9)) as markup_cost, -- pretax_cost x markup = markup_cost
    max(cast(azure.pretax_cost as decimal(24,9))) as pod_cost,
    max(cast(azure.pretax_cost as decimal(24,9))) * cast({{markup}} as decimal(24,9)) as project_markup_cost,
    max(azure.tags) as tags,
    max(ocp.cost_category_id) as cost_category_id,
    {{azure_source_uuid}} as azure_source,
    {{ocp_source_uuid}} as ocp_source,
    max(azure.year) as year,
    max(azure.month) as month,
    cast(day(max(azure.usage_start)) as varchar) as day
    FROM hive.{{schema | sqlsafe}}.reporting_ocpusagelineitem_daily_summary as ocp
    JOIN hive.{{schema | sqlsafe}}.azure_openshift_daily_resource_matched_temp as azure
        ON azure.usage_start = ocp.usage_start
        AND (azure.resource_id = ocp.node AND ocp.data_source = 'Pod')
    WHERE ocp.source = {{ocp_source_uuid}}
        AND ocp.year = {{year}}
        AND lpad(ocp.month, 2, '0') = {{month}} -- Zero pad the month when fewer than 2 characters
        AND ocp.usage_start >= {{start_date}}
        AND ocp.usage_start < date_add('day', 1, {{end_date}})
        AND (ocp.resource_id IS NOT NULL AND ocp.resource_id != '')
        -- Filter for Node Network Costs to tie them to the Network unattributed project
        AND azure.data_transfer_direction IS NOT NULL
        AND azure.ocp_source = {{ocp_source_uuid}}
        AND azure.year = {{year}}
        AND azure.month = {{month}}
    GROUP BY azure.uuid, ocp.node
;

INSERT INTO postgres.{{schema | sqlsafe}}.reporting_ocpazurecostlineitem_project_daily_summary_p (
    uuid,
    report_period_id,
    cluster_id,
    cluster_alias,
    data_source,
    namespace,
    node,
    persistentvolumeclaim,
    persistentvolume,
    storageclass,
    pod_labels,
    resource_id,
    usage_start,
    usage_end,
    cost_entry_bill_id,
    subscription_guid,
    subscription_name,
    instance_type,
    service_name,
    infrastructure_data_in_gigabytes,
    infrastructure_data_out_gigabytes,
    data_transfer_direction,
    resource_location,
    usage_quantity,
    unit_of_measure,
    currency,
    pretax_cost,
    markup_cost,
    project_markup_cost,
    pod_cost,
    tags,
    cost_category_id,
    source_uuid
)
SELECT uuid(),
    {{report_period_id | sqlsafe}} as report_period_id,
    cluster_id as cluster_id,
    cluster_alias as cluster_alias,
    data_source,
    namespace,
    node,
    persistentvolumeclaim,
    persistentvolume,
    storageclass,
    json_parse(pod_labels),
    resource_id,
    date(usage_start),
    date(usage_end),
    {{bill_id | sqlsafe}} as cost_entry_bill_id,
    subscription_guid,
    subscription_name,
    instance_type,
    service_name,
    CASE
        WHEN lower(data_transfer_direction) = 'datatrin' THEN usage_quantity
        ELSE 0
    END as infrastructure_data_in_gigabytes,
    CASE
        WHEN lower(data_transfer_direction) = 'datatrout' THEN usage_quantity
        ELSE 0
    END as infrastructure_data_out_gigabytes,
    -- gives each row a unique identifier for group by during back populate
    CASE
        WHEN lower(data_transfer_direction) = 'datatrin' THEN 'IN'
        WHEN lower(data_transfer_direction) = 'datatrout' THEN 'OUT'
        ELSE NULL
    END as data_transfer_direction,
    resource_location,
    usage_quantity,
    unit_of_measure,
    currency,
    pretax_cost,
    markup_cost,
    project_markup_cost,
    pod_cost,
    json_parse(tags),
    cost_category_id,
    cast(azure_source as UUID)
FROM hive.{{schema | sqlsafe}}.reporting_ocpazurecostlineitem_project_daily_summary
WHERE azure_source = {{azure_source_uuid}}
    AND ocp_source = {{ocp_source_uuid}}
    AND year = {{year}}
    AND lpad(month, 2, '0') = {{month}} -- Zero pad the month when fewer than 2 characters
    AND day in {{days | inclause}}
;
