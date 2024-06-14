INSERT INTO public.delayed_celery_tasks (
    id,
    task_name,
    task_args,
    task_kwargs,
    timeout_timestamp,
    provider_uuid,
    queue_name,
    metadata
) VALUES (
    3,
    'masu.celery.tasks.cost_verification',
    '["org1234567", "ca4f603c-1ece-4ef7-925f-e472a51494a3"]',
    '{"class_name": "VerifyUnattributedStorage", "tracing_id": "4c671309-2385-442e-a4e3-a326e48d3e47", "module_path": "masu.verification.unattributed_storage"}',
    '2024-06-06 15:15:03.287129+00',
    'ca4f603c-1ece-4ef7-925f-e472a51494a3',
    'summary',
    '{}'
);
