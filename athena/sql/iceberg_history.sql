SELECT
    h.snapshot_id,
    h.made_current_at AS committed_at,
    s.parent_id,
    s.operation,
    s.summary
FROM "gold"."fct_trips$history" h
JOIN "gold"."fct_trips$snapshots" s ON h.snapshot_id = s.snapshot_id
ORDER BY h.made_current_at DESC
LIMIT 100;
