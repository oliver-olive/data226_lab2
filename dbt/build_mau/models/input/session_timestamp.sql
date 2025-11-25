WITH CET AS
(SELECT
sessionId,
ts
FROM {{source('raw', 'session_timestamp')}}
WHERE sessionId is not NULL
)
SELECT * FROM CET
