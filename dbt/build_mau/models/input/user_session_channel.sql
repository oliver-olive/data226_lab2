WITH CET AS
(
SELECT
userId,
sessionId,
channel
FROM {{source('raw','user_session_channel')}}
WHERE sessionId is not NULL
)
SELECT * FROM CET
