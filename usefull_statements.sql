SELECT
  count(hashrequest.id)                      AS Requests,
  sum(hashrequest.size) / 1024 / 1024 / 1024 AS Size,
  host.name                                  AS Host
FROM host
  JOIN hashrequest ON hashrequest.host_id = host.id
--JOIN file ON file.host_id = host.id
  GROUP BY host.name;

--

SELECT
  count(file.id)                      AS Files,
  sum(file.size) / 1024 / 1024 / 1024 AS Size,
  host.name                           AS Host
FROM file
  JOIN host ON file.host_id = host.id
GROUP BY host.name;

