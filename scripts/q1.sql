WITH campaign_info AS (
   SELECT 
     c.campaign_id,
     c.campaign_type,
     m.message_id,
     m.client_id,
     ud.user_id,
     ud.platform,
     ud.stream,
     m.sent_at,
     m.purchased_at
   FROM ecommerce.Messages m
   JOIN ecommerce.UserDevices ud ON m.client_id = ud.client_id
   LEFT JOIN ecommerce.Campaigns c ON m.campaign_type_id = c.campaign_type_id
),
campaign_stats AS (
   SELECT 
      campaign_id,
      campaign_type,
      COUNT(DISTINCT user_id) AS users_received,
      COUNT(DISTINCT CASE WHEN purchased_at IS NOT NULL THEN user_id END) AS users_purchased,
      ROUND(
         (COUNT(DISTINCT CASE WHEN purchased_at IS NOT NULL THEN user_id END)::numeric 
          / NULLIF(COUNT(DISTINCT user_id),0)) * 100, 2) AS purchase_rate_percentage,
      json_agg(json_build_object(
         'message_id', message_id,
         'platform', platform,
         'stream', stream,
         'sent_at', sent_at
      )) AS message_details
   FROM campaign_info
   GROUP BY campaign_id, campaign_type
)
SELECT *
FROM campaign_stats
ORDER BY purchase_rate_percentage DESC;
