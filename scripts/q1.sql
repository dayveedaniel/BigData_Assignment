
WITH aggregated_campaign AS (
   SELECT 
      c.campaign_id,
      c.campaign_type,
      c.channel,
      COUNT(DISTINCT ud.user_id) AS users_received,
      COUNT(DISTINCT CASE WHEN m.purchased_at IS NOT NULL THEN ud.user_id END) AS users_purchased,
      ROUND(
            (COUNT(DISTINCT CASE WHEN m.purchased_at IS NOT NULL THEN ud.user_id END)::numeric 
            / NULLIF(COUNT(DISTINCT ud.user_id), 0)) * 100, 
            2
      ) AS purchase_percentage
   FROM ecommerce.Messages m
   JOIN ecommerce.UserDevices ud ON m.client_id = ud.client_id
   JOIN ecommerce.Campaigns c ON m.campaign_type_id = c.campaign_type_id
   GROUP BY c.campaign_id, c.campaign_type, c.channel
)
SELECT * FROM aggregated_campaign
ORDER BY purchase_percentage DESC;