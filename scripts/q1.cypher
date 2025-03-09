MATCH (c:Campaign)-[:SENT_MESSAGE]->(m:Message)
OPTIONAL MATCH (u:User)-[:RECEIVED]->(m)
WITH c, 
     collect(DISTINCT u.user_id) AS allUsers,
     collect(DISTINCT CASE WHEN m.purchased_at IS NOT NULL THEN u.user_id END) AS purchasedUsers
RETURN 
    c.campaign_id AS campaign_id,
    c.campaign_type AS campaign_type,
    c.channel AS channel,
    size(allUsers) AS users_received,
    size(purchasedUsers) AS users_purchased,
    CASE 
       WHEN size(allUsers) > 0 THEN round(100.0 * size(purchasedUsers) / size(allUsers), 2)
       ELSE 0 
    END AS purchase_percentage
ORDER BY purchase_percentage DESC
