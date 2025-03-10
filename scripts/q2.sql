WITH client_events AS (
    SELECT 
        ud.client_id, 
        e.product_id, 
        COUNT(*) AS event_count
    FROM ecommerce.Events e
    JOIN ecommerce.UserDevices ud 
      ON e.user_id = ud.user_id
    WHERE e.event_type IN ('view', 'purchase')
    GROUP BY ud.client_id, e.product_id
),
ranked_events AS (
    SELECT 
        client_id, 
        product_id, 
        event_count,
        ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY event_count DESC) AS rn
    FROM client_events
)
SELECT 
    re.client_id, 
    re.product_id, 
    p.category_id, 
    c.category_code, 
    re.event_count
FROM ranked_events re
JOIN ecommerce.Products p 
  ON re.product_id = p.product_id
JOIN ecommerce.Categories c 
  ON p.category_id = c.category_id
WHERE re.rn <= 5;
