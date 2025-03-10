// DROP CONSTRAINT user_id_unique IF EXISTS;
// DROP CONSTRAINT product_id_unique IF EXISTS;
// DROP CONSTRAINT campaign_type_id_unique IF EXISTS;
// DROP CONSTRAINT message_id_unique IF EXISTS;

// CREATE CONSTRAINT user_id_unique
//   FOR (u:User)
//   REQUIRE u.user_id IS UNIQUE;
// CREATE CONSTRAINT product_id_unique
//   FOR (p:Product)
//   REQUIRE p.product_id IS UNIQUE;
// CREATE CONSTRAINT campaign_type_id_unique
//   FOR (c:Campaign)
//   REQUIRE c.campaign_type_id IS UNIQUE;
// CREATE CONSTRAINT message_id_unique
//   FOR (m:Message)
//   REQUIRE m.message_id IS UNIQUE;

// // ----- Node Creation (reduced transaction sizes and optimized) -----
// // Create Users
// LOAD CSV WITH HEADERS FROM 'file:///cleaned_data/Users.csv' AS row
// FIELDTERMINATOR ','
// MERGE (u:User { user_id: toInteger(row.user_id) })
// WITH count(*) AS dummy
// RETURN "Users loaded: " + dummy + " rows" AS result;

// // Create Categories
// LOAD CSV WITH HEADERS FROM 'file:///cleaned_data/Categories.csv' AS row
// FIELDTERMINATOR ','
// MERGE (c:Category { category_id: toInteger(row.category_id) })
// ON CREATE SET c.category_code = row.category_code
// WITH count(*) AS dummy
// RETURN "Categories loaded: " + dummy + " rows" AS result;

// // Create Products
// LOAD CSV WITH HEADERS FROM 'file:///cleaned_data/Products.csv' AS row
// FIELDTERMINATOR ','
// MERGE (p:Product { product_id: toInteger(row.product_id) })
// ON CREATE SET 
//     p.brand = row.brand,
//     p.composite_id = row.id,
//     p.category_id = toInteger(row.category_id)
// WITH count(*) AS dummy
// RETURN "Products loaded: " + dummy + " rows" AS result;

// // Create Campaigns
// LOAD CSV WITH HEADERS FROM 'file:///cleaned_data/Campaigns.csv' AS row
// FIELDTERMINATOR ','
// MERGE (c:Campaign { campaign_type_id: row.campaign_type_id })
// ON CREATE SET 
//     c.campaign_id  = toInteger(row.campaign_id),
//     c.campaign_type = row.campaign_type,
//     c.channel       = row.channel,
//     c.topic         = CASE WHEN row.topic <> "" THEN row.topic ELSE null END,
//     c.position      = CASE WHEN row.position <> "" THEN toInteger(row.position) ELSE null END
// WITH count(*) AS dummy
// RETURN "Campaigns loaded: " + dummy + " rows" AS result;

// // Create Messages (smaller transaction size)
// CALL {
//   LOAD CSV WITH HEADERS FROM 'file:///cleaned_data/Messages.csv' AS row
//   FIELDTERMINATOR ','
//   CREATE (m:Message {
//     message_id: row.message_id,
//     sent_at: CASE WHEN row.sent_at <> "" THEN datetime(replace(row.sent_at, ' ', 'T')) ELSE null END,
//     opened_first_time_at: CASE WHEN row.opened_first_time_at <> "" THEN datetime(replace(row.opened_first_time_at, ' ', 'T')) ELSE null END,
//     opened_last_time_at: CASE WHEN row.opened_last_time_at <> "" THEN datetime(replace(row.opened_last_time_at, ' ', 'T')) ELSE null END,
//     clicked_first_time_at: CASE WHEN row.clicked_first_time_at <> "" THEN datetime(replace(row.clicked_first_time_at, ' ', 'T')) ELSE null END,
//     clicked_last_time_at: CASE WHEN row.clicked_last_time_at <> "" THEN datetime(replace(row.clicked_last_time_at, ' ', 'T')) ELSE null END,
//     unsubscribed_at: CASE WHEN row.unsubscribed_at <> "" THEN datetime(replace(row.unsubscribed_at, ' ', 'T')) ELSE null END,
//     hard_bounced_at: CASE WHEN row.hard_bounced_at <> "" THEN datetime(replace(row.hard_bounced_at, ' ', 'T')) ELSE null END,
//     soft_bounced_at: CASE WHEN row.soft_bounced_at <> "" THEN datetime(replace(row.soft_bounced_at, ' ', 'T')) ELSE null END,
//     complained_at: CASE WHEN row.complained_at <> "" THEN datetime(replace(row.complained_at, ' ', 'T')) ELSE null END,
//     blocked_at: CASE WHEN row.blocked_at <> "" THEN datetime(replace(row.blocked_at, ' ', 'T')) ELSE null END,
//     purchased_at: CASE WHEN row.purchased_at <> "" THEN datetime(replace(row.purchased_at, ' ', 'T')) ELSE null END
//   })
//   RETURN count(*) AS dummy
// } IN TRANSACTIONS OF 10000 ROWS
// RETURN "Messages loaded";

// // Create Devices
// LOAD CSV WITH HEADERS FROM 'file:///cleaned_data/UserDevices.csv' AS dev
// FIELDTERMINATOR ','
// MERGE (d:Device { client_id: toInteger(dev.client_id) })
// ON CREATE SET d.user_id = toInteger(dev.user_id)
// WITH count(*) AS dummy
// RETURN "Devices loaded: " + dummy + " rows" AS result;

// // ----- Relationship Creation (separate from node creation) -----
// // User-User Friendship relationships
// CALL {
//   LOAD CSV WITH HEADERS FROM 'file:///cleaned_data/UserFriends.csv' AS row
//   FIELDTERMINATOR ','
//   MATCH (u1:User { user_id: toInteger(row.user_id_1) })
//   MATCH (u2:User { user_id: toInteger(row.user_id_2) })
//   MERGE (u1)-[:FRIEND_WITH]->(u2)
//   MERGE (u2)-[:FRIEND_WITH]->(u1)
//   RETURN count(*) AS dummy
// } IN TRANSACTIONS OF 1000 ROWS
// RETURN "Friendship relationships created";

// // Product-Category relationships
// CALL {
//   LOAD CSV WITH HEADERS FROM 'file:///cleaned_data/Products.csv' AS row
//   FIELDTERMINATOR ','
//   MATCH (p:Product { product_id: toInteger(row.product_id) })
//   MATCH (c:Category { category_id: toInteger(row.category_id) })
//   MERGE (p)-[:BELONGS_TO]->(c)
//   RETURN count(*) AS dummy
// } IN TRANSACTIONS OF 1000 ROWS
// RETURN "Product-Category relationships created";

// // Campaign attributes (separated)
// CALL {
//   LOAD CSV WITH HEADERS FROM 'file:///cleaned_data/BulkCampaignAttributes.csv' AS row
//   FIELDTERMINATOR ','
//   MATCH (c:Campaign { campaign_type_id: row.campaign_type_id })
//   SET c.started_at  = datetime(replace(row.started_at, ' ', 'T')),
//       c.finished_at = datetime(replace(row.finished_at, ' ', 'T')),
//       c.total_count = CASE WHEN row.total_count <> "" THEN toInteger(row.total_count) ELSE null END,
//       c.ab_test     = (row.ab_test = "true"),
//       c.warmup_mode = (row.warmup_mode = "true"),
//       c.hour_limit  = CASE WHEN row.hour_limit <> "" THEN toFloat(row.hour_limit) ELSE null END,
//       c.is_test     = (row.is_test = "true")
//   RETURN count(*) AS dummy
// } IN TRANSACTIONS OF 1000 ROWS
// RETURN "Campaign bulk attributes added";

// // Campaign subject attributes
// CALL {
//   LOAD CSV WITH HEADERS FROM 'file:///cleaned_data/CampaignSubjectAttributes.csv' AS row
//   FIELDTERMINATOR ','
//   MATCH (c:Campaign { campaign_type_id: row.campaign_type_id })
//   SET c.subject_length               = CASE WHEN row.subject_length <> "" THEN toInteger(row.subject_length) ELSE null END,
//       c.subject_with_personalization = (row.subject_with_personalization = "true"),
//       c.subject_with_deadline        = (row.subject_with_deadline = "true"),
//       c.subject_with_emoji           = (row.subject_with_emoji = "true"),
//       c.subject_with_bonuses         = (row.subject_with_bonuses = "true"),
//       c.subject_with_discount        = (row.subject_with_discount = "true"),
//       c.subject_with_saleout         = (row.subject_with_saleout = "true")
//   RETURN count(*) AS dummy
// } IN TRANSACTIONS OF 1000 ROWS
// RETURN "Campaign subject attributes added";

// // Campaign-Message relationships
// CALL {
//   LOAD CSV WITH HEADERS FROM 'file:///cleaned_data/Messages.csv' AS row
//   FIELDTERMINATOR ','
//   MATCH (m:Message { message_id: row.message_id })
//   MATCH (cam:Campaign { campaign_type_id: row.campaign_type_id })
//   MERGE (cam)-[:SENT_MESSAGE]->(m)
//   RETURN count(*) AS dummy
// } IN TRANSACTIONS OF 1000 ROWS
// RETURN "Campaign-Message relationships created";

// // User-Device relationships
// CALL {
//   LOAD CSV WITH HEADERS FROM 'file:///cleaned_data/UserDevices.csv' AS dev
//   FIELDTERMINATOR ','
//   MATCH (d:Device { client_id: toInteger(dev.client_id) })
//   MATCH (u:User { user_id: toInteger(dev.user_id) })
//   MERGE (u)-[:HAS_DEVICE]->(d)
//   RETURN count(*) AS dummy
// } IN TRANSACTIONS OF 1000 ROWS
// RETURN "User-Device relationships created";

// // Device-Message relationships
// CALL {
//   LOAD CSV WITH HEADERS FROM 'file:///cleaned_data/Messages.csv' AS row
//   FIELDTERMINATOR ','
//   MATCH (m:Message { message_id: row.message_id })
//   MATCH (d:Device { client_id: toInteger(row.client_id) })
//   MERGE (d)-[:RECEIVED]->(m)
//   RETURN count(*) AS dummy
// } IN TRANSACTIONS OF 1000 ROWS
// RETURN "Device-Message relationships created";

// // User-Message relationships
// CALL {
//   LOAD CSV WITH HEADERS FROM 'file:///cleaned_data/Messages.csv' AS row
//   FIELDTERMINATOR ','
//   MATCH (d:Device { client_id: toInteger(row.client_id) })
//   MATCH (u:User { user_id: d.user_id })
//   MATCH (m:Message { message_id: row.message_id })
//   MERGE (u)-[:RECEIVED]->(m)
//   RETURN count(*) AS dummy
// } IN TRANSACTIONS OF 1000 ROWS
// RETURN "User-Message relationships created";

// // Process Events in smaller batches (potentially most memory intensive)
// CALL {
//   LOAD CSV WITH HEADERS FROM 'file:///cleaned_data/Events.csv' AS row
//   FIELDTERMINATOR ','
//   MATCH (u:User { user_id: toInteger(row.user_id) })
//   MATCH (p:Product { product_id: toInteger(row.product_id) })
//   CREATE (u)-[:PERFORMED_EVENT {
//     event_time: datetime(replace(row.event_time, ' ', 'T')),
//     event_type: row.event_type,
//     user_session: row.user_session,
//     price: toFloatOrNull(row.price)
//   }]->(p)
//   RETURN count(*) AS dummy
// } IN TRANSACTIONS OF 10000 ROWS
// RETURN "Event relationships created";

// RETURN "All Data Loaded Successfully!" AS result;


DROP CONSTRAINT user_id_unique IF EXISTS;
DROP CONSTRAINT product_id_unique IF EXISTS;
DROP CONSTRAINT campaign_type_id_unique IF EXISTS;
DROP CONSTRAINT message_id_unique IF EXISTS;

CREATE CONSTRAINT user_id_unique
  FOR (u:User)
  REQUIRE u.user_id IS UNIQUE;
CREATE CONSTRAINT product_id_unique
  FOR (p:Product)
  REQUIRE p.product_id IS UNIQUE;
CREATE CONSTRAINT campaign_type_id_unique
  FOR (c:Campaign)
  REQUIRE c.campaign_type_id IS UNIQUE;
CREATE CONSTRAINT message_id_unique
  FOR (m:Message)
  REQUIRE m.message_id IS UNIQUE;

// ----- Node Creation (reduced transaction sizes and optimized) -----
// Create Users
LOAD CSV WITH HEADERS FROM 'file:///cleaned_data/Users.csv' AS row
FIELDTERMINATOR ','
MERGE (u:User { user_id: toInteger(row.user_id) })
WITH count(*) AS dummy
RETURN "Users loaded: " + dummy + " rows" AS result;

// Create Categories
LOAD CSV WITH HEADERS FROM 'file:///cleaned_data/Categories.csv' AS row
FIELDTERMINATOR ','
MERGE (c:Category { category_id: toInteger(row.category_id) })
ON CREATE SET c.category_code = row.category_code
WITH count(*) AS dummy
RETURN "Categories loaded: " + dummy + " rows" AS result;

// Create Products
LOAD CSV WITH HEADERS FROM 'file:///cleaned_data/Products.csv' AS row
FIELDTERMINATOR ','
MERGE (p:Product { product_id: toInteger(row.product_id) })
ON CREATE SET 
    p.brand = row.brand,
    p.composite_id = row.id,
    p.category_id = toInteger(row.category_id)
WITH count(*) AS dummy
RETURN "Products loaded: " + dummy + " rows" AS result;

// Create Campaigns
LOAD CSV WITH HEADERS FROM 'file:///cleaned_data/Campaigns.csv' AS row
FIELDTERMINATOR ','
MERGE (c:Campaign { campaign_type_id: row.campaign_type_id })
ON CREATE SET 
    c.campaign_id  = toInteger(row.campaign_id),
    c.campaign_type = row.campaign_type,
    c.channel       = row.channel,
    c.topic         = CASE WHEN row.topic <> "" THEN row.topic ELSE null END,
    c.position      = CASE WHEN row.position <> "" THEN toInteger(row.position) ELSE null END
WITH count(*) AS dummy
RETURN "Campaigns loaded: " + dummy + " rows" AS result;

// ----- Create Messages individually -----
// Process Message_1.csv
CALL {
  LOAD CSV WITH HEADERS FROM 'file:///cleaned_data/Message_1.csv' AS row
  FIELDTERMINATOR ','
  CREATE (m:Message {
    message_id: row.message_id,
    sent_at: CASE WHEN row.sent_at <> "" THEN datetime(replace(row.sent_at, ' ', 'T')) ELSE null END,
    opened_first_time_at: CASE WHEN row.opened_first_time_at <> "" THEN datetime(replace(row.opened_first_time_at, ' ', 'T')) ELSE null END,
    opened_last_time_at: CASE WHEN row.opened_last_time_at <> "" THEN datetime(replace(row.opened_last_time_at, ' ', 'T')) ELSE null END,
    clicked_first_time_at: CASE WHEN row.clicked_first_time_at <> "" THEN datetime(replace(row.clicked_first_time_at, ' ', 'T')) ELSE null END,
    clicked_last_time_at: CASE WHEN row.clicked_last_time_at <> "" THEN datetime(replace(row.clicked_last_time_at, ' ', 'T')) ELSE null END,
    unsubscribed_at: CASE WHEN row.unsubscribed_at <> "" THEN datetime(replace(row.unsubscribed_at, ' ', 'T')) ELSE null END,
    hard_bounced_at: CASE WHEN row.hard_bounced_at <> "" THEN datetime(replace(row.hard_bounced_at, ' ', 'T')) ELSE null END,
    soft_bounced_at: CASE WHEN row.soft_bounced_at <> "" THEN datetime(replace(row.soft_bounced_at, ' ', 'T')) ELSE null END,
    complained_at: CASE WHEN row.complained_at <> "" THEN datetime(replace(row.complained_at, ' ', 'T')) ELSE null END,
    blocked_at: CASE WHEN row.blocked_at <> "" THEN datetime(replace(row.blocked_at, ' ', 'T')) ELSE null END,
    purchased_at: CASE WHEN row.purchased_at <> "" THEN datetime(replace(row.purchased_at, ' ', 'T')) ELSE null END
  })
  RETURN count(*) AS dummy
} IN TRANSACTIONS OF 50 ROWS
RETURN "Messages_1 loaded" AS result;

// Process Message_2.csv
CALL {
  LOAD CSV WITH HEADERS FROM 'file:///cleaned_data/Message_2.csv' AS row
  FIELDTERMINATOR ','
  CREATE (m:Message {
    message_id: row.message_id,
    sent_at: CASE WHEN row.sent_at <> "" THEN datetime(replace(row.sent_at, ' ', 'T')) ELSE null END,
    opened_first_time_at: CASE WHEN row.opened_first_time_at <> "" THEN datetime(replace(row.opened_first_time_at, ' ', 'T')) ELSE null END,
    opened_last_time_at: CASE WHEN row.opened_last_time_at <> "" THEN datetime(replace(row.opened_last_time_at, ' ', 'T')) ELSE null END,
    clicked_first_time_at: CASE WHEN row.clicked_first_time_at <> "" THEN datetime(replace(row.clicked_first_time_at, ' ', 'T')) ELSE null END,
    clicked_last_time_at: CASE WHEN row.clicked_last_time_at <> "" THEN datetime(replace(row.clicked_last_time_at, ' ', 'T')) ELSE null END,
    unsubscribed_at: CASE WHEN row.unsubscribed_at <> "" THEN datetime(replace(row.unsubscribed_at, ' ', 'T')) ELSE null END,
    hard_bounced_at: CASE WHEN row.hard_bounced_at <> "" THEN datetime(replace(row.hard_bounced_at, ' ', 'T')) ELSE null END,
    soft_bounced_at: CASE WHEN row.soft_bounced_at <> "" THEN datetime(replace(row.soft_bounced_at, ' ', 'T')) ELSE null END,
    complained_at: CASE WHEN row.complained_at <> "" THEN datetime(replace(row.complained_at, ' ', 'T')) ELSE null END,
    blocked_at: CASE WHEN row.blocked_at <> "" THEN datetime(replace(row.blocked_at, ' ', 'T')) ELSE null END,
    purchased_at: CASE WHEN row.purchased_at <> "" THEN datetime(replace(row.purchased_at, ' ', 'T')) ELSE null END
  })
  RETURN count(*) AS dummy
} IN TRANSACTIONS OF 50 ROWS
RETURN "Messages_2 loaded" AS result;

// Process Message_3.csv
CALL {
  LOAD CSV WITH HEADERS FROM 'file:///cleaned_data/Message_3.csv' AS row
  FIELDTERMINATOR ','
  CREATE (m:Message {
    message_id: row.message_id,
    sent_at: CASE WHEN row.sent_at <> "" THEN datetime(replace(row.sent_at, ' ', 'T')) ELSE null END,
    opened_first_time_at: CASE WHEN row.opened_first_time_at <> "" THEN datetime(replace(row.opened_first_time_at, ' ', 'T')) ELSE null END,
    opened_last_time_at: CASE WHEN row.opened_last_time_at <> "" THEN datetime(replace(row.opened_last_time_at, ' ', 'T')) ELSE null END,
    clicked_first_time_at: CASE WHEN row.clicked_first_time_at <> "" THEN datetime(replace(row.clicked_first_time_at, ' ', 'T')) ELSE null END,
    clicked_last_time_at: CASE WHEN row.clicked_last_time_at <> "" THEN datetime(replace(row.clicked_last_time_at, ' ', 'T')) ELSE null END,
    unsubscribed_at: CASE WHEN row.unsubscribed_at <> "" THEN datetime(replace(row.unsubscribed_at, ' ', 'T')) ELSE null END,
    hard_bounced_at: CASE WHEN row.hard_bounced_at <> "" THEN datetime(replace(row.hard_bounced_at, ' ', 'T')) ELSE null END,
    soft_bounced_at: CASE WHEN row.soft_bounced_at <> "" THEN datetime(replace(row.soft_bounced_at, ' ', 'T')) ELSE null END,
    complained_at: CASE WHEN row.complained_at <> "" THEN datetime(replace(row.complained_at, ' ', 'T')) ELSE null END,
    blocked_at: CASE WHEN row.blocked_at <> "" THEN datetime(replace(row.blocked_at, ' ', 'T')) ELSE null END,
    purchased_at: CASE WHEN row.purchased_at <> "" THEN datetime(replace(row.purchased_at, ' ', 'T')) ELSE null END
  })
  RETURN count(*) AS dummy
} IN TRANSACTIONS OF 50 ROWS
RETURN "Messages_3 loaded" AS result;

// Process Message_4.csv
CALL {
  LOAD CSV WITH HEADERS FROM 'file:///cleaned_data/Message_4.csv' AS row
  FIELDTERMINATOR ','
  CREATE (m:Message {
    message_id: row.message_id,
    sent_at: CASE WHEN row.sent_at <> "" THEN datetime(replace(row.sent_at, ' ', 'T')) ELSE null END,
    opened_first_time_at: CASE WHEN row.opened_first_time_at <> "" THEN datetime(replace(row.opened_first_time_at, ' ', 'T')) ELSE null END,
    opened_last_time_at: CASE WHEN row.opened_last_time_at <> "" THEN datetime(replace(row.opened_last_time_at, ' ', 'T')) ELSE null END,
    clicked_first_time_at: CASE WHEN row.clicked_first_time_at <> "" THEN datetime(replace(row.clicked_first_time_at, ' ', 'T')) ELSE null END,
    clicked_last_time_at: CASE WHEN row.clicked_last_time_at <> "" THEN datetime(replace(row.clicked_last_time_at, ' ', 'T')) ELSE null END,
    unsubscribed_at: CASE WHEN row.unsubscribed_at <> "" THEN datetime(replace(row.unsubscribed_at, ' ', 'T')) ELSE null END,
    hard_bounced_at: CASE WHEN row.hard_bounced_at <> "" THEN datetime(replace(row.hard_bounced_at, ' ', 'T')) ELSE null END,
    soft_bounced_at: CASE WHEN row.soft_bounced_at <> "" THEN datetime(replace(row.soft_bounced_at, ' ', 'T')) ELSE null END,
    complained_at: CASE WHEN row.complained_at <> "" THEN datetime(replace(row.complained_at, ' ', 'T')) ELSE null END,
    blocked_at: CASE WHEN row.blocked_at <> "" THEN datetime(replace(row.blocked_at, ' ', 'T')) ELSE null END,
    purchased_at: CASE WHEN row.purchased_at <> "" THEN datetime(replace(row.purchased_at, ' ', 'T')) ELSE null END
  })
  RETURN count(*) AS dummy
} IN TRANSACTIONS OF 50 ROWS
RETURN "Messages_4 loaded" AS result;

// Process Message_5.csv
CALL {
  LOAD CSV WITH HEADERS FROM 'file:///cleaned_data/Message_5.csv' AS row
  FIELDTERMINATOR ','
  CREATE (m:Message {
    message_id: row.message_id,
    sent_at: CASE WHEN row.sent_at <> "" THEN datetime(replace(row.sent_at, ' ', 'T')) ELSE null END,
    opened_first_time_at: CASE WHEN row.opened_first_time_at <> "" THEN datetime(replace(row.opened_first_time_at, ' ', 'T')) ELSE null END,
    opened_last_time_at: CASE WHEN row.opened_last_time_at <> "" THEN datetime(replace(row.opened_last_time_at, ' ', 'T')) ELSE null END,
    clicked_first_time_at: CASE WHEN row.clicked_first_time_at <> "" THEN datetime(replace(row.clicked_first_time_at, ' ', 'T')) ELSE null END,
    clicked_last_time_at: CASE WHEN row.clicked_last_time_at <> "" THEN datetime(replace(row.clicked_last_time_at, ' ', 'T')) ELSE null END,
    unsubscribed_at: CASE WHEN row.unsubscribed_at <> "" THEN datetime(replace(row.unsubscribed_at, ' ', 'T')) ELSE null END,
    hard_bounced_at: CASE WHEN row.hard_bounced_at <> "" THEN datetime(replace(row.hard_bounced_at, ' ', 'T')) ELSE null END,
    soft_bounced_at: CASE WHEN row.soft_bounced_at <> "" THEN datetime(replace(row.soft_bounced_at, ' ', 'T')) ELSE null END,
    complained_at: CASE WHEN row.complained_at <> "" THEN datetime(replace(row.complained_at, ' ', 'T')) ELSE null END,
    blocked_at: CASE WHEN row.blocked_at <> "" THEN datetime(replace(row.blocked_at, ' ', 'T')) ELSE null END,
    purchased_at: CASE WHEN row.purchased_at <> "" THEN datetime(replace(row.purchased_at, ' ', 'T')) ELSE null END
  })
  RETURN count(*) AS dummy
} IN TRANSACTIONS OF 50 ROWS
RETURN "Messages_5 loaded" AS result;

// Create Devices
LOAD CSV WITH HEADERS FROM 'file:///cleaned_data/UserDevices.csv' AS dev
FIELDTERMINATOR ','
MERGE (d:Device { client_id: toInteger(dev.client_id) })
ON CREATE SET d.user_id = toInteger(dev.user_id)
WITH count(*) AS dummy
RETURN "Devices loaded: " + dummy + " rows" AS result;

// ----- Relationship Creation (separate from node creation) -----
// User-User Friendship relationships
CALL {
  LOAD CSV WITH HEADERS FROM 'file:///cleaned_data/UserFriends.csv' AS row
  FIELDTERMINATOR ','
  MATCH (u1:User { user_id: toInteger(row.user_id_1) })
  MATCH (u2:User { user_id: toInteger(row.user_id_2) })
  MERGE (u1)-[:FRIEND_WITH]->(u2)
  MERGE (u2)-[:FRIEND_WITH]->(u1)
  RETURN count(*) AS dummy
} IN TRANSACTIONS OF 100 ROWS
RETURN "Friendship relationships created";

// Product-Category relationships
CALL {
  LOAD CSV WITH HEADERS FROM 'file:///cleaned_data/Products.csv' AS row
  FIELDTERMINATOR ','
  MATCH (p:Product { product_id: toInteger(row.product_id) })
  MATCH (c:Category { category_id: toInteger(row.category_id) })
  MERGE (p)-[:BELONGS_TO]->(c)
  RETURN count(*) AS dummy
} IN TRANSACTIONS OF 100 ROWS
RETURN "Product-Category relationships created";

// Campaign attributes (separated)
CALL {
  LOAD CSV WITH HEADERS FROM 'file:///cleaned_data/BulkCampaignAttributes.csv' AS row
  FIELDTERMINATOR ','
  MATCH (c:Campaign { campaign_type_id: row.campaign_type_id })
  SET c.started_at  = datetime(replace(row.started_at, ' ', 'T')),
      c.finished_at = datetime(replace(row.finished_at, ' ', 'T')),
      c.total_count = CASE WHEN row.total_count <> "" THEN toInteger(row.total_count) ELSE null END,
      c.ab_test     = (row.ab_test = "true"),
      c.warmup_mode = (row.warmup_mode = "true"),
      c.hour_limit  = CASE WHEN row.hour_limit <> "" THEN toFloat(row.hour_limit) ELSE null END,
      c.is_test     = (row.is_test = "true")
  RETURN count(*) AS dummy
} IN TRANSACTIONS OF 100 ROWS
RETURN "Campaign bulk attributes added";

// Campaign subject attributes
CALL {
  LOAD CSV WITH HEADERS FROM 'file:///cleaned_data/CampaignSubjectAttributes.csv' AS row
  FIELDTERMINATOR ','
  MATCH (c:Campaign { campaign_type_id: row.campaign_type_id })
  SET c.subject_length               = CASE WHEN row.subject_length <> "" THEN toInteger(row.subject_length) ELSE null END,
      c.subject_with_personalization = (row.subject_with_personalization = "true"),
      c.subject_with_deadline        = (row.subject_with_deadline = "true"),
      c.subject_with_emoji           = (row.subject_with_emoji = "true"),
      c.subject_with_bonuses         = (row.subject_with_bonuses = "true"),
      c.subject_with_discount        = (row.subject_with_discount = "true"),
      c.subject_with_saleout         = (row.subject_with_saleout = "true")
  RETURN count(*) AS dummy
} IN TRANSACTIONS OF 100 ROWS
RETURN "Campaign subject attributes added";

// ----- Campaign-Message Relationships (process each Message file individually) -----
// For Message_1.csv
CALL {
  LOAD CSV WITH HEADERS FROM 'file:///cleaned_data/Message_1.csv' AS row
  FIELDTERMINATOR ','
  MATCH (m:Message { message_id: row.message_id })
  MATCH (cam:Campaign { campaign_type_id: row.campaign_type_id })
  MERGE (cam)-[:SENT_MESSAGE]->(m)
  RETURN count(*) AS dummy
} IN TRANSACTIONS OF 100 ROWS
RETURN "Campaign-Message relationships created for Message_1";

// For Message_2.csv
CALL {
  LOAD CSV WITH HEADERS FROM 'file:///cleaned_data/Message_2.csv' AS row
  FIELDTERMINATOR ','
  MATCH (m:Message { message_id: row.message_id })
  MATCH (cam:Campaign { campaign_type_id: row.campaign_type_id })
  MERGE (cam)-[:SENT_MESSAGE]->(m)
  RETURN count(*) AS dummy
} IN TRANSACTIONS OF 100 ROWS
RETURN "Campaign-Message relationships created for Message_2";

// For Message_3.csv
CALL {
  LOAD CSV WITH HEADERS FROM 'file:///cleaned_data/Message_3.csv' AS row
  FIELDTERMINATOR ','
  MATCH (m:Message { message_id: row.message_id })
  MATCH (cam:Campaign { campaign_type_id: row.campaign_type_id })
  MERGE (cam)-[:SENT_MESSAGE]->(m)
  RETURN count(*) AS dummy
} IN TRANSACTIONS OF 100 ROWS
RETURN "Campaign-Message relationships created for Message_3";

// For Message_4.csv
CALL {
  LOAD CSV WITH HEADERS FROM 'file:///cleaned_data/Message_4.csv' AS row
  FIELDTERMINATOR ','
  MATCH (m:Message { message_id: row.message_id })
  MATCH (cam:Campaign { campaign_type_id: row.campaign_type_id })
  MERGE (cam)-[:SENT_MESSAGE]->(m)
  RETURN count(*) AS dummy
} IN TRANSACTIONS OF 100 ROWS
RETURN "Campaign-Message relationships created for Message_4";

// For Message_5.csv
CALL {
  LOAD CSV WITH HEADERS FROM 'file:///cleaned_data/Message_5.csv' AS row
  FIELDTERMINATOR ','
  MATCH (m:Message { message_id: row.message_id })
  MATCH (cam:Campaign { campaign_type_id: row.campaign_type_id })
  MERGE (cam)-[:SENT_MESSAGE]->(m)
  RETURN count(*) AS dummy
} IN TRANSACTIONS OF 100 ROWS
RETURN "Campaign-Message relationships created for Message_5";

// ----- Device-Message Relationships (process each Message file individually) -----
// For Message_1.csv
CALL {
  LOAD CSV WITH HEADERS FROM 'file:///cleaned_data/Message_1.csv' AS row
  FIELDTERMINATOR ','
  MATCH (m:Message { message_id: row.message_id })
  MATCH (d:Device { client_id: toInteger(row.client_id) })
  MERGE (d)-[:RECEIVED]->(m)
  RETURN count(*) AS dummy
} IN TRANSACTIONS OF 100 ROWS
RETURN "Device-Message relationships created for Message_1";

// For Message_2.csv
CALL {
  LOAD CSV WITH HEADERS FROM 'file:///cleaned_data/Message_2.csv' AS row
  FIELDTERMINATOR ','
  MATCH (m:Message { message_id: row.message_id })
  MATCH (d:Device { client_id: toInteger(row.client_id) })
  MERGE (d)-[:RECEIVED]->(m)
  RETURN count(*) AS dummy
} IN TRANSACTIONS OF 100 ROWS
RETURN "Device-Message relationships created for Message_2";

// For Message_3.csv
CALL {
  LOAD CSV WITH HEADERS FROM 'file:///cleaned_data/Message_3.csv' AS row
  FIELDTERMINATOR ','
  MATCH (m:Message { message_id: row.message_id })
  MATCH (d:Device { client_id: toInteger(row.client_id) })
  MERGE (d)-[:RECEIVED]->(m)
  RETURN count(*) AS dummy
} IN TRANSACTIONS OF 100 ROWS
RETURN "Device-Message relationships created for Message_3";

// For Message_4.csv
CALL {
  LOAD CSV WITH HEADERS FROM 'file:///cleaned_data/Message_4.csv' AS row
  FIELDTERMINATOR ','
  MATCH (m:Message { message_id: row.message_id })
  MATCH (d:Device { client_id: toInteger(row.client_id) })
  MERGE (d)-[:RECEIVED]->(m)
  RETURN count(*) AS dummy
} IN TRANSACTIONS OF 100 ROWS
RETURN "Device-Message relationships created for Message_4";

// For Message_5.csv
CALL {
  LOAD CSV WITH HEADERS FROM 'file:///cleaned_data/Message_5.csv' AS row
  FIELDTERMINATOR ','
  MATCH (m:Message { message_id: row.message_id })
  MATCH (d:Device { client_id: toInteger(row.client_id) })
  MERGE (d)-[:RECEIVED]->(m)
  RETURN count(*) AS dummy
} IN TRANSACTIONS OF 100 ROWS
RETURN "Device-Message relationships created for Message_5";

// ----- User-Message Relationships (process each Message file individually) -----
// For Message_1.csv
CALL {
  LOAD CSV WITH HEADERS FROM 'file:///cleaned_data/Message_1.csv' AS row
  FIELDTERMINATOR ','
  MATCH (d:Device { client_id: toInteger(row.client_id) })
  MATCH (u:User { user_id: d.user_id })
  MATCH (m:Message { message_id: row.message_id })
  MERGE (u)-[:RECEIVED]->(m)
  RETURN count(*) AS dummy
} IN TRANSACTIONS OF 100 ROWS
RETURN "User-Message relationships created for Message_1";

// For Message_2.csv
CALL {
  LOAD CSV WITH HEADERS FROM 'file:///cleaned_data/Message_2.csv' AS row
  FIELDTERMINATOR ','
  MATCH (d:Device { client_id: toInteger(row.client_id) })
  MATCH (u:User { user_id: d.user_id })
  MATCH (m:Message { message_id: row.message_id })
  MERGE (u)-[:RECEIVED]->(m)
  RETURN count(*) AS dummy
} IN TRANSACTIONS OF 100 ROWS
RETURN "User-Message relationships created for Message_2";

// For Message_3.csv
CALL {
  LOAD CSV WITH HEADERS FROM 'file:///cleaned_data/Message_3.csv' AS row
  FIELDTERMINATOR ','
  MATCH (d:Device { client_id: toInteger(row.client_id) })
  MATCH (u:User { user_id: d.user_id })
  MATCH (m:Message { message_id: row.message_id })
  MERGE (u)-[:RECEIVED]->(m)
  RETURN count(*) AS dummy
} IN TRANSACTIONS OF 100 ROWS
RETURN "User-Message relationships created for Message_3";

// For Message_4.csv
CALL {
  LOAD CSV WITH HEADERS FROM 'file:///cleaned_data/Message_4.csv' AS row
  FIELDTERMINATOR ','
  MATCH (d:Device { client_id: toInteger(row.client_id) })
  MATCH (u:User { user_id: d.user_id })
  MATCH (m:Message { message_id: row.message_id })
  MERGE (u)-[:RECEIVED]->(m)
  RETURN count(*) AS dummy
} IN TRANSACTIONS OF 100 ROWS
RETURN "User-Message relationships created for Message_4";

// For Message_5.csv
CALL {
  LOAD CSV WITH HEADERS FROM 'file:///cleaned_data/Message_5.csv' AS row
  FIELDTERMINATOR ','
  MATCH (d:Device { client_id: toInteger(row.client_id) })
  MATCH (u:User { user_id: d.user_id })
  MATCH (m:Message { message_id: row.message_id })
  MERGE (u)-[:RECEIVED]->(m)
  RETURN count(*) AS dummy
} IN TRANSACTIONS OF 100 ROWS
RETURN "User-Message relationships created for Message_5";

// ----- Process Events in smaller batches (potentially most memory intensive) -----
CALL {
  LOAD CSV WITH HEADERS FROM 'file:///cleaned_data/Events.csv' AS row
  FIELDTERMINATOR ','
  MATCH (u:User { user_id: toInteger(row.user_id) })
  MATCH (p:Product { product_id: toInteger(row.product_id) })
  CREATE (u)-[:PERFORMED_EVENT {
    event_time: datetime(replace(row.event_time, ' ', 'T')),
    event_type: row.event_type,
    user_session: row.user_session,
    price: toFloatOrNull(row.price)
  }]->(p)
  RETURN count(*) AS dummy
} IN TRANSACTIONS OF 50 ROWS
RETURN "Event relationships created";

RETURN "All Data Loaded Successfully!" AS result;
