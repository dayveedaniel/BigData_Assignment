// DROP CONSTRAINT ON (u:User)
// DROP CONSTRAINT ON (p:Product)
// DROP CONSTRAINT ON (c:Campaign)
// DROP CONSTRAINT ON (m:Message)

// Creating constraints in Memgraph
CREATE CONSTRAINT ON (u:User) ASSERT u.user_id IS UNIQUE;
CREATE CONSTRAINT ON (p:Product) ASSERT p.product_id IS UNIQUE;
CREATE CONSTRAINT ON (c:Campaign) ASSERT c.campaign_type_id IS UNIQUE;
CREATE CONSTRAINT ON (m:Message) ASSERT m.message_id IS UNIQUE;

// ----- Node Creation (adapted for Memgraph) -----
// Create Users
LOAD CSV FROM "/import/cleaned_data/Users.csv" WITH HEADER AS row
MERGE (u:User { user_id: ToInteger(row.user_id) });

// Create Categories
LOAD CSV FROM "/import/cleaned_data/Categories.csv" WITH HEADER AS row
MERGE (c:Category { category_id: ToInteger(row.category_id) })
ON CREATE SET c.category_code = row.category_code;

// Create Products
LOAD CSV FROM "/import/cleaned_data/Products.csv" WITH HEADER AS row
MERGE (p:Product { product_id: ToInteger(row.product_id) })
ON CREATE SET 
    p.brand = row.brand,
    p.composite_id = row.id,
    p.category_id = ToInteger(row.category_id);

// Create Campaigns
LOAD CSV FROM "/import/cleaned_data/Campaigns.csv" WITH HEADER AS row
MERGE (c:Campaign { campaign_type_id: row.campaign_type_id })
ON CREATE SET 
    c.campaign_id  = ToInteger(row.campaign_id),
    c.campaign_type = row.campaign_type,
    c.channel       = row.channel,
    c.topic         = CASE WHEN row.topic <> "" THEN row.topic ELSE null END,
    c.position      = CASE WHEN row.position <> "" THEN ToInteger(row.position) ELSE null END;

// Create Messages
LOAD CSV FROM "/import/cleaned_data/Messages.csv" WITH HEADER AS row
CREATE (m:Message {
  message_id: row.message_id,
  sent_at: CASE WHEN row.sent_at <> "" THEN Timestamp(Replace(row.sent_at, ' ', 'T')) ELSE null END,
  opened_first_time_at: CASE WHEN row.opened_first_time_at <> "" THEN Timestamp(Replace(row.opened_first_time_at, ' ', 'T')) ELSE null END,
  opened_last_time_at: CASE WHEN row.opened_last_time_at <> "" THEN Timestamp(Replace(row.opened_last_time_at, ' ', 'T')) ELSE null END,
  clicked_first_time_at: CASE WHEN row.clicked_first_time_at <> "" THEN Timestamp(Replace(row.clicked_first_time_at, ' ', 'T')) ELSE null END,
  clicked_last_time_at: CASE WHEN row.clicked_last_time_at <> "" THEN Timestamp(Replace(row.clicked_last_time_at, ' ', 'T')) ELSE null END,
  unsubscribed_at: CASE WHEN row.unsubscribed_at <> "" THEN Timestamp(Replace(row.unsubscribed_at, ' ', 'T')) ELSE null END,
  hard_bounced_at: CASE WHEN row.hard_bounced_at <> "" THEN Timestamp(Replace(row.hard_bounced_at, ' ', 'T')) ELSE null END,
  soft_bounced_at: CASE WHEN row.soft_bounced_at <> "" THEN Timestamp(Replace(row.soft_bounced_at, ' ', 'T')) ELSE null END,
  complained_at: CASE WHEN row.complained_at <> "" THEN Timestamp(Replace(row.complained_at, ' ', 'T')) ELSE null END,
  blocked_at: CASE WHEN row.blocked_at <> "" THEN Timestamp(Replace(row.blocked_at, ' ', 'T')) ELSE null END,
  purchased_at: CASE WHEN row.purchased_at <> "" THEN Timestamp(Replace(row.purchased_at, ' ', 'T')) ELSE null END
});

// Create Devices
LOAD CSV FROM "/import/cleaned_data/UserDevices.csv" WITH HEADER AS dev
MERGE (d:Device { client_id: ToInteger(dev.client_id) })
ON CREATE SET d.user_id = ToInteger(dev.user_id);

// ----- Relationship Creation (adapted for Memgraph) -----
// User-User Friendship relationships
LOAD CSV FROM "/import/cleaned_data/UserFriends.csv" WITH HEADER AS row
MATCH (u1:User { user_id: ToInteger(row.user_id_1) })
MATCH (u2:User { user_id: ToInteger(row.user_id_2) })
MERGE (u1)-[:FRIEND_WITH]->(u2)
MERGE (u2)-[:FRIEND_WITH]->(u1);

// Product-Category relationships
LOAD CSV FROM "/import/cleaned_data/Products.csv" WITH HEADER AS row

MATCH (p:Product { product_id: ToInteger(row.product_id) })
MATCH (c:Category { category_id: ToInteger(row.category_id) })
MERGE (p)-[:BELONGS_TO]->(c);

// Campaign attributes
LOAD CSV FROM "/import/cleaned_data/BulkCampaignAttributes.csv" WITH HEADER AS row

MATCH (c:Campaign { campaign_type_id: row.campaign_type_id })
SET c.started_at  = Timestamp(Replace(row.started_at, ' ', 'T')),
    c.finished_at = Timestamp(Replace(row.finished_at, ' ', 'T')),
    c.total_count = CASE WHEN row.total_count <> "" THEN ToInteger(row.total_count) ELSE null END,
    c.ab_test     = (row.ab_test = "true"),
    c.warmup_mode = (row.warmup_mode = "true"),
    c.hour_limit  = CASE WHEN row.hour_limit <> "" THEN ToFloat(row.hour_limit) ELSE null END,
    c.is_test     = (row.is_test = "true");

// Campaign subject attributes
LOAD CSV FROM "/import/cleaned_data/CampaignSubjectAttributes.csv" WITH HEADER AS row

MATCH (c:Campaign { campaign_type_id: row.campaign_type_id })
SET c.subject_length               = CASE WHEN row.subject_length <> "" THEN ToInteger(row.subject_length) ELSE null END,
    c.subject_with_personalization = (row.subject_with_personalization = "true"),
    c.subject_with_deadline        = (row.subject_with_deadline = "true"),
    c.subject_with_emoji           = (row.subject_with_emoji = "true"),
    c.subject_with_bonuses         = (row.subject_with_bonuses = "true"),
    c.subject_with_discount        = (row.subject_with_discount = "true"),
    c.subject_with_saleout         = (row.subject_with_saleout = "true");

// Campaign-Message relationships
LOAD CSV FROM "/import/cleaned_data/Messages.csv" WITH HEADER AS row

MATCH (m:Message { message_id: row.message_id })
MATCH (cam:Campaign { campaign_type_id: row.campaign_type_id })
MERGE (cam)-[:SENT_MESSAGE]->(m);

// User-Device relationships
LOAD CSV FROM "/import/cleaned_data/UserDevices.csv" WITH HEADER AS dev

MATCH (d:Device { client_id: ToInteger(dev.client_id) })
MATCH (u:User { user_id: ToInteger(dev.user_id) })
MERGE (u)-[:HAS_DEVICE]->(d);

// Device-Message relationships
LOAD CSV FROM "/import/cleaned_data/Messages.csv" WITH HEADER AS row

MATCH (m:Message { message_id: row.message_id })
MATCH (d:Device { client_id: ToInteger(row.client_id) })
MERGE (d)-[:RECEIVED]->(m);

// User-Message relationships
LOAD CSV FROM "/import/cleaned_data/Messages.csv" WITH HEADER AS row

MATCH (d:Device { client_id: ToInteger(row.client_id) })
MATCH (u:User { user_id: d.user_id })
MATCH (m:Message { message_id: row.message_id })
MERGE (u)-[:RECEIVED]->(m);

// Process Events
LOAD CSV FROM "/import/cleaned_data/Events.csv" WITH HEADER AS row

MATCH (u:User { user_id: ToInteger(row.user_id) })
MATCH (p:Product { product_id: ToInteger(row.product_id) })
CREATE (u)-[:PERFORMED_EVENT {
  event_time: Timestamp(Replace(row.event_time, ' ', 'T')),
  event_type: row.event_type,
  user_session: row.user_session,
  price: CASE WHEN row.price <> "" THEN ToFloat(row.price) ELSE null END
}]->(p);