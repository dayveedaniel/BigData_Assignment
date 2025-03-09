import csv
from neo4j import GraphDatabase

# Connection details – adjust as needed.
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "12345678"
BATCH_SIZE = 1000  # adjust the batch size as needed

# --- Functions for loading data for each CSV file in batches ---

def load_csv_in_batches(file_path, batch_size, process_batch_fn):
    """Reads a CSV file and sends rows to the provided batch processor."""
    with open(file_path, "r", newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        batch = []
        for row in reader:
            batch.append(row)
            if len(batch) >= batch_size:
                process_batch_fn(batch)
                batch = []
        if batch:
            process_batch_fn(batch)

# --- Node Creation Functions ---

def _load_users(tx, rows):
    query = """
    UNWIND $rows AS row
    MERGE (u:User { user_id: toInteger(row.user_id) })
    """
    tx.run(query, rows=rows)

def load_users(session):
    load_csv_in_batches("cleaned_data/Users.csv", BATCH_SIZE,
                        lambda batch: session.execute_write(_load_users, batch))
    print("Users loaded.")

def _load_categories(tx, rows):
    query = """
    UNWIND $rows AS row
    MERGE (c:Category { category_id: toInteger(row.category_id) })
      ON CREATE SET c.category_code = row.category_code
    """
    tx.run(query, rows=rows)

def load_categories(session):
    load_csv_in_batches("cleaned_data/Categories.csv", BATCH_SIZE,
                        lambda batch: session.execute_write(_load_categories, batch))
    print("Categories loaded.")

def _load_products(tx, rows):
    query = """
    UNWIND $rows AS row
    MERGE (p:Product { product_id: toInteger(row.product_id) })
      ON CREATE SET p.brand = row.brand,
                    p.composite_id = row.id,
                    p.category_id = toInteger(row.category_id)
    """
    tx.run(query, rows=rows)

def load_products(session):
    load_csv_in_batches("cleaned_data/Products.csv", BATCH_SIZE,
                        lambda batch: session.execute_write(_load_products, batch))
    print("Products loaded.")

def _load_campaigns(tx, rows):
    query = """
    UNWIND $rows AS row
    MERGE (c:Campaign { campaign_type_id: row.campaign_type_id })
      ON CREATE SET c.campaign_id  = toInteger(row.campaign_id),
                    c.campaign_type = row.campaign_type,
                    c.channel       = row.channel,
                    c.topic         = CASE WHEN row.topic <> "" THEN row.topic ELSE null END,
                    c.position      = CASE WHEN row.position <> "" THEN toInteger(row.position) ELSE null END
    """
    tx.run(query, rows=rows)

def load_campaigns(session):
    load_csv_in_batches("cleaned_data/Campaigns.csv", BATCH_SIZE,
                        lambda batch: session.execute_write(_load_campaigns, batch))
    print("Campaigns loaded.")

def _load_messages(tx, rows):
    query = """
    UNWIND $rows AS row
    MERGE (m:Message { message_id: row.message_id })
      ON CREATE SET 
          m.sent_at = CASE WHEN row.sent_at <> "" THEN datetime(replace(row.sent_at, ' ', 'T')) ELSE null END,
          m.opened_first_time_at = CASE WHEN row.opened_first_time_at <> "" THEN datetime(replace(row.opened_first_time_at, ' ', 'T')) ELSE null END,
          m.opened_last_time_at = CASE WHEN row.opened_last_time_at <> "" THEN datetime(replace(row.opened_last_time_at, ' ', 'T')) ELSE null END,
          m.clicked_first_time_at = CASE WHEN row.clicked_first_time_at <> "" THEN datetime(replace(row.clicked_first_time_at, ' ', 'T')) ELSE null END,
          m.clicked_last_time_at = CASE WHEN row.clicked_last_time_at <> "" THEN datetime(replace(row.clicked_last_time_at, ' ', 'T')) ELSE null END,
          m.unsubscribed_at = CASE WHEN row.unsubscribed_at <> "" THEN datetime(replace(row.unsubscribed_at, ' ', 'T')) ELSE null END,
          m.hard_bounced_at = CASE WHEN row.hard_bounced_at <> "" THEN datetime(replace(row.hard_bounced_at, ' ', 'T')) ELSE null END,
          m.soft_bounced_at = CASE WHEN row.soft_bounced_at <> "" THEN datetime(replace(row.soft_bounced_at, ' ', 'T')) ELSE null END,
          m.complained_at = CASE WHEN row.complained_at <> "" THEN datetime(replace(row.complained_at, ' ', 'T')) ELSE null END,
          m.blocked_at = CASE WHEN row.blocked_at <> "" THEN datetime(replace(row.blocked_at, ' ', 'T')) ELSE null END,
          m.purchased_at = CASE WHEN row.purchased_at <> "" THEN datetime(replace(row.purchased_at, ' ', 'T')) ELSE null END
    """
    tx.run(query, rows=rows)

def load_messages(session):
    load_csv_in_batches("cleaned_data/Messages.csv", BATCH_SIZE,
                        lambda batch: session.execute_write(_load_messages, batch))
    print("Messages loaded.")

def _load_devices(tx, rows):
    query = """
    UNWIND $rows AS row
    MERGE (d:Device { client_id: toInteger(row.client_id) })
      ON CREATE SET d.user_id = toInteger(row.user_id)
    """
    tx.run(query, rows=rows)

def load_devices(session):
    load_csv_in_batches("cleaned_data/UserDevices.csv", BATCH_SIZE,
                        lambda batch: session.execute_write(_load_devices, batch))
    print("Devices loaded.")

# --- Relationship Creation Functions ---

def _create_friendships(tx, rows):
    query = """
    UNWIND $rows AS row
    MATCH (u1:User { user_id: toInteger(row.user_id_1) })
    MATCH (u2:User { user_id: toInteger(row.user_id_2) })
    MERGE (u1)-[:FRIEND_WITH]->(u2)
    MERGE (u2)-[:FRIEND_WITH]->(u1)
    """
    tx.run(query, rows=rows)

def load_friendships(session):
    load_csv_in_batches("cleaned_data/UserFriends.csv", BATCH_SIZE,
                        lambda batch: session.execute_write(_create_friendships, batch))
    print("Friendship relationships created.")

def _create_product_categories(tx, rows):
    query = """
    UNWIND $rows AS row
    MATCH (p:Product { product_id: toInteger(row.product_id) })
    MATCH (c:Category { category_id: toInteger(row.category_id) })
    MERGE (p)-[:BELONGS_TO]->(c)
    """
    tx.run(query, rows=rows)

def load_product_categories(session):
    load_csv_in_batches("cleaned_data/Products.csv", BATCH_SIZE,
                        lambda batch: session.execute_write(_create_product_categories, batch))
    print("Product-Category relationships created.")

def _add_campaign_bulk_attributes(tx, rows):
    query = """
    UNWIND $rows AS row
    MATCH (c:Campaign { campaign_type_id: row.campaign_type_id })
    SET c.started_at  = datetime(replace(row.started_at, ' ', 'T')),
        c.finished_at = datetime(replace(row.finished_at, ' ', 'T')),
        c.total_count = CASE WHEN row.total_count <> "" THEN toInteger(row.total_count) ELSE null END,
        c.ab_test     = (row.ab_test = "true"),
        c.warmup_mode = (row.warmup_mode = "true"),
        c.hour_limit  = CASE WHEN row.hour_limit <> "" THEN toFloat(row.hour_limit) ELSE null END,
        c.is_test     = (row.is_test = "true")
    """
    tx.run(query, rows=rows)

def load_campaign_bulk_attributes(session):
    load_csv_in_batches("cleaned_data/BulkCampaignAttributes.csv", BATCH_SIZE,
                        lambda batch: session.execute_write(_add_campaign_bulk_attributes, batch))
    print("Campaign bulk attributes added.")

def _add_campaign_subject_attributes(tx, rows):
    query = """
    UNWIND $rows AS row
    MATCH (c:Campaign { campaign_type_id: row.campaign_type_id })
    SET c.subject_length               = CASE WHEN row.subject_length <> "" THEN toInteger(row.subject_length) ELSE null END,
        c.subject_with_personalization = (row.subject_with_personalization = "true"),
        c.subject_with_deadline        = (row.subject_with_deadline = "true"),
        c.subject_with_emoji           = (row.subject_with_emoji = "true"),
        c.subject_with_bonuses         = (row.subject_with_bonuses = "true"),
        c.subject_with_discount        = (row.subject_with_discount = "true"),
        c.subject_with_saleout         = (row.subject_with_saleout = "true")
    """
    tx.run(query, rows=rows)

def load_campaign_subject_attributes(session):
    load_csv_in_batches("cleaned_data/CampaignSubjectAttributes.csv", BATCH_SIZE,
                        lambda batch: session.execute_write(_add_campaign_subject_attributes, batch))
    print("Campaign subject attributes added.")

def _create_campaign_messages(tx, rows):
    query = """
    UNWIND $rows AS row
    MATCH (m:Message { message_id: row.message_id })
    MATCH (c:Campaign { campaign_type_id: row.campaign_type_id })
    MERGE (c)-[:SENT_MESSAGE]->(m)
    """
    tx.run(query, rows=rows)

def load_campaign_messages(session):
    load_csv_in_batches("cleaned_data/Messages.csv", BATCH_SIZE,
                        lambda batch: session.execute_write(_create_campaign_messages, batch))
    print("Campaign-Message relationships created.")

def _create_user_devices(tx, rows):
    query = """
    UNWIND $rows AS row
    MATCH (d:Device { client_id: toInteger(row.client_id) })
    MATCH (u:User { user_id: toInteger(row.user_id) })
    MERGE (u)-[:HAS_DEVICE]->(d)
    """
    tx.run(query, rows=rows)

def load_user_devices(session):
    load_csv_in_batches("cleaned_data/UserDevices.csv", BATCH_SIZE,
                        lambda batch: session.execute_write(_create_user_devices, batch))
    print("User-Device relationships created.")

def _create_device_messages(tx, rows):
    query = """
    UNWIND $rows AS row
    MATCH (m:Message { message_id: row.message_id })
    MATCH (d:Device { client_id: toInteger(row.client_id) })
    MERGE (d)-[:RECEIVED]->(m)
    """
    tx.run(query, rows=rows)

def load_device_messages(session):
    load_csv_in_batches("cleaned_data/Messages.csv", BATCH_SIZE,
                        lambda batch: session.execute_write(_create_device_messages, batch))
    print("Device-Message relationships created.")

def _create_user_messages(tx, rows):
    query = """
    UNWIND $rows AS row
    MATCH (d:Device { client_id: toInteger(row.client_id) })
    MATCH (u:User { user_id: d.user_id })
    MATCH (m:Message { message_id: row.message_id })
    MERGE (u)-[:RECEIVED]->(m)
    """
    tx.run(query, rows=rows)

def load_user_messages(session):
    load_csv_in_batches("cleaned_data/Messages.csv", BATCH_SIZE,
                        lambda batch: session.execute_write(_create_user_messages, batch))
    print("User-Message relationships created.")

def _create_events(tx, rows):
    query = """
    UNWIND $rows AS row
    MATCH (u:User { user_id: toInteger(row.user_id) })
    MATCH (p:Product { product_id: toInteger(row.product_id) })
    CREATE (u)-[:PERFORMED_EVENT {
        event_time: datetime(replace(row.event_time, ' ', 'T')),
        event_type: row.event_type,
        user_session: row.user_session,
        price: toFloat(row.price)
    }]->(p)
    """
    tx.run(query, rows=rows)

def load_events(session):
    load_csv_in_batches("cleaned_data/Events.csv", BATCH_SIZE,
                        lambda batch: session.execute_write(_create_events, batch))
    print("Event relationships created.")

# --- Main Loader Function ---

def main():
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    try:
        with driver.session() as session:
            # Load nodes
            load_users(session)
            load_categories(session)
            load_products(session)
            load_campaigns(session)
            load_messages(session)
            load_devices(session)
            # Load relationships
            load_friendships(session)
            load_product_categories(session)
            load_campaign_bulk_attributes(session)
            load_campaign_subject_attributes(session)
            load_campaign_messages(session)
            load_user_devices(session)
            load_device_messages(session)
            load_user_messages(session)
            load_events(session)
    finally:
        driver.close()

if __name__ == "__main__":
    main()
