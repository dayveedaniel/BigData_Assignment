import os
import pandas as pd
import numpy as np
import json
from datetime import datetime
import gc

path = os.getcwd()

# Read source CSV files
df_first_purchase = pd.read_csv(f"{path}/data/client_first_purchase_date.csv", low_memory=False)
df_events = pd.read_csv(f"{path}/data/events.csv", low_memory=False)
df_campaigns = pd.read_csv(f"{path}/data/campaigns.csv", low_memory=False)
df_messages = pd.read_csv(f"{path}/data/messages.csv", low_memory=False)
df_friends = pd.read_csv(f"{path}/data/friends.csv", low_memory=False)

# Function to extract user_id and device_id from client_id.
#  client_id = "151591562" + <9-digit user_id> + <device_id>
def extract_ids(client_id):
    prefix = "151591562"
    s = str(client_id)
    if s.startswith(prefix):
        remainder = s[len(prefix):]
        # Assume the first 9 digits represent user_id
        user_id_str = remainder[:9]
        device_id_str = remainder[9:]
        try:
            user_id = int(user_id_str)
        except ValueError:
            user_id = None
        try:
            device_id = int(device_id_str) if device_id_str != "" else None
        except ValueError:
            device_id = None
        return user_id, device_id
    else:
        return None, None

# -----------------------------
# 1) WRITE CLEANED CSV FILES
# -----------------------------

# --- Process Users table ---
df_first_purchase['user_id'] = df_first_purchase['client_id'].apply(lambda x: extract_ids(x)[0])
df_messages['user_id'] = df_messages['client_id'].apply(lambda x: extract_ids(x)[0])

users_from_first_purchase = df_first_purchase[['user_id']]
users_from_messages = df_messages[['user_id']]
users_df = pd.concat([users_from_first_purchase, users_from_messages]).drop_duplicates()
users_df = users_df[['user_id']]
users_df.to_csv(f"{path}/cleaned_data/Users.csv", index=False)

# --- Process UserDevices table ---
df_messages['device_id'] = df_messages['client_id'].apply(lambda x: extract_ids(x)[1])
user_devices_df = df_messages[['client_id', 'user_id', 'device_id', 'platform', 'email_provider', 'stream']].drop_duplicates()

df_first_purchase['first_purchase_date'] = pd.to_datetime(df_first_purchase['first_purchase_date'], errors='coerce')
first_purchase_df = df_first_purchase[['client_id', 'first_purchase_date']].drop_duplicates()

user_devices_df = user_devices_df.merge(first_purchase_df, on='client_id', how='left')

# Remove duplicates by keeping the row with the maximum non-null columns for each client_id
user_devices_df['non_nulls'] = user_devices_df.notnull().sum(axis=1)
user_devices_df = user_devices_df.loc[user_devices_df.groupby('client_id')['non_nulls'].idxmax()]
user_devices_df.drop(columns='non_nulls', inplace=True)

user_devices_df.to_csv(f"{path}/cleaned_data/UserDevices.csv", index=False)

# --- Process UserFriends table ---
df_friends.columns = ['user_id_1', 'user_id_2']
df_friends[['user_id_1', 'user_id_2']] = np.sort(df_friends[['user_id_1', 'user_id_2']], axis=1)
user_friends_df = df_friends.drop_duplicates()
user_friends_df.to_csv(f"{path}/cleaned_data/UserFriends.csv", index=False)

# --- Process Categories table ---
categories_df = df_events[['category_id', 'category_code']]
categories_df = categories_df.sort_values(by='category_code', na_position='last')
categories_df = categories_df.drop_duplicates(subset=['category_id'], keep='first')
categories_df.to_csv(f"{path}/cleaned_data/Categories.csv", index=False)

# --- Process Products table ---
products_df = df_events[['product_id', 'brand', 'category_id']].drop_duplicates()
products_df['id'] = products_df.apply(lambda row: f"{row['product_id']}_{row['category_id']}", axis=1)
products_df = products_df[['id', 'product_id', 'brand', 'category_id']]

products_df['non_nulls'] = products_df.notnull().sum(axis=1)
products_df = products_df.loc[products_df.groupby('id')['non_nulls'].idxmax()]
products_df.drop(columns='non_nulls', inplace=True)

products_df.to_csv(f"{path}/cleaned_data/Products.csv", index=False)

# --- Process Events table ---
events_df = df_events.copy()
events_df['event_time'] = pd.to_datetime(events_df['event_time'], errors='coerce')
if 'user_id' not in events_df.columns:
    events_df['user_id'] = events_df['client_id'].apply(lambda x: extract_ids(x)[0])
events_df = events_df[['event_time', 'event_type', 'product_id', 'user_id', 'user_session', 'price']]
events_df.to_csv(f"{path}/cleaned_data/Events.csv", index=False)

# --- Process Campaigns table ---
campaigns_df = df_campaigns.rename(columns={'id': 'campaign_id'})
campaigns_df['position'] = campaigns_df['position'].astype('Int64')
campaigns_df['campaign_type_id'] = campaigns_df['campaign_type'].astype(str) + '_' + campaigns_df['campaign_id'].astype(str)
campaigns_table = campaigns_df[['campaign_type_id', 'campaign_id', 'campaign_type', 'channel', 'topic', 'position']].drop_duplicates()
campaigns_table.to_csv(f"{path}/cleaned_data/Campaigns.csv", index=False)

# --- Process Messages table ---
datetime_cols = [
    'sent_at', 'opened_first_time_at', 'opened_last_time_at', 
    'clicked_first_time_at', 'clicked_last_time_at', 'unsubscribed_at', 
    'hard_bounced_at', 'soft_bounced_at', 'complained_at', 'blocked_at', 'purchased_at'
]
for col in datetime_cols:
    if col in df_messages.columns:
        df_messages[col] = pd.to_datetime(df_messages[col], errors='coerce')

df_messages['campaign_type_id'] = df_messages['message_type'].astype(str) + '_' + df_messages['campaign_id'].astype(str)
messages_table = df_messages[[
    'message_id', 'client_id', 'campaign_type_id', 'sent_at', 
    'opened_first_time_at', 'opened_last_time_at', 'clicked_first_time_at', 
    'clicked_last_time_at', 'unsubscribed_at', 'hard_bounced_at', 
    'soft_bounced_at', 'complained_at', 'blocked_at', 'purchased_at'
]]
messages_table.to_csv(f"{path}/cleaned_data/Messages.csv", index=False)

# Split the messages_table into 5 parts
split_messages = np.array_split(messages_table, 5)

# Write each part to a separate CSV file (Message_1.csv to Message_5.csv)
for i, split_df in enumerate(split_messages, start=1):
    split_df.to_csv(f"{path}/cleaned_data/Message_{i}.csv", index=False)

# --- Process BulkCampaignAttributes table ---
bulk_campaigns = campaigns_df[campaigns_df['campaign_type'] == 'bulk']
bulk_attr_df = bulk_campaigns[['campaign_type_id', 'started_at', 'finished_at', 'total_count', 
                               'ab_test', 'warmup_mode', 'hour_limit', 'is_test']].drop_duplicates()
bulk_attr_df['started_at'] = pd.to_datetime(bulk_attr_df['started_at'], errors='coerce')
bulk_attr_df['finished_at'] = pd.to_datetime(bulk_attr_df['finished_at'], errors='coerce')
bulk_attr_df['total_count'] = bulk_attr_df['total_count'].astype('Int64')
bulk_attr_df.to_csv(f"{path}/cleaned_data/BulkCampaignAttributes.csv", index=False)

# --- Process CampaignSubjectAttributes table ---
subject_attr_df = campaigns_df[[
    'campaign_type_id', 'subject_length', 'subject_with_personalization', 
    'subject_with_deadline', 'subject_with_emoji', 'subject_with_bonuses', 
    'subject_with_discount', 'subject_with_saleout'
]].drop_duplicates()
subject_attr_df['subject_length'] = subject_attr_df['subject_length'].astype('Int64')
subject_attr_df.to_csv(f"{path}/cleaned_data/CampaignSubjectAttributes.csv", index=False)

print('Done writing cleaned CSV files!')

# --- 2a) Build final 'users' collection ---
users_final_list = []

# Convert user_friends_df and user_devices_df to dict for quick lookup
friends_map = {}
for row in user_friends_df.itertuples(index=False):
    u1, u2 = row.user_id_1, row.user_id_2
    friends_map.setdefault(u1, set()).add(u2)
    friends_map.setdefault(u2, set()).add(u1)

devices_map = {}
for row in user_devices_df.itertuples(index=False):
    # row: (client_id, user_id, device_id, platform, email_provider, stream, first_purchase_date)
    user = row.user_id
    if pd.isna(user):
        continue

    # Build device sub-document ensuring required fields
    device_doc = {
        "device_id": (int(row.device_id) if not pd.isna(row.device_id) else -1),  # default to -1 if missing
        "first_purchase_date": None,
        "platform": (str(row.platform) if not pd.isna(row.platform) else None),
        "email_provider": (str(row.email_provider) if not pd.isna(row.email_provider) else None),
        "stream": (str(row.stream) if not pd.isna(row.stream) else ""),  # must be a non-null string
        "client_id": (int(row.client_id) if not pd.isna(row.client_id) else -1)
    }

    if not pd.isna(row.first_purchase_date):
        device_doc["first_purchase_date"] = row.first_purchase_date.isoformat()

    devices_map.setdefault(user, []).append(device_doc)

unique_users = users_df['user_id'].dropna().unique()
for u in sorted(unique_users):
    u_int = int(u)
    doc = {
        "user_id": u_int,
        "friends": sorted(list(friends_map.get(u_int, []))),
        "devices": devices_map.get(u_int, [])
    }
    users_final_list.append(doc)

with open(f"{path}/cleaned_data/Users.json", "w") as f:
    json.dump(users_final_list, f, indent=2)

print("Users.json written.")


# 2b) Build final 'products' collection:
# Read categories into dict for category_code lookup
cat_code_map = {}
for row in categories_df.itertuples(index=False):
    cat_id = row.category_id
    cat_code = row.category_code if pd.notna(row.category_code) else None
    cat_code_map[cat_id] = cat_code

products_final_list = []
for row in products_df.itertuples(index=False):
    _id = row.id  # string "productId_categoryId"
    p_id = (None if pd.isna(row.product_id) else int(row.product_id))
    brand = None if pd.isna(row.brand) else str(row.brand)
    c_id = (None if pd.isna(row.category_id) else int(row.category_id))

    doc = {
        "_id": _id,
        "product_id": p_id,
        "brand": brand,
        "category_id": c_id,
        "category_code": cat_code_map.get(c_id)
    }
    products_final_list.append(doc)

with open(f"{path}/cleaned_data/Products.json", "w") as f:
    json.dump(products_final_list, f, indent=2)

print("Products.json written.")


# --- 2c) Build final 'events' collection ---
events_final_list = []
for row in events_df.itertuples(index=False):
    # Convert event_time to extended JSON for a date type
    e_time = None
    if not pd.isna(row.event_time):
        e_time = {"$date": row.event_time.isoformat()}  # using extended JSON for later conversion

    doc = {
        # Do not include _id here – it will be added by the JS importer.
        "event_time": e_time,
        "event_type": (None if pd.isna(row.event_type) else str(row.event_type)),
        "product_id": (None if pd.isna(row.product_id) else int(row.product_id)),
        "user_id": (None if pd.isna(row.user_id) else int(row.user_id)),
        "user_session": (None if pd.isna(row.user_session) else str(row.user_session)),
        "price": None
    }

    if not pd.isna(row.price):
        val_str = str(row.price).replace("$", "").replace(",", "")
        try:
            doc["price"] = float(val_str)
        except ValueError:
            doc["price"] = None

    events_final_list.append(doc)

with open(f"{path}/cleaned_data/Events.json", "w") as f:
    json.dump(events_final_list, f, indent=2)

print("Events.json written.")


# 2d) Build final 'campaigns' collection:
# Prepare bulk_attributes and subject_attributes mappings
bulk_map = {}
for row in bulk_attr_df.itertuples(index=False):
    subdoc = {}
    subdoc["started_at"] = row.started_at.isoformat() if not pd.isna(row.started_at) else None
    subdoc["finished_at"] = row.finished_at.isoformat() if not pd.isna(row.finished_at) else None
    subdoc["total_count"] = (None if pd.isna(row.total_count) else int(row.total_count))
    subdoc["ab_test"] = bool(row.ab_test) if not pd.isna(row.ab_test) else False
    subdoc["warmup_mode"] = bool(row.warmup_mode) if not pd.isna(row.warmup_mode) else False
    subdoc["hour_limit"] = (None if pd.isna(row.hour_limit) else float(row.hour_limit))
    subdoc["is_test"] = bool(row.is_test) if not pd.isna(row.is_test) else False

    bulk_map[row.campaign_type_id] = subdoc

subj_map = {}
for row in subject_attr_df.itertuples(index=False):
    subdoc = {
        "subject_length": (None if pd.isna(row.subject_length) else int(row.subject_length)),
        "subject_with_personalization": bool(row.subject_with_personalization) if not pd.isna(row.subject_with_personalization) else False,
        "subject_with_deadline": bool(row.subject_with_deadline) if not pd.isna(row.subject_with_deadline) else False,
        "subject_with_emoji": bool(row.subject_with_emoji) if not pd.isna(row.subject_with_emoji) else False,
        "subject_with_bonuses": bool(row.subject_with_bonuses) if not pd.isna(row.subject_with_bonuses) else False,
        "subject_with_discount": bool(row.subject_with_discount) if not pd.isna(row.subject_with_discount) else False,
        "subject_with_saleout": bool(row.subject_with_saleout) if not pd.isna(row.subject_with_saleout) else False
    }
    subj_map[row.campaign_type_id] = subdoc

campaigns_final_list = []
for row in campaigns_table.itertuples(index=False):
    doc = {
        "_id": str(row.campaign_type_id),  # _id must be a string
        "campaign_id": int(row.campaign_id),
        "campaign_type": str(row.campaign_type),
        "channel": str(row.channel),
        "topic": (None if pd.isna(row.topic) else str(row.topic)),
        "position": (None if pd.isna(row.position) else int(row.position))
    }
    if row.campaign_type_id in bulk_map:
        doc["bulk_attributes"] = bulk_map[row.campaign_type_id]
    if row.campaign_type_id in subj_map:
        doc["subject_attributes"] = subj_map[row.campaign_type_id]

    campaigns_final_list.append(doc)

with open(f"{path}/cleaned_data/Campaigns.json", "w") as f:
    json.dump(campaigns_final_list, f, indent=2)

print("Campaigns.json written.")


# 2e) Build final 'messages' collection:
messages_final_list = []
for row in messages_table.itertuples(index=False):
    # Note: Renaming campaign_type_id to campaign_id to match the schema.
    doc = {
        "_id": (None if pd.isna(row.message_id) else str(row.message_id)),
        "campaign_id": (None if pd.isna(row.campaign_type_id) else str(row.campaign_type_id)),
        "client_id": (None if pd.isna(row.client_id) else int(row.client_id))
    }
    # Process all timestamp columns
    for dcol in [
        'sent_at', 'opened_first_time_at', 'opened_last_time_at', 
        'clicked_first_time_at', 'clicked_last_time_at', 'unsubscribed_at', 
        'hard_bounced_at', 'soft_bounced_at', 'complained_at', 
        'blocked_at', 'purchased_at'
    ]:
        val = getattr(row, dcol)
        if dcol == 'sent_at' and not pd.isna(val):
            # sent_at must be a date type, so output as extended JSON
            doc[dcol] = {"$date": val.isoformat()}
        else:
            # Other time fields remain as ISO strings (or null)
            doc[dcol] = None if pd.isna(val) else val.isoformat()
    messages_final_list.append(doc)

# Split messages into 6 chunks
num_chunks = 6
total_messages = len(messages_final_list)
chunk_size = (total_messages // num_chunks) + (total_messages % num_chunks > 0)

for i in range(num_chunks):
    chunk = messages_final_list[i*chunk_size : (i+1)*chunk_size]
    with open(f"{path}/cleaned_data/Messages_chunk_{i+1}.json", "w") as f:
        json.dump(chunk, f, indent=2)
    print(f"Messages chunk {i+1} written.")

print("Messages.json written.")

# Clean up DataFrames from memory
del (
    df_first_purchase,
    df_events,
    df_campaigns,
    df_messages,
    df_friends,
    users_from_first_purchase,
    users_from_messages,
    users_df,
    user_devices_df,
    user_friends_df,
    categories_df,
    products_df,
    events_df,
    campaigns_df,
    campaigns_table,
    bulk_attr_df,
    subject_attr_df,
    messages_table
)

gc.collect()  # Force garbage collection if needed

print("All DataFrames dropped from memory, and script is complete.")
print("Done! CSV + JSON files are in the 'cleaned_data' directory.")
