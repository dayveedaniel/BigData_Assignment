import os
import pandas as pd
import numpy as np

# Get the current working directory
path = os.getcwd()

# Read source CSV files
df_first_purchase = pd.read_csv(f"{path}/data/client_first_purchase_date.csv", low_memory=False)
df_events = pd.read_csv(f"{path}/data/events.csv", low_memory=False)
df_campaigns = pd.read_csv(f"{path}/data/campaigns.csv", low_memory=False)
df_messages = pd.read_csv(f"{path}/data/messages.csv", low_memory=False)
df_friends = pd.read_csv(f"{path}/data/friends.csv", low_memory=False)

# Function to extract user_id and device_id from client_id.
# Assumes client_id = "151591562" + <9-digit user_id> + <device_id>
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

# --- Process Users table ---
# Get user_id from both client_first_purchase and messages (if missing in first purchase)
df_first_purchase['user_id'] = df_first_purchase['client_id'].apply(lambda x: extract_ids(x)[0])
df_messages['user_id'] = df_messages['client_id'].apply(lambda x: extract_ids(x)[0])
# Combine unique user_ids from both dataframes
users_from_first_purchase = df_first_purchase[['user_id']]
users_from_messages = df_messages[['user_id']]
users_df = pd.concat([users_from_first_purchase, users_from_messages]).drop_duplicates()
# Select only user_id as per schema
users_df = users_df[['user_id']]
users_df.to_csv(f"{path}/cleaned_data/Users.csv", index=False)

# --- Process UserDevices table ---
# Extract device info along with email_provider from messages.csv and merge first_purchase_date from client_first_purchase.csv
df_messages['device_id'] = df_messages['client_id'].apply(lambda x: extract_ids(x)[1])
user_devices_df = df_messages[['client_id', 'user_id', 'device_id', 'platform', 'email_provider', 'stream']].drop_duplicates()
# Convert first_purchase_date to datetime in df_first_purchase
df_first_purchase['first_purchase_date'] = pd.to_datetime(df_first_purchase['first_purchase_date'], errors='coerce')
first_purchase_df = df_first_purchase[['client_id', 'first_purchase_date']].drop_duplicates()
# Merge to include first_purchase_date; some client_ids might not be present in client_first_purchase
user_devices_df = user_devices_df.merge(first_purchase_df, on='client_id', how='left')

# --- Remove duplicates in UserDevices ---
# Calculate non-null count per row and keep the row with maximum non-null values for each client_id
user_devices_df['non_nulls'] = user_devices_df.notnull().sum(axis=1)
user_devices_df = user_devices_df.loc[user_devices_df.groupby('client_id')['non_nulls'].idxmax()]
user_devices_df = user_devices_df.drop(columns='non_nulls')

user_devices_df.to_csv(f"{path}/cleaned_data/UserDevices.csv", index=False)

# --- Process UserFriends table ---
# Assume friends.csv has two columns; rename them if necessary
df_friends.columns = ['user_id_1', 'user_id_2']
# Enforce uniqueness by sorting each row so that the lower id is first
df_friends[['user_id_1', 'user_id_2']] = np.sort(df_friends[['user_id_1', 'user_id_2']], axis=1)
user_friends_df = df_friends.drop_duplicates()
user_friends_df.to_csv(f"{path}/cleaned_data/UserFriends.csv", index=False)

# --- Process Categories table ---
# From events.csv extract unique category_id and category_code
categories_df = df_events[['category_id', 'category_code']]
# Sort so that non-null category_code appears first
categories_df = categories_df.sort_values(by='category_code', na_position='last')
# Drop duplicates based on category_id, keeping the row with non-null category_code if available
categories_df = categories_df.drop_duplicates(subset=['category_id'], keep='first')
categories_df.to_csv(f"{path}/cleaned_data/Categories.csv", index=False)

# --- Process Products table ---
# UPDATED: Add new column 'id' which is a combination of product_id and category_id, because product_id isn't unique.
products_df = df_events[['product_id', 'brand', 'category_id']].drop_duplicates()
products_df['id'] = products_df.apply(lambda row: f"{row['product_id']}_{row['category_id']}", axis=1)
# Reorder columns as per schema: id, product_id, brand, category_id
products_df = products_df[['id', 'product_id', 'brand', 'category_id']]

# --- Remove duplicates in Products ---
# Calculate non-null count per row and keep the row with maximum non-null values for each id
products_df['non_nulls'] = products_df.notnull().sum(axis=1)
products_df = products_df.loc[products_df.groupby('id')['non_nulls'].idxmax()]
products_df = products_df.drop(columns='non_nulls')

products_df.to_csv(f"{path}/cleaned_data/Products.csv", index=False)

# --- Process Events table ---
events_df = df_events.copy()
events_df['event_time'] = pd.to_datetime(events_df['event_time'], errors='coerce')
# Ensure user_id exists in events_df (if not, extract it)
if 'user_id' not in events_df.columns:
    events_df['user_id'] = events_df['client_id'].apply(lambda x: extract_ids(x)[0])
# Select required columns including price
events_df = events_df[['event_time', 'event_type', 'product_id', 'user_id', 'user_session', 'price']]
events_df.to_csv(f"{path}/cleaned_data/Events.csv", index=False)

# --- Process Campaigns table ---
# Rename 'id' column to campaign_id and generate campaign_type_id as combination of campaign_type and campaign_id
campaigns_df = df_campaigns.rename(columns={'id': 'campaign_id'})
# Force position as int type where possible (fill NaN with 0) using nullable integer type
campaigns_df['position'] = campaigns_df['position'].astype('Int64')
# Create campaign_type_id as f"{campaign_type}_{campaign_id}"
campaigns_df['campaign_type_id'] = campaigns_df['campaign_type'].astype(str) + '_' + campaigns_df['campaign_id'].astype(str)
campaigns_table = campaigns_df[['campaign_type_id', 'campaign_id', 'campaign_type', 'channel', 'topic', 'position']].drop_duplicates()
campaigns_table.to_csv(f"{path}/cleaned_data/Campaigns.csv", index=False)

# --- Process Messages table ---
# Convert datetime columns in messages dataframe
datetime_cols = ['sent_at', 'opened_first_time_at', 'opened_last_time_at', 
                 'clicked_first_time_at', 'clicked_last_time_at', 'unsubscribed_at', 
                 'hard_bounced_at', 'soft_bounced_at', 'complained_at', 'blocked_at', 'purchased_at']
for col in datetime_cols:
    if col in df_messages.columns:
        df_messages[col] = pd.to_datetime(df_messages[col], errors='coerce')
# Create campaign_type_id in messages using message_type and campaign_id (similar to campaigns table)
df_messages['campaign_type_id'] = df_messages['message_type'].astype(str) + '_' + df_messages['campaign_id'].astype(str)
# Select required columns as per schema: message_id, client_id, campaign_type_id, and datetime columns
messages_table = df_messages[['message_id', 'client_id', 'campaign_type_id', 'sent_at', 
                              'opened_first_time_at', 'opened_last_time_at', 'clicked_first_time_at', 
                              'clicked_last_time_at', 'unsubscribed_at', 'hard_bounced_at', 
                              'soft_bounced_at', 'complained_at', 'blocked_at', 'purchased_at']]
messages_table.to_csv(f"{path}/cleaned_data/Messages.csv", index=False)

# --- Process BulkCampaignAttributes table (Bulk campaigns only) ---
bulk_campaigns = campaigns_df[campaigns_df['campaign_type'] == 'bulk']
bulk_attr_df = bulk_campaigns[['campaign_type_id', 'started_at', 'finished_at', 'total_count', 
                               'ab_test', 'warmup_mode', 'hour_limit', 'is_test']].drop_duplicates()
bulk_attr_df['started_at'] = pd.to_datetime(bulk_attr_df['started_at'], errors='coerce')
bulk_attr_df['finished_at'] = pd.to_datetime(bulk_attr_df['finished_at'], errors='coerce')
bulk_attr_df['total_count'] = bulk_attr_df['total_count'].astype('Int64')
bulk_attr_df.to_csv(f"{path}/cleaned_data/BulkCampaignAttributes.csv", index=False)

# --- Process CampaignSubjectAttributes table ---
subject_attr_df = campaigns_df[['campaign_type_id', 'subject_length', 'subject_with_personalization', 
                                'subject_with_deadline', 'subject_with_emoji', 'subject_with_bonuses', 
                                'subject_with_discount', 'subject_with_saleout']].drop_duplicates()
subject_attr_df['subject_length'] = subject_attr_df['subject_length'].astype('Int64')
subject_attr_df.to_csv(f"{path}/cleaned_data/CampaignSubjectAttributes.csv", index=False)

print('Done: Files written to cleaned_data directory')
