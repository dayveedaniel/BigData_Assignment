-- Enable the OrioleDB extension
CREATE EXTENSION orioledb;

------------------------------------
-- Schema and ecommerce table definitions --
------------------------------------

CREATE SCHEMA IF NOT EXISTS ecommerce;
SET search_path TO ecommerce, public;

CREATE TABLE IF NOT EXISTS ecommerce.Users (
    user_id bigint PRIMARY KEY NOT NULL
) USING orioledb;

CREATE TABLE IF NOT EXISTS ecommerce.UserFriends (
    user_id_1 bigint NOT NULL,
    user_id_2 bigint NOT NULL,
    UNIQUE (user_id_1, user_id_2),
    CONSTRAINT fk_Users_user_id_to_UserFriends_user_id_1 FOREIGN KEY (user_id_1) REFERENCES ecommerce.Users (user_id),
    CONSTRAINT fk_Users_user_id_to_UserFriends_user_id_2 FOREIGN KEY (user_id_2) REFERENCES ecommerce.Users (user_id)
) USING orioledb;

CREATE INDEX userfriends_user_id_1_idx ON ecommerce.UserFriends(user_id_1);
CREATE INDEX userfriends_user_id_2_idx ON ecommerce.UserFriends(user_id_2);

CREATE TABLE IF NOT EXISTS ecommerce.Events (
    event_time timestamp,
    event_type varchar,
    product_id bigint,
    user_id bigint NOT NULL,
    user_session uuid,
    price money,
    CONSTRAINT fk_Users_user_id_to_Events_user_id FOREIGN KEY (user_id) REFERENCES ecommerce.Users (user_id)
) USING orioledb;


-- Categories table with an index on category_code
CREATE TABLE IF NOT EXISTS ecommerce.Categories (
    category_id bigint PRIMARY KEY NOT NULL,
    category_code varchar
) USING orioledb;

CREATE INDEX categories_category_code_idx ON ecommerce.Categories(category_code);

-- Products table with an index on product_id
CREATE TABLE IF NOT EXISTS ecommerce.Products (
    id varchar PRIMARY KEY NOT NULL,
    product_id bigint NOT NULL,
    brand varchar,
    category_id bigint,
    CONSTRAINT fk_Categories_category_id_to_Products_category_id FOREIGN KEY (category_id) REFERENCES ecommerce.Categories (category_id)
) USING orioledb;

CREATE INDEX products_product_id_idx ON ecommerce.Products(product_id);

-- Campaigns table with an index on channel
CREATE TABLE IF NOT EXISTS ecommerce.Campaigns (
    campaign_type_id varchar PRIMARY KEY NOT NULL,
    campaign_id bigint NOT NULL,
    campaign_type varchar NOT NULL,
    channel varchar NOT NULL,
    topic varchar,
    position integer
) USING orioledb;

CREATE INDEX campaigns_channel_idx ON ecommerce.Campaigns(channel);

-- UserDevices table with an index on user_id (foreign key)
CREATE TABLE IF NOT EXISTS ecommerce.UserDevices (
    client_id bigint PRIMARY KEY NOT NULL,
    user_id bigint NOT NULL,
    device_id bigint,
    first_purchase_date timestamp,
    platform varchar,
    email_provider varchar,
    stream varchar,
    CONSTRAINT fk_Users_user_id_to_UserDevices_user_id FOREIGN KEY (user_id) REFERENCES ecommerce.Users (user_id)
) USING orioledb;

CREATE INDEX userdevices_user_id_idx ON ecommerce.UserDevices(user_id);

-- Messages table with an index on sent_at for message time filtering
CREATE TABLE IF NOT EXISTS ecommerce.Messages (
    message_id varchar PRIMARY KEY NOT NULL,
    client_id bigint NOT NULL,
    campaign_type_id varchar,
    sent_at timestamp,
    opened_first_time_at timestamp,
    opened_last_time_at timestamp,
    clicked_first_time_at timestamp,
    clicked_last_time_at timestamp,
    unsubscribed_at timestamp,
    hard_bounced_at timestamp,
    soft_bounced_at timestamp,
    complained_at timestamp,
    blocked_at timestamp,
    purchased_at timestamp,
    CONSTRAINT fk_UserDevices_client_id_to_Messages_client_id FOREIGN KEY (client_id) REFERENCES ecommerce.UserDevices (client_id),
    CONSTRAINT fk_Campaigns_campaign_type_id_to_Messages_campaign_type_id FOREIGN KEY (campaign_type_id) REFERENCES ecommerce.Campaigns (campaign_type_id)
) USING orioledb;

CREATE INDEX messages_sent_at_idx ON ecommerce.Messages(sent_at);

-- BulkCampaignAttributes table with an index on started_at
CREATE TABLE IF NOT EXISTS ecommerce.BulkCampaignAttributes (
    campaign_type_id varchar PRIMARY KEY NOT NULL,
    started_at timestamp,
    finished_at timestamp,
    total_count bigint,
    ab_test boolean,
    warmup_mode boolean,
    hour_limit real,
    is_test boolean,
    CONSTRAINT fk_Campaigns_campaign_type_id_to_BulkCampaignAttributes_campaign_type_id FOREIGN KEY (campaign_type_id) REFERENCES ecommerce.Campaigns (campaign_type_id)
) USING orioledb;

CREATE INDEX bulkcampaignattributes_started_at_idx ON ecommerce.BulkCampaignAttributes(started_at);

-- CampaignSubjectAttributes table with an index on subject_length
CREATE TABLE IF NOT EXISTS ecommerce.CampaignSubjectAttributes (
    campaign_type_id varchar PRIMARY KEY NOT NULL,
    subject_length bigint,
    subject_with_personalization boolean,
    subject_with_deadline boolean,
    subject_with_emoji boolean,
    subject_with_bonuses boolean,
    subject_with_discount boolean,
    subject_with_saleout boolean,
    CONSTRAINT fk_Campaigns_campaign_type_id_to_CampaignSubjectAttributes_campaign_type_id FOREIGN KEY (campaign_type_id) REFERENCES ecommerce.Campaigns (campaign_type_id)
) USING orioledb;

CREATE INDEX campaignsubjectattributes_subject_length_idx ON ecommerce.CampaignSubjectAttributes(subject_length);

------------------------------------
-- Data Loading Commands (unchanged) --
------------------------------------

\COPY ecommerce.Users(user_id) FROM 'project/cleaned_data/Users.csv' WITH (FORMAT csv, HEADER true);
\COPY ecommerce.UserDevices(client_id, user_id, device_id, platform, email_provider, stream, first_purchase_date) FROM 'project/cleaned_data/UserDevices.csv' WITH (FORMAT csv, HEADER true);
\COPY ecommerce.UserFriends(user_id_1, user_id_2) FROM 'project/cleaned_data/UserFriends.csv' WITH (FORMAT csv, HEADER true);
\COPY ecommerce.Categories(category_id, category_code) FROM 'project/cleaned_data/Categories.csv' WITH (FORMAT csv, HEADER true);
\COPY ecommerce.Products(id, product_id, brand, category_id) FROM 'project/cleaned_data/Products.csv' WITH (FORMAT csv, HEADER true);
\COPY ecommerce.Events(event_time, event_type, product_id, user_id, user_session, price) FROM 'project/cleaned_data/Events.csv' WITH (FORMAT csv, HEADER true);
\COPY ecommerce.Campaigns(campaign_type_id, campaign_id, campaign_type, channel, topic, position) FROM 'project/cleaned_data/Campaigns.csv' WITH (FORMAT csv, HEADER true);
\COPY ecommerce.Messages(message_id, client_id, campaign_type_id, sent_at, opened_first_time_at, opened_last_time_at, clicked_first_time_at, clicked_last_time_at, unsubscribed_at, hard_bounced_at, soft_bounced_at, complained_at, blocked_at, purchased_at) FROM 'project/cleaned_data/Messages.csv' WITH (FORMAT csv, HEADER true);
\COPY ecommerce.BulkCampaignAttributes(campaign_type_id, started_at, finished_at, total_count, ab_test, warmup_mode, hour_limit, is_test) FROM 'project/cleaned_data/BulkCampaignAttributes.csv' WITH (FORMAT csv, HEADER true);
\COPY ecommerce.CampaignSubjectAttributes(campaign_type_id, subject_length, subject_with_personalization, subject_with_deadline, subject_with_emoji, subject_with_bonuses, subject_with_discount, subject_with_saleout) FROM 'project/cleaned_data/CampaignSubjectAttributes.csv' WITH (FORMAT csv, HEADER true);
