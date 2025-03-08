const fs = require('fs');

use("ecommerce");

// Drop the existing database
db.dropDatabase();

function importJsonToCollection(filePath, collectionName) {
  const file = fs.readFileSync(filePath, 'utf8');
  const collection = db.getCollection(collectionName);
  let jsonData = JSON.parse(file);

  if (Array.isArray(jsonData)) {
    jsonData.forEach(doc => {
      // For collections that expect an ObjectId (_id as ObjectId), add it if not already present.
      if ((collectionName === 'users' || collectionName === 'events') && !doc.hasOwnProperty('_id')) {
        doc._id = ObjectId();
      }
      // Convert extended JSON date (if present) to a Date object for events
      if (collectionName === 'events' && doc.event_time && doc.event_time.$date) {
        doc.event_time = new Date(doc.event_time.$date);
      }
      // For messages: if sent_at is in extended JSON format, convert it to Date
      if (collectionName === 'messages' && doc.sent_at && doc.sent_at.$date) {
        doc.sent_at = new Date(doc.sent_at.$date);
      }
    });
    collection.insertMany(jsonData);
  } else {
    if ((collectionName === 'users' || collectionName === 'events') && !jsonData.hasOwnProperty('_id')) {
      jsonData._id = ObjectId();
    }
    collection.insertOne(jsonData);
  }
  console.log(`=== ${filePath} imported into ${collectionName} ===`);
}

print("=== Importing JSON files ===");
importJsonToCollection("project/cleaned_data/Users.json", "users");
importJsonToCollection("project/cleaned_data/Campaigns.json", "campaigns");
importJsonToCollection("project/cleaned_data/Events.json", "events");
importJsonToCollection("project/cleaned_data/Messages_chunk_1.json", "messages");
importJsonToCollection("project/cleaned_data/Messages_chunk_2.json", "messages");
importJsonToCollection("project/cleaned_data/Messages_chunk_3.json", "messages");
importJsonToCollection("project/cleaned_data/Messages_chunk_4.json", "messages");
importJsonToCollection("project/cleaned_data/Messages_chunk_5.json", "messages");
importJsonToCollection("project/cleaned_data/Messages_chunk_6.json", "messages");
importJsonToCollection("project/cleaned_data/Products.json", "products");

print("=== Creating Indexes ===");

// Create index on the campaigns collection for campaign_type_id.
db.getCollection("campaigns").createIndex({ campaign_type_id: 1 });
print("Created index on campaigns.campaign_type_id");

// Create indexes on the messages collection.
// These indexes are useful if you frequently query by campaign_id (for joins) or client_id.
db.getCollection("messages").createIndex({ campaign_id: 1 });
db.getCollection("messages").createIndex({ client_id: 1 });
print("Created indexes on messages.campaign_id and messages.client_id");

// Create indexes on the events collection.
// Indexing event_time and user_id can help speed up time-based queries or user lookups.
db.getCollection("events").createIndex({ event_time: 1 });
db.getCollection("events").createIndex({ user_id: 1 });
print("Created indexes on events.event_time and events.user_id");

// Create an index on the users collection.
db.getCollection("users").createIndex({ user_id: 1 });
print("Created index on users.user_id");

// Create an index on the products collection.
db.getCollection("products").createIndex({ product_id: 1 });
print("Created index on products.product_id");
