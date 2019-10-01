/**
 * MongoDB client binding for YCSB.
 *
 * Submitted by Yen Pai on 5/11/2010.
 *
 * https://gist.github.com/000a66b8db2caf42467b#file_mongo_db.java
 *
 * updated by MongoDB 3/18/2015
 *
 */

package com.yahoo.ycsb.db;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.UUID;

import com.mongodb.AutoEncryptionSettings;
import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteResult;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.vault.DataKeyOptions;
import com.mongodb.client.vault.ClientEncryption;
import com.mongodb.client.vault.ClientEncryptions;
import com.mongodb.ClientEncryptionSettings;
import com.mongodb.ConnectionString;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.InsertOptions;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoClientURI;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.Document;

import java.nio.ByteBuffer;

class UuidUtils {
    public static UUID asUuid(byte[] bytes) {
      ByteBuffer bb = ByteBuffer.wrap(bytes);
      long firstLong = bb.getLong();
      long secondLong = bb.getLong();
      return new UUID(firstLong, secondLong);
    }

    public static byte[] asBytes(UUID uuid) {
      ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
      bb.putLong(uuid.getMostSignificantBits());
      bb.putLong(uuid.getLeastSignificantBits());
      return bb.array();
    }
  }

/**
 * MongoDB client for YCSB framework.
 *
 * Properties to set:
 *
 * mongodb.url=mongodb://localhost:27017 mongodb.database=ycsb mongodb.writeConcern=acknowledged
 * For replica set use:
 * mongodb.url=mongodb://hostname:27017?replicaSet=nameOfYourReplSet
 * to pass connection to multiple mongos end points to round-robin between them, separate
 * hostnames with "|" character
 *
 * @author ypai
 */
public class MongoDbClient extends DB {

    /** Used to include a field in a response. */
    protected static final Integer INCLUDE = Integer.valueOf(1);

    /** A singleton MongoClient instance. */
    private static MongoClient[] mongo;

    private static com.mongodb.DB[] db;

    private static int serverCounter = 0;

    /** The default write concern for the test. */
    private static WriteConcern writeConcern;

    /** The default read preference for the test */
    private static ReadPreference readPreference;

    /** Allow inserting batches to save time during load */
    private static Integer BATCHSIZE;
    private List<DBObject> insertList = new ArrayList<DBObject>();
    private Integer insertCount = 0;
    private BulkWriteOperation bulkWriteOperation = null;

    /** The database to access. */
    private static String database;

    /** Count the number of times initialized to teardown on the last {@link #cleanup()}. */
    private static final AtomicInteger initCount = new AtomicInteger(0);

    private static InsertOptions io = new InsertOptions().continueOnError(true);

    /** Measure of how compressible the data is, compressibility=10 means the data can compress tenfold.
     *  The default is 1, which is uncompressible */
    private static float compressibility = (float) 1.0;

    private static String datatype = "binData";

    private static String generateSchema(String keyId) {
        StringBuilder schema = new StringBuilder();

        schema.append(
            "{" +
            "  properties: {" );

        for(int i =0; i < 10; i++) {
            schema.append(
                "    field" + i + ": {" +
                "      encrypt: {" +
                "        keyId: [{" +
                "          \"$binary\": {" +
                "            \"base64\": \"" + keyId + "\"," +
                "            \"subType\": \"04\"" +
                "          }" +
                "        }]," +
                "        bsonType: \"" + datatype  +  "\"," +
                "        algorithm: \"AEAD_AES_256_CBC_HMAC_SHA_512-Random\"" +
                "      }" +
                "    },");
        }

        schema.append(
            "  }," +
            "  \"bsonType\": \"object\"" +
            "}");

        return schema.toString();
    }

    private static String generateRemoteSchema(String keyId) {
        return "{ $jsonSchema : " + generateSchema(keyId) + "}";
    }

    private static synchronized String getDataKeyOrCreate(MongoCollection<Document> keyCollection, ClientEncryption clientEncryption ) {
        BsonDocument findFilter = new BsonDocument();
        Document keyDoc = keyCollection.find(findFilter).first();

        String base64DataKeyId;
        if(keyDoc == null ) {
            BsonBinary dataKeyId = clientEncryption.createDataKey("local", new DataKeyOptions());
            base64DataKeyId = Base64.getEncoder().encodeToString(dataKeyId.getData());
        } else {
            UUID dataKeyId = (UUID) keyDoc.get("_id");
            base64DataKeyId = Base64.getEncoder().encodeToString(UuidUtils.asBytes(dataKeyId));
        }

        return base64DataKeyId;
    }

    private static AutoEncryptionSettings generateEncryptionSettings(String url, Boolean remote_schema) {
        // Use a hard coded local key since it needs to be shared between load and run phases
        byte[] localMasterKey = new byte[]{0x77, 0x1f, 0x2d, 0x7d, 0x76, 0x74, 0x39, 0x08, 0x50, 0x0b, 0x61, 0x14,
            0x3a, 0x07, 0x24, 0x7c, 0x37, 0x7b, 0x60, 0x0f, 0x09, 0x11, 0x23, 0x65,
            0x35, 0x01, 0x3a, 0x76, 0x5f, 0x3e, 0x4b, 0x6a, 0x65, 0x77, 0x21, 0x6d,
            0x34, 0x13, 0x24, 0x1b, 0x47, 0x73, 0x21, 0x5d, 0x56, 0x6a, 0x38, 0x30,
            0x6d, 0x5e, 0x79, 0x1b, 0x25, 0x4d, 0x2a, 0x00, 0x7c, 0x0b, 0x65, 0x1d,
            0x70, 0x22, 0x22, 0x61, 0x2e, 0x6a, 0x52, 0x46, 0x6a, 0x43, 0x43, 0x23,
            0x58, 0x21, 0x78, 0x59, 0x64, 0x35, 0x5c, 0x23, 0x00, 0x27, 0x43, 0x7d,
            0x50, 0x13, 0x65, 0x3c, 0x54, 0x1e, 0x74, 0x3c, 0x3b, 0x57, 0x21, 0x1a};

        Map<String, Map<String, Object>> kmsProviders =
            Collections.singletonMap("local", Collections.singletonMap("key", (Object)localMasterKey));

        // Use the same database, admin is slow
        String keyVaultNamespace = database + ".datakeys";
        String keyVaultUrls = url;
        if (!keyVaultUrls.startsWith("mongodb")) {
            keyVaultUrls = "mongodb://" + keyVaultUrls;
        }

        ClientEncryptionSettings clientEncryptionSettings = ClientEncryptionSettings.builder()
        .keyVaultMongoClientSettings(MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(keyVaultUrls))
                .readPreference(readPreference)
                .writeConcern(writeConcern)
                .build())
        .keyVaultNamespace(keyVaultNamespace)
        .kmsProviders(kmsProviders)
        .build();

        ClientEncryption clientEncryption = ClientEncryptions.create(clientEncryptionSettings);

        MongoClient vaultClient = new MongoClient( new MongoClientURI(keyVaultUrls) );

        final MongoCollection<Document> keyCollection = vaultClient.getDatabase(database).getCollection(keyVaultNamespace);

        String base64DataKeyId = getDataKeyOrCreate(keyCollection, clientEncryption);

        String collName = "usertable";
        AutoEncryptionSettings.Builder autoEncryptionSettingsBuilder = AutoEncryptionSettings.builder()
            .keyVaultNamespace(keyVaultNamespace)
            .extraOptions(Collections.singletonMap("mongocryptdBypassSpawn", (Object)true) )
            .kmsProviders(kmsProviders);

        autoEncryptionSettingsBuilder.schemaMap(Collections.singletonMap(database + "." + collName,
            // Need a schema that references the new data key
            BsonDocument.parse(generateSchema(base64DataKeyId))
        ));

        AutoEncryptionSettings autoEncryptionSettings = autoEncryptionSettingsBuilder.build();

        if (remote_schema) {
            com.mongodb.client.MongoClient client = com.mongodb.client.MongoClients.create(keyVaultUrls);
            CreateCollectionOptions options = new CreateCollectionOptions();
            options.getValidationOptions().validator(BsonDocument.parse(generateRemoteSchema(base64DataKeyId)));
            try {
                client.getDatabase(database).createCollection(collName,  options);
            } catch (com.mongodb.MongoCommandException e) {
                System.err.println("ERROR: Failed to create collection " + collName + " with error "
                         + e.toString());
                e.printStackTrace();
                System.exit(1);
            }
        }

        return autoEncryptionSettings;
    }

    /**
     * Initialize any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    @Override
    public void init() throws DBException {
        initCount.incrementAndGet();
        synchronized (INCLUDE) {
            if (mongo != null) {
                return;
            }

            // initialize MongoDb driver
            Properties props = getProperties();
            String urls = props.getProperty("mongodb.url", "localhost:27017");

            database = props.getProperty("mongodb.database", "ycsb");

            // Set insert batchsize, default 1 - to be YCSB-original equivalent
            final String batchSizeString = props.getProperty("batchsize", "1");
            BATCHSIZE = Integer.parseInt(batchSizeString);

            // allow "string" in addition to "byte" array for data type
            final String datatypeString = props.getProperty("datatype","binData");
            this.datatype = datatypeString;

            final String compressibilityString = props.getProperty("compressibility", "1");
            this.compressibility = Float.parseFloat(compressibilityString);

            // Set connectionpool to size of ycsb thread pool
            final String maxConnections = props.getProperty("threadcount", "100");

            String writeConcernType = props.getProperty("mongodb.writeConcern",
                    "acknowledged").toLowerCase();
            if ("unacknowledged".equals(writeConcernType)) {
                writeConcern = WriteConcern.UNACKNOWLEDGED;
            }
            else if ("acknowledged".equals(writeConcernType)) {
                writeConcern = WriteConcern.ACKNOWLEDGED;
            }
            else if ("journaled".equals(writeConcernType)) {
                writeConcern = WriteConcern.JOURNALED;
            }
            else if ("replica_acknowledged".equals(writeConcernType)) {
                writeConcern = WriteConcern.REPLICA_ACKNOWLEDGED;
            }
            else if ("majority".equals(writeConcernType)) {
                writeConcern = WriteConcern.MAJORITY;
            }
            else {
                System.err.println("ERROR: Invalid writeConcern: '"
                                + writeConcernType
                                + "'. "
                                + "Must be [ unacknowledged | acknowledged | journaled | replica_acknowledged | majority ]");
                System.exit(1);
            }

            // readPreference
            String readPreferenceType = props.getProperty("mongodb.readPreference", "primary").toLowerCase();
            if ("primary".equals(readPreferenceType)) {
                readPreference = ReadPreference.primary();
            }
            else if ("primary_preferred".equals(readPreferenceType)) {
                readPreference = ReadPreference.primaryPreferred();
            }
            else if ("secondary".equals(readPreferenceType)) {
                readPreference = ReadPreference.secondary();
            }
            else if ("secondary_preferred".equals(readPreferenceType)) {
                readPreference = ReadPreference.secondaryPreferred();
            }
            else if ("nearest".equals(readPreferenceType)) {
                readPreference = ReadPreference.nearest();
            }
            else {
                System.err.println("ERROR: Invalid readPreference: '"
                                + readPreferenceType
                                + "'. Must be [ primary | primary_preferred | secondary | secondary_preferred | nearest ]");
                System.exit(1);
            }

            // encryption - FLE
            boolean use_encryption = Boolean.parseBoolean(props.getProperty("mongodb.fle", "false"));
            boolean remote_schema = Boolean.parseBoolean(props.getProperty("mongodb.remote_schema", "false"));

            try {
                MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
                builder.cursorFinalizerEnabled(false);
                // Need to use a larger connection pool to talk to mongocryptd/keyvault
                if (use_encryption) {
                    builder.connectionsPerHost(Integer.parseInt(maxConnections) * 3);
                } else {
                    builder.connectionsPerHost(Integer.parseInt(maxConnections));
                }
                builder.writeConcern(writeConcern);
                builder.readPreference(readPreference);

                if (use_encryption) {
                    AutoEncryptionSettings autoEncryptionSettings = generateEncryptionSettings(urls, remote_schema);
                    builder.autoEncryptionSettings(autoEncryptionSettings);
                }

                String[] server = urls.split("\\|"); // split on the "|" character
                mongo = new MongoClient[server.length];
                db = new com.mongodb.DB[server.length];
                for (int i=0; i<server.length; i++) {
                   String url=server[i];
                   System.err.println("Found server connection string " + url);
                   // if mongodb:// prefix is present then this is MongoClientURI format
                   // combine with options to get MongoClient
                   if (url.startsWith("mongodb://")) {
                       MongoClientURI uri = new MongoClientURI(url, builder);
                       mongo[i] = new MongoClient(uri);
                   } else {
                       mongo[i] = new MongoClient(new ServerAddress(url), builder.build());
                   }
                   db[i] = mongo[i].getDB(database);

                   System.out.println("mongo connection created with " + url);
                 }
            } catch (Exception e1) {
                System.err
                        .println("Could not initialize MongoDB connection pool for Loader: "
                                + e1.toString());
                e1.printStackTrace();
                return;
            }
        }
    }

    /**
     * Cleanup any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    @Override
    public void cleanup() throws DBException {
        if (initCount.decrementAndGet() <= 0) {
             for (int i=0;i<mongo.length;i++) {
                try {
                   mongo[i].close();
               } catch (Exception e1) { /* ignore */ }
            }
        }
    }

    private byte[] applyCompressibility(byte[] data){
        long string_length = data.length;

        long random_string_length = (int) Math.round(string_length /compressibility);
        long compressible_len = string_length - random_string_length;
        for(int i=0;i<compressible_len;i++)
            data[i] = 97;
        return data;
    }

    /**
     * Delete a record from the database.
     *
     * @param table The name of the table
     * @param key The record key of the record to delete.
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    @Override
    public int delete(String table, String key) {
        try {
            DBCollection collection = db[serverCounter++%db.length].getCollection(table);
            DBObject q = new BasicDBObject().append("_id", key);
            WriteResult res = collection.remove(q);
            return 0;
        }
        catch (Exception e) {
            System.err.println(e.toString());
            e.printStackTrace();
            return 1;
        }
    }

    /**
     * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
     * record key.
     *
     * @param table The name of the table
     * @param key The record key of the record to insert.
     * @param values A HashMap of field/value pairs to insert in the record
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    @Override
    public int insert(String table, String key,
            HashMap<String, ByteIterator> values) {
        DBCollection collection = db[serverCounter++%db.length].getCollection(table);
        DBObject r = new BasicDBObject().append("_id", key);
        for (String k : values.keySet()) {
            byte[] data = values.get(k).toArray();
            if (this.datatype.equals("string")) {
                r.put(k, new String(applyCompressibility(data)));
            } else {
                r.put(k,applyCompressibility(data));
            }
        }
        if (BATCHSIZE == 1 ) {
           try {
             WriteResult res = collection.insert(r);
             return 0;
           }
           catch (Exception e) {
             System.err.println("Couldn't insert key " + key);
             e.printStackTrace();
             return 1;
           }
        }
        if (insertCount == 0) {
           bulkWriteOperation = collection.initializeUnorderedBulkOperation();
        }
        insertCount++;
        bulkWriteOperation.insert(r);
        if (insertCount < BATCHSIZE) {
            return 0;
        } else {
           try {
             BulkWriteResult res = bulkWriteOperation.execute();
             if (res.getInsertedCount() == insertCount ) {
                 insertCount = 0;
                 return 0;
             }
             System.err.println("Number of inserted documents doesn't match the number sent, " + res.getInsertedCount() + " inserted, sent " + insertCount);
             return 1;
           }
           catch (Exception e) {
             System.err.println("Exception while trying bulk insert with " + insertCount);
             e.printStackTrace();
             return 1;
           }
        }
    }

    /**
     * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
     *
     * @param table The name of the table
     * @param key The record key of the record to read.
     * @param fields The list of fields to read, or null for all of them
     * @param result A HashMap of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error or "not found".
     */
    @Override
    @SuppressWarnings("unchecked")
    public int read(String table, String key, Set<String> fields,
            HashMap<String, ByteIterator> result) {
        try {
            DBCollection collection = db[serverCounter++%db.length].getCollection(table);
            DBObject q = new BasicDBObject().append("_id", key);
            DBObject fieldsToReturn = null;

            DBObject queryResult = null;
            if (fields != null) {
                fieldsToReturn = new BasicDBObject();
                Iterator<String> iter = fields.iterator();
                while (iter.hasNext()) {
                    fieldsToReturn.put(iter.next(), INCLUDE);
                }
                queryResult = collection.findOne(q, fieldsToReturn);
            }
            else {
                queryResult = collection.findOne(q);
            }

            if (queryResult != null) {
                result.putAll(queryResult.toMap());
                return 0;
            }
            System.err.println("No results returned for key " + key);
            return 1;
        }
        catch (Exception e) {
            System.err.println(e.toString());
            return 1;
        }
    }

    /**
     * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
     * record key, overwriting any existing values with the same field name.
     *
     * @param table The name of the table
     * @param key The record key of the record to write.
     * @param values A HashMap of field/value pairs to update in the record
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    @Override
    public int update(String table, String key,
            HashMap<String, ByteIterator> values) {
        try {
            DBCollection collection = db[serverCounter++%db.length].getCollection(table);
            DBObject q = new BasicDBObject().append("_id", key);
            DBObject u = new BasicDBObject();
            DBObject fieldsToSet = new BasicDBObject();
            Iterator<String> keys = values.keySet().iterator();
            while (keys.hasNext()) {
                String tmpKey = keys.next();
                byte[] data = values.get(tmpKey).toArray();
                if (this.datatype.equals("string")) {
                    fieldsToSet.put(tmpKey, new String(applyCompressibility(data)));
                } else {
                    fieldsToSet.put(tmpKey, applyCompressibility(data));
                }
            }
            u.put("$set", fieldsToSet);
            WriteResult res = collection.update(q, u);
            if (res.getN() == 0) {
                System.err.println("Nothing updated for key " + key);
                return 1;
            }
            return 0;
        }
        catch (Exception e) {
            System.err.println(e.toString());
            return 1;
        }
    }

    /**
     * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored in a HashMap.
     *
     * @param table The name of the table
     * @param startkey The record key of the first record to read.
     * @param recordcount The number of records to read
     * @param fields The list of fields to read, or null for all of them
     * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    @Override
    public int scan(String table, String startkey, int recordcount,
            Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        DBCursor cursor = null;
        try {
            DBCollection collection = db[serverCounter++%db.length].getCollection(table);
            DBObject fieldsToReturn = null;
            // { "_id":{"$gte":startKey, "$lte":{"appId":key+"\uFFFF"}} }
            DBObject scanRange = new BasicDBObject().append("$gte", startkey);
            DBObject q = new BasicDBObject().append("_id", scanRange);
            DBObject s = new BasicDBObject().append("_id",INCLUDE);
            if (fields != null) {
                fieldsToReturn = new BasicDBObject();
                Iterator<String> iter = fields.iterator();
                while (iter.hasNext()) {
                    fieldsToReturn.put(iter.next(), INCLUDE);
                }
            }
            cursor = collection.find(q, fieldsToReturn).sort(s).limit(recordcount);
            if (!cursor.hasNext()) {
                System.err.println("Nothing found in scan for key " + startkey);
                return 1;
            }
            while (cursor.hasNext()) {
                // toMap() returns a Map, but result.add() expects a
                // Map<String,String>. Hence, the suppress warnings.
                HashMap<String, ByteIterator> resultMap = new HashMap<String, ByteIterator>();

                DBObject obj = cursor.next();
                fillMap(resultMap, obj);

                result.add(resultMap);
            }

            return 0;
        }
        catch (Exception e) {
            System.err.println(e.toString());
            return 1;
        }
        finally {
             if( cursor != null ) {
                    cursor.close();
             }
        }

    }

    /**
     * TODO - Finish
     *
     * @param resultMap
     * @param obj
     */
    @SuppressWarnings("unchecked")
    protected void fillMap(HashMap<String, ByteIterator> resultMap, DBObject obj) {
        Map<String, Object> objMap = obj.toMap();
        for (Map.Entry<String, Object> entry : objMap.entrySet()) {
            if (entry.getValue() instanceof byte[]) {
                resultMap.put(entry.getKey(), new ByteArrayByteIterator(
                        (byte[]) entry.getValue()));
            }
        }
    }
}
