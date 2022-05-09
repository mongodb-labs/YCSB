/*
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

import com.mongodb.AutoEncryptionSettings;
import com.mongodb.ClientEncryptionSettings;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.vault.DataKeyOptions;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.client.vault.ClientEncryption;
import com.mongodb.client.vault.ClientEncryptions;
import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.Document;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

class UuidUtils {

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
@SuppressWarnings({"UnnecessaryToStringCall", "StringConcatenationInsideStringBufferAppend"})
public class MongoDbClient extends DB {

    /** Used to include a field in a response. */
    protected static final Integer INCLUDE = 1;

    /** A singleton MongoClient instance. */
    private static MongoClient[] mongo;

    private static MongoDatabase[] db;                               

    private static int serverCounter = 0;

    /** The default write concern for the test. */
    private static WriteConcern writeConcern;

    /** The default read preference for the test */
    private static ReadPreference readPreference;

    /** Allow inserting batches to save time during load */
    private static Integer BATCHSIZE;
    private List<Document> insertList = null;
    private Integer insertCount = 0;

    /** The database to access. */
    private static String database;

    /** Count the number of times initialized to teardown on the last {@link #cleanup()}. */
    private static final AtomicInteger initCount = new AtomicInteger(0);

    /** Measure of how compressible the data is, compressibility=10 means the data can compress tenfold.
     *  The default is 1, which is uncompressible */
    private static float compressibility = (float) 1.0;

    private static String datatype = "binData";

    private static final String algorithm = "AEAD_AES_256_CBC_HMAC_SHA_512-Random";

    private static String generateSchema(String keyId, int numFields) {
        StringBuilder schema = new StringBuilder();

        schema.append(
            "{" +
            "  properties: {" );

        for(int i =0; i < numFields; i++) {
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
                "        algorithm: \"" + algorithm + "\"" +
                "      }" +
                "    },");
        }

        schema.append(
            "  }," +
            "  \"bsonType\": \"object\"" +
            "}");

        return schema.toString();
    }

    private static String generateRemoteSchema(String keyId, int numFields) {
        return "{ $jsonSchema : " + generateSchema(keyId, numFields) + "}";
    }

    private static synchronized String getDataKeyOrCreate(MongoCollection<BsonDocument> keyCollection, ClientEncryption clientEncryption ) {
        BsonDocument findFilter = new BsonDocument();
        BsonDocument keyDoc = keyCollection.find(findFilter).first();
 
        String base64DataKeyId;
        if (keyDoc == null ) {
            BsonBinary dataKeyId = clientEncryption.createDataKey("local", new DataKeyOptions());
            base64DataKeyId = Base64.getEncoder().encodeToString(dataKeyId.getData());
        } else {
            UUID dataKeyId = keyDoc.getBinary("_id").asUuid();
            base64DataKeyId = Base64.getEncoder().encodeToString(UuidUtils.asBytes(dataKeyId));
        }
 
        return base64DataKeyId;
    }

    private static AutoEncryptionSettings generateEncryptionSettings(String url, Boolean remote_schema, int numFields) {
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
            Collections.singletonMap("local", Collections.singletonMap("key", localMasterKey));

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

        MongoClient vaultClient = MongoClients.create(keyVaultUrls);

        final MongoCollection<BsonDocument> keyCollection = vaultClient.getDatabase(database).getCollection(keyVaultNamespace, BsonDocument.class);

        String base64DataKeyId = getDataKeyOrCreate(keyCollection, clientEncryption);

        String collName = "usertable";
        AutoEncryptionSettings.Builder autoEncryptionSettingsBuilder = AutoEncryptionSettings.builder()
            .keyVaultNamespace(keyVaultNamespace)
            .extraOptions(Collections.singletonMap("mongocryptdBypassSpawn", true) )
            .kmsProviders(kmsProviders);

        autoEncryptionSettingsBuilder.schemaMap(Collections.singletonMap(database + "." + collName,
            // Need a schema that references the new data key
            BsonDocument.parse(generateSchema(base64DataKeyId, numFields))
        ));

        AutoEncryptionSettings autoEncryptionSettings = autoEncryptionSettingsBuilder.build();

        if (remote_schema) {
            com.mongodb.client.MongoClient client = com.mongodb.client.MongoClients.create(keyVaultUrls);
            CreateCollectionOptions options = new CreateCollectionOptions();
            options.getValidationOptions().validator(BsonDocument.parse(generateRemoteSchema(base64DataKeyId, numFields)));
            try {
                client.getDatabase(database).createCollection(collName,  options);
            } catch (com.mongodb.MongoCommandException e) {
                // if this is load phase, then should error, if it's run then should ignore
                // how to tell properly?
                if (client.getDatabase(database).getCollection(collName).estimatedDocumentCount() <= 0) {
                   System.err.println("ERROR: Failed to create collection " + collName + " with error " + e);
                   e.printStackTrace();
                   System.exit(1);
                }
            }
        }

        return autoEncryptionSettings;
    }

    /**
     * Initialize any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    @Override
    public void init() {
        initCount.incrementAndGet();
        synchronized (INCLUDE) {
            if (mongo != null) {
                return;
            }

            // initialize MongoDb driver
            Properties props = getProperties();
            String urls = props.getProperty("mongodb.url", "mongodb://localhost:27017");

            database = props.getProperty("mongodb.database", "ycsb");
            /* Credentials */
            String username = props.getProperty("mongodb.username", "");
            String password = props.getProperty("mongodb.password", "");

            // Set insert batchsize, default 1 - to be YCSB-original equivalent
            final String batchSizeString = props.getProperty("batchsize", "1");
            BATCHSIZE = Integer.parseInt(batchSizeString);

            // allow "string" in addition to "byte" array for data type
            datatype = props.getProperty("datatype","binData");

            final String compressibilityString = props.getProperty("compressibility", "1");
            compressibility = Float.parseFloat(compressibilityString);

            // Set connectionpool to size of ycsb thread pool
            final String maxConnections = props.getProperty("threadcount", "100");

            String writeConcernType = props.getProperty("mongodb.writeConcern",
                    "acknowledged").toLowerCase();
            switch (writeConcernType) {
                case "unacknowledged":
                    writeConcern = WriteConcern.UNACKNOWLEDGED;
                    break;
                case "acknowledged":
                    writeConcern = WriteConcern.ACKNOWLEDGED;
                    break;
                case "journaled":
                    writeConcern = WriteConcern.JOURNALED;
                    break;
                case "replica_acknowledged":
                    writeConcern = WriteConcern.W2;
                    break;
                case "majority":
                    writeConcern = WriteConcern.MAJORITY;
                    break;
                default:
                    System.err.println("ERROR: Invalid writeConcern: '"
                            + writeConcernType
                            + "'. "
                            + "Must be [ unacknowledged | acknowledged | journaled | replica_acknowledged | majority ]");
                    System.exit(1);
            }

            // readPreference
            String readPreferenceType = props.getProperty("mongodb.readPreference", "primary").toLowerCase();
            switch (readPreferenceType) {
                case "primary":
                    readPreference = ReadPreference.primary();
                    break;
                case "primary_preferred":
                    readPreference = ReadPreference.primaryPreferred();
                    break;
                case "secondary":
                    readPreference = ReadPreference.secondary();
                    break;
                case "secondary_preferred":
                    readPreference = ReadPreference.secondaryPreferred();
                    break;
                case "nearest":
                    readPreference = ReadPreference.nearest();
                    break;
                default:
                    System.err.println("ERROR: Invalid readPreference: '"
                            + readPreferenceType
                            + "'. Must be [ primary | primary_preferred | secondary | secondary_preferred | nearest ]");
                    System.exit(1);
            }

            // encryption - FLE
            boolean use_encryption = Boolean.parseBoolean(props.getProperty("mongodb.fle", "false"));
            boolean remote_schema = Boolean.parseBoolean(props.getProperty("mongodb.remote_schema", "false"));
            int numEncryptFields = Integer.parseInt(props.getProperty("mongodb.numFleFields", "10"));

            try {
                MongoClientSettings.Builder settingsBuilder = MongoClientSettings.builder();
                // Need to use a larger connection pool to talk to mongocryptd/keyvault
                if (use_encryption) {
                    settingsBuilder.applyToConnectionPoolSettings(builder -> builder.maxSize(Integer.parseInt(maxConnections) * 3));
                } else {
                    settingsBuilder.applyToConnectionPoolSettings(builder -> builder.maxSize(Integer.parseInt(maxConnections)));
                }
                settingsBuilder.writeConcern(writeConcern);
                settingsBuilder.readPreference(readPreference);

                String userPassword = username.equals("") ? "" : username + (password.equals("") ? "" : ":" + password) + "@";

                String[] server = urls.split("\\|"); // split on the "|" character
                mongo = new MongoClient[server.length];
                db = new MongoDatabase[server.length];

                for (int i=0; i<server.length; i++) {
                   String url= userPassword.equals("") ? server[i] : server[i].replace("://","://"+userPassword);
                   if ( i==0 && use_encryption) {
                       AutoEncryptionSettings autoEncryptionSettings = generateEncryptionSettings(url, remote_schema, numEncryptFields);
                       settingsBuilder.autoEncryptionSettings(autoEncryptionSettings);
                   }

                   // if mongodb:// prefix is present then this is MongoClientURI format
                   // combine with options to get MongoClient
                   if (url.startsWith("mongodb://") || url.startsWith("mongodb+srv://") ) {
                       settingsBuilder.applyConnectionString(new ConnectionString(url));
                       mongo[i] = MongoClients.create(settingsBuilder.build());

                       String dispURI = userPassword.equals("")
                               ? url
                               : url.replace(":" + userPassword, ":XXXXXX");
                       System.out.println("DEBUG mongo connection created to " + dispURI);
                   } else {
                       settingsBuilder.applyToClusterSettings(builder ->
                               builder.hosts(Collections.singletonList(new ServerAddress(url))));
                       mongo[i] = MongoClients.create(settingsBuilder.build());
                       System.out.println("DEBUG mongo server connection to " + mongo[i].toString());
                   }
                   db[i] = mongo[i].getDatabase(database);

                 }
            } catch (Exception e1) {
                System.err.println("Could not initialize MongoDB connection pool for Loader: " + e1);
                e1.printStackTrace();
                System.exit(1);
            }
        }
    }

    /**
     * Cleanup any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    @Override
    public void cleanup() {
        if (initCount.decrementAndGet() <= 0) {
            for (MongoClient mongoClient : mongo) {
                try {
                    mongoClient.close();
                } catch (Exception e1) { /* ignore */ }
            }
        }
    }

    private byte[] applyCompressibility(byte[] data){
        long string_length = data.length;

        long random_string_length = Math.round(string_length /compressibility);
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
            MongoCollection<Document> collection = db[serverCounter++%db.length].getCollection(table);
            Document q = new Document("_id", key);
            collection.deleteMany(q);
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
        MongoCollection<Document> collection = db[serverCounter++%db.length].getCollection(table);
        Document r = new Document("_id", key);
        for (String k : values.keySet()) {
            byte[] data = values.get(k).toArray();
            if (datatype.equals("string")) {
                r.put(k, new String(applyCompressibility(data)));
            } else {
                r.put(k,applyCompressibility(data));
            }
        }
        if (BATCHSIZE == 1 ) {
           try {
             collection.insertOne(r);
             return 0;
           }
           catch (Exception e) {
             System.err.println("Couldn't insert key " + key);
             e.printStackTrace();
             return 1;
           }
        }
        if (insertCount == 0) {
           insertList = new ArrayList<>(BATCHSIZE);
        }
        insertCount++;
        insertList.add(r);
        if (insertCount < BATCHSIZE) {
            return 0;
        } else {
           try {
             collection.insertMany(insertList);
             insertCount = 0;
             return 0;
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
    @SuppressWarnings({"unchecked", "rawtypes"})
    public int read(String table, String key, Set<String> fields,
            HashMap<String, ByteIterator> result) {
        try {
            MongoCollection<Document> collection = db[serverCounter++%db.length].getCollection(table);
            Document q = new Document("_id", key);
            Document fieldsToReturn;

            Document queryResult;
            if (fields != null) {
                fieldsToReturn = new Document();
                for (final String field : fields) {
                    fieldsToReturn.put(field, INCLUDE);
                }
                queryResult = collection.find(q).projection(fieldsToReturn).first();
            }
            else {
                queryResult = collection.find(q).first();
            }

            if (queryResult != null) {
                // TODO: this is wrong.  It is totally violating the expected type of the values in result, which is ByteIterator
                // TODO: somewhere up the chain this should be resulting in a ClassCastException
                result.putAll(new LinkedHashMap(queryResult));
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
            MongoCollection<Document> collection = db[serverCounter++%db.length].getCollection(table);
            Document q = new Document("_id", key);
            Document u = new Document();
            Document fieldsToSet = new Document();
            for (String tmpKey : values.keySet()) {
                byte[] data = values.get(tmpKey).toArray();
                if (datatype.equals("string")) {
                    fieldsToSet.put(tmpKey, new String(applyCompressibility(data)));
                } else {
                    fieldsToSet.put(tmpKey, applyCompressibility(data));
                }
            }
            u.put("$set", fieldsToSet);
            UpdateResult res = collection.updateOne(q, u);
            if (res.getMatchedCount() == 0) {
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
        MongoCursor<Document> cursor = null;
        try {
            MongoCollection<Document> collection = db[serverCounter++%db.length].getCollection(table);
            Document fieldsToReturn = null;
            // { "_id":{"$gte":startKey, "$lte":{"appId":key+"\uFFFF"}} }
            Document scanRange = new Document("$gte", startkey);
            Document q = new Document("_id", scanRange);
            Document s = new Document("_id",INCLUDE);
            if (fields != null) {
                fieldsToReturn = new Document();
                for (final String field : fields) {
                    fieldsToReturn.put(field, INCLUDE);
                }
            }
            cursor = collection.find(q).projection(fieldsToReturn).sort(s).limit(recordcount).cursor();
            if (!cursor.hasNext()) {
                System.err.println("Nothing found in scan for key " + startkey);
                return 1;
            }
            while (cursor.hasNext()) {
                // toMap() returns a Map, but result.add() expects a
                // Map<String,String>. Hence, the suppress warnings.
                HashMap<String, ByteIterator> resultMap = new HashMap<>();

                Document obj = cursor.next();
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
     * @param resultMap result map
     * @param document source document
     */
    protected void fillMap(HashMap<String, ByteIterator> resultMap, Document document) {
        for (Map.Entry<String, Object> entry : document.entrySet()) {
            if (entry.getValue() instanceof byte[]) {
                resultMap.put(entry.getKey(), new ByteArrayByteIterator(
                        (byte[]) entry.getValue()));
            }
        }
    }
}
