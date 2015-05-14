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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteResult;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoClientOptions;
import com.mongodb.InsertOptions;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

/**
 * MongoDB client for YCSB framework.
 *
 * Properties to set:
 *
 * mongodb.url=mongodb://localhost:27017 mongodb.database=ycsb
 * mongodb.writeConcern=acknowledged
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

            try {

                MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
                builder.cursorFinalizerEnabled(false);
                builder.connectionsPerHost(Integer.parseInt(maxConnections));
                builder.writeConcern(writeConcern);
                builder.readPreference(readPreference);

                String[] server = urls.split(",");
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
<<<<<<< HEAD
        if (initCount.decrementAndGet() == 0) {
             for (int i=0;i<mongo.length;i++) { 
                try {
                   mongo[i].close(); 
               } catch (Exception e1) { /* ignore */ }
=======
        if (initCount.decrementAndGet() <= 0) {
            try {
                for (int i=0;i<mongo.length;i++) mongo[i].close();
            }
            catch (Exception e1) {
                System.err.println("Could not close MongoDB connection pool: "
                        + e1.toString());
                e1.printStackTrace();
                return;
>>>>>>> parent of e73dbf7... fix connection pool size and cleanup code
            }
        }
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
            r.put(k, values.get(k).toArray());
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
                fieldsToSet.put(tmpKey, values.get(tmpKey).toArray());

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
