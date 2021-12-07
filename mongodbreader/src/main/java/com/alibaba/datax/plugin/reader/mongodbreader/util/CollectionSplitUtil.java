package com.alibaba.datax.plugin.reader.mongodbreader.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.mongodbreader.KeyConstant;
import com.alibaba.datax.plugin.reader.mongodbreader.MongoDBReaderErrorCode;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Strings;
import com.mongodb.MongoClient;
import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by jianying.wcj on 2015/3/19 0019.
 * Modified by mingyan.zc on 2016/6/13.
 * Modified by mingyan.zc on 2017/7/5.
 */
public class CollectionSplitUtil {

    public static List<Configuration> doSplit(
        Configuration originalSliceConfig, int adviceNumber, MongoClient mongoClient) {

        List<Configuration> confList = new ArrayList<Configuration>();

        String dbName = originalSliceConfig.getString(KeyConstant.MONGO_DB_NAME, originalSliceConfig.getString(KeyConstant.MONGO_DATABASE));

        String collName = originalSliceConfig.getString(KeyConstant.MONGO_COLLECTION_NAME);

        if(Strings.isNullOrEmpty(dbName) || Strings.isNullOrEmpty(collName) || mongoClient == null) {
            throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE,
                MongoDBReaderErrorCode.ILLEGAL_VALUE.getDescription());
        }

        //若是聚合，则按聚合的规则来切分
        if (StringUtils.isNotBlank(originalSliceConfig.getString(KeyConstant.AGG_PRE))) {
            return doSplitAgg(adviceNumber, mongoClient, dbName, collName, originalSliceConfig);
        }

        boolean isObjectId = isPrimaryIdObjectId(mongoClient, dbName, collName);

        List<Range> rangeList = doSplitCollection(adviceNumber, mongoClient, dbName, collName, isObjectId);
        for(Range range : rangeList) {
            Configuration conf = originalSliceConfig.clone();
            conf.set(KeyConstant.LOWER_BOUND, range.lowerBound);
            conf.set(KeyConstant.UPPER_BOUND, range.upperBound);
            conf.set(KeyConstant.IS_OBJECTID, isObjectId);
            confList.add(conf);
        }
        return confList;
    }


    private static boolean isPrimaryIdObjectId(MongoClient mongoClient, String dbName, String collName) {
        MongoDatabase database = mongoClient.getDatabase(dbName);
        MongoCollection<Document> col = database.getCollection(collName);
        Document doc = col.find().limit(1).first();
        Object id = doc.get(KeyConstant.MONGO_PRIMARY_ID);
        if (id instanceof ObjectId) {
            return true;
        }
        return false;
    }

    // split the collection into multiple chunks, each chunk specifies a range
    private static List<Range> doSplitCollection(int adviceNumber, MongoClient mongoClient,
                                                 String dbName, String collName, boolean isObjectId) {

        MongoDatabase database = mongoClient.getDatabase(dbName);
        List<Range> rangeList = new ArrayList<Range>();
        if (adviceNumber == 1) {
            Range range = new Range();
            range.lowerBound = "min";
            range.upperBound = "max";
            return Arrays.asList(range);
        }

        Document result = database.runCommand(new Document("collStats", collName));
        int docCount = result.getInteger("count");
        if (docCount == 0) {
            return rangeList;
        }
        int avgObjSize = 1;
        Object avgObjSizeObj = result.get("avgObjSize");
        if (avgObjSizeObj instanceof Integer) {
            avgObjSize = ((Integer) avgObjSizeObj).intValue();
        } else if (avgObjSizeObj instanceof Double) {
            avgObjSize = ((Double) avgObjSizeObj).intValue();
        }
        int splitPointCount = adviceNumber - 1;
        int chunkDocCount = docCount / adviceNumber;
        ArrayList<Object> splitPoints = new ArrayList<Object>();

        // test if user has splitVector role(clusterManager)
        boolean supportSplitVector = true;
        try {
            database.runCommand(new Document("splitVector", dbName + "." + collName)
                .append("keyPattern", new Document(KeyConstant.MONGO_PRIMARY_ID, 1))
                .append("force", true));
        } catch (MongoCommandException e) {
            if (e.getErrorCode() == KeyConstant.MONGO_UNAUTHORIZED_ERR_CODE ||
                e.getErrorCode() == KeyConstant.MONGO_ILLEGALOP_ERR_CODE) {
                supportSplitVector = false;
            }
        }

        if (supportSplitVector) {
            boolean forceMedianSplit = false;
            int maxChunkSize = (docCount / splitPointCount - 1) * 2 * avgObjSize / (1024 * 1024);
            //int maxChunkSize = (chunkDocCount - 1) * 2 * avgObjSize / (1024 * 1024);
            if (maxChunkSize < 1) {
                forceMedianSplit = true;
            }
            if (!forceMedianSplit) {
                result = database.runCommand(new Document("splitVector", dbName + "." + collName)
                    .append("keyPattern", new Document(KeyConstant.MONGO_PRIMARY_ID, 1))
                    .append("maxChunkSize", maxChunkSize)
                    .append("maxSplitPoints", adviceNumber - 1));
            } else {
                result = database.runCommand(new Document("splitVector", dbName + "." + collName)
                    .append("keyPattern", new Document(KeyConstant.MONGO_PRIMARY_ID, 1))
                    .append("force", true));
            }
            ArrayList<Document> splitKeys = result.get("splitKeys", ArrayList.class);

            for (int i = 0; i < splitKeys.size(); i++) {
                Document splitKey = splitKeys.get(i);
                Object id = splitKey.get(KeyConstant.MONGO_PRIMARY_ID);
                if (isObjectId) {
                    ObjectId oid = (ObjectId)id;
                    splitPoints.add(oid.toHexString());
                } else {
                    splitPoints.add(id);
                }
            }
        } else {
            int skipCount = chunkDocCount;
            MongoCollection<Document> col = database.getCollection(collName);

            for (int i = 0; i < splitPointCount; i++) {
                Document doc = col.find().skip(skipCount).limit(chunkDocCount).first();
                Object id = doc.get(KeyConstant.MONGO_PRIMARY_ID);
                if (isObjectId) {
                    ObjectId oid = (ObjectId)id;
                    splitPoints.add(oid.toHexString());
                } else {
                    splitPoints.add(id);
                }
                skipCount += chunkDocCount;
            }
        }

        Object lastObjectId = "min";
        for (Object splitPoint : splitPoints) {
            Range range = new Range();
            range.lowerBound = lastObjectId;
            lastObjectId = splitPoint;
            range.upperBound = lastObjectId;
            rangeList.add(range);
        }
        Range range = new Range();
        range.lowerBound = lastObjectId;
        range.upperBound = "max";
        rangeList.add(range);

        return rangeList;
    }

    private static List<Configuration> doSplitAgg(int adviceNumber, MongoClient mongoClient,
                                                  String dbName, String collName, Configuration originalSliceConfig) {
        List<Configuration> confList = new ArrayList<>();
        String query = originalSliceConfig.getString(KeyConstant.AGG_QUERY);
        originalSliceConfig.set(KeyConstant.IS_OBJECTID, false);
        originalSliceConfig.set(KeyConstant.LOWER_BOUND, "min");
        originalSliceConfig.set(KeyConstant.UPPER_BOUND, "max");
        if (StringUtils.isBlank(query)) {
            Configuration conf = originalSliceConfig.clone();
            confList.add(conf);
            return confList;
        }
        if (adviceNumber == 1) {
            Configuration conf = originalSliceConfig.clone();
            conf.set(KeyConstant.AGG_SKIP, 0);
            conf.set(KeyConstant.AGG_LIMIT, 1000000);
            confList.add(conf);
            return confList;
        }

        MongoDatabase database = mongoClient.getDatabase(dbName);
        //获取mongo聚合的explain语句
        String pre = originalSliceConfig.getString(KeyConstant.AGG_PRE);
        Document parse = Document.parse(pre);
        Document document = database.runCommand(parse);

        //获取command的执行结果，得到其nReturned做为分页的依据
        JSONObject jsonObject = JSONObject.parseObject(document.toJson());
        JSONArray stages = jsonObject.getJSONArray("stages");
        Integer num = -1;
        if (stages == null) {
            Configuration conf = originalSliceConfig.clone();
            confList.add(conf);
            return confList;
        }
        AtomicReference<JSONObject> cursor = new AtomicReference<>();
        stages.forEach(o -> {
            if (((JSONObject)o).containsKey("$cursor")) {
                cursor.set(((JSONObject) o).getJSONObject("$cursor"));
            }
        });
        if (cursor.get() != null) {
            JSONObject executionStats = cursor.get().getJSONObject("executionStats");
            num = executionStats.getInteger("nReturned");
        }
        if(num <= 20000) {
            Configuration conf = originalSliceConfig.clone();
            confList.add(conf);
            return confList;
        }

        int dvc = num / adviceNumber;
        for (int i = 0;i < adviceNumber - 1;i++) {
            Configuration conf = originalSliceConfig.clone();
            conf.set(KeyConstant.AGG_SKIP, i * dvc);
            conf.set(KeyConstant.AGG_LIMIT, dvc);
            confList.add(conf);
        }

        return confList;
    }
}

class Range {
    Object lowerBound;
    Object upperBound;
}
