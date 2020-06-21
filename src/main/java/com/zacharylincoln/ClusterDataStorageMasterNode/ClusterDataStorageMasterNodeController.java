package com.zacharylincoln.ClusterDataStorageMasterNode;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.mongodb.client.*;
import org.bson.Document;

import org.bson.conversions.Bson;
import org.json.simple.JSONObject;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.print.Doc;
import javax.servlet.http.HttpServletRequest;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.*;

@RestController
public class ClusterDataStorageMasterNodeController {

    @RequestMapping("/getNodeIpForNewFile")
    public String getNodeIpForNewFile(@RequestParam Map<String,String> requestParams){
        int numOfSplits = Integer.valueOf(requestParams.get("numOfSplits"));
        int sizeOfEachSplitInBytes = Integer.valueOf(requestParams.get("sizeOfEachSplitInBytes"));

        String out = "";
        JSONObject nodeJSON = new JSONObject();
        for(String node : ClusterDataStorageMasterNodeController.getNodes(numOfSplits * 2, sizeOfEachSplitInBytes)){
            out += node + " ";
        }
        nodeJSON.put("nodes",out);
        System.out.println(nodeJSON.toJSONString());
        return nodeJSON.toJSONString();
    }

    @RequestMapping("/getNodeIp")
    public JSONObject getNodeIp(@RequestParam Map<String,String> requestParams){
        int fileId = Integer.valueOf(requestParams.get("fileId"));
        MongoClient mongoClient = MongoClients.create(ClusterDataStorageMasterNodeApplication.mongoDBLink);
        MongoDatabase database = mongoClient.getDatabase("ClusterDataStorage");

        // Retrieving a collection
        MongoCollection<Document> collection = database.getCollection("Files");

        Bson conditionID = new Document("$eq", fileId);
        Bson filterID = new Document("id", conditionID);

        // Gets the maxId Document
        FindIterable<Document> iterDoc =  collection.find(filterID);
        Iterator iterator = iterDoc.iterator();
        String main = "";
        String backup = "";
        while(iterator.hasNext()) {
            Document doc = (Document) iterator.next();
            main = doc.get("main").toString();
            backup = doc.get("backup").toString();
        }

        JSONObject json = new JSONObject();
        json.put("main", main);
        json.put("backup",backup);


        // send request to the database and fetch the record of the node's ips

        return json;
    }

    @RequestMapping("/setUpNode")
    public String setUpNode(@RequestParam Map<String,String> requestParams){
        int totalSpaceInBytes = Integer.valueOf(requestParams.get("totalSpaceInBytes"));
        int port = Integer.valueOf(requestParams.get("port"));

        HttpServletRequest request = ((ServletRequestAttributes) RequestContextHolder.currentRequestAttributes()).getRequest();
        String ip = request.getRemoteAddr() +":"+ port + "";

        addNode(ip + "", "Now", totalSpaceInBytes);
        System.out.println(ip);

        System.out.println(request.getHeader("origin"));


        return null;
    }

    @RequestMapping("/setUpFile")
    public JSONObject setUpFile(@RequestParam Map<String,String> requestParams){
        String main = requestParams.get("main");
        String backup = requestParams.get("backup");
        String size = requestParams.get("size");

        // Logs into database
        MongoClient mongoClient = MongoClients.create(ClusterDataStorageMasterNodeApplication.mongoDBLink);
        MongoDatabase database = mongoClient.getDatabase("ClusterDataStorage");

        // Retrieving a collection
        MongoCollection<Document> collection = database.getCollection("Files");

        Bson conditionID = new Document("$eq", "maxId");
        Bson filterID = new Document("id", conditionID);

        // Gets the maxId Document
        FindIterable<Document> iterDoc =  collection.find(filterID);
        Iterator iterator = iterDoc.iterator();
        int max = 0;
        while(iterator.hasNext()) {
            Document doc = (Document) iterator.next();
            // Gets the max id
            max = Integer.valueOf(doc.get("maxId").toString());
            doc.replace("maxId", max+1);
            collection.deleteOne(filterID);
            collection.insertOne(doc);
            break;
        }

        // Creates a new document with the max id + 1
        Document document = new Document("title", "MongoDB")
                .append("id", (max+1))
                .append("main", main)
                .append("backup", backup)
                .append("size", size);

        collection.insertOne(document);

        JSONObject fileJSON = new JSONObject();


        fileJSON.put("id", max + 1);

        mongoClient.close();
        return fileJSON;
    }

    @RequestMapping("/setUpFileNode")
    public JSONObject setUpFileNode(@RequestParam Map<String,String> requestParams){
        //Id with deltion code in format of id.deletioncode ie: 153.473429875489
        String id = requestParams.get("id");
        int bytes = Integer.valueOf(requestParams.get("bytes"));
        int port = Integer.valueOf(requestParams.get("port"));

        HttpServletRequest request = ((ServletRequestAttributes) RequestContextHolder.currentRequestAttributes()).getRequest();
        String ip = request.getRemoteAddr() +":"+ port;

        System.out.println(request.getHeader("origin"));




        MongoClient mongoClient = MongoClients.create(ClusterDataStorageMasterNodeApplication.mongoDBLink);
        MongoDatabase database = mongoClient.getDatabase("ClusterDataStorage");

        // Retrieving a collection
        MongoCollection<Document> nodeCollection = database.getCollection("Nodes");

        Bson conditionIp = new Document("$eq", ip);
        Bson filterIp = new Document("ip", conditionIp);

        // Gets the node Document
        FindIterable<Document> iterDoc =  nodeCollection.find(filterIp);
        Iterator iterator = iterDoc.iterator();

        while(iterator.hasNext()) {
            Document doc = (Document) iterator.next();
            nodeCollection.deleteOne(doc);

            String files = doc.get("files").toString();
            files += id + ",";
            doc.replace("files", files);

            int takenSpace = Integer.valueOf(doc.get("takenSpace").toString());
            takenSpace += bytes;
            doc.replace("takenSpace", takenSpace);

            nodeCollection.insertOne(doc);
        }



        //nodeCollection.findOneAndReplace();
        return null;


    }


    public static void addNode(String ip, String time, int totalSpaceInBytes){
        MongoClient mongoClient = MongoClients.create("mongodb+srv://master:masterpassword@clusterdatastorage-zjtvv.mongodb.net/<dbname>?retryWrites=true&w=majority");
        MongoDatabase database = mongoClient.getDatabase("ClusterDataStorage");

        // Retrieving a collection
        MongoCollection<Document> collection = database.getCollection("Nodes");


        Document document = new Document("title", "MongoDB")
                .append("ip", ip)
                .append("files", "")
                .append("totalSpace", totalSpaceInBytes)
                .append("takenSpace", 0)
                .append("time", time);

        //Inserting document into the collection
        collection.insertOne(document);

        System.out.println("Document inserted successfully");

        mongoClient.close();
    }

    public static String[] getNodes(int numOfNodes, int sizeInBytesNeeded){
        // Logs onto the mongo db
        MongoClient mongoClient = MongoClients.create(ClusterDataStorageMasterNodeApplication.mongoDBLink);
        MongoDatabase database = mongoClient.getDatabase("ClusterDataStorage");

        // Retrieving a collection
        MongoCollection<Document> nodeCollection = database.getCollection("Nodes");
        FindIterable<Document> nodeIterDoc = nodeCollection.find();

        // Getting the iterator
        Iterator it = nodeIterDoc.iterator();

        List<Object> ids = new ArrayList<>();

        // Getting all ids of the nodes
        while (it.hasNext()) {
            Document doc = (Document) it.next();
            if(Integer.valueOf(doc.get("totalSpace").toString()) - Integer.valueOf(doc.get("takenSpace").toString()) > sizeInBytesNeeded) {
                ids.add(doc.get("_id"));
            }
            System.out.println(doc.get("_id"));
            //}

        }

        Object[] nodesId = new Object[numOfNodes];

        // Picks at random nodes that are online
        String indexs = "";
        for(int i = 0; i < numOfNodes; i++){
            Random rand = new Random();
            int index = rand.nextInt(ids.size());

            boolean stop =  false;
            while(!stop){
                if(!indexs.contains(index + "")){
                    System.out.println(index);
                    indexs += (index);
                    stop = true;
                }else {
                    index = rand.nextInt(ids.size());
                }
            }

            nodesId[i] = ids.get(index);
        }

        String[] nodes = new String[numOfNodes];
        int i = 0;
        for(Object id : nodesId){
            Bson nodeId = new Document("$eq", id);
            Bson filterFile = new Document("_id", nodeId);
            FindIterable<Document> fileIterDoc =  nodeCollection.find(filterFile);
            Iterator nodeIt = fileIterDoc.iterator();
            while(nodeIt.hasNext()) {
                Document nodeDoc = (Document) nodeIt.next();
                String ip = (String) nodeDoc.get("ip");
                nodes[i] = ip;
                i++;
            }
        }
        return nodes;
    }
}
