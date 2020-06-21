package com.zacharylincoln.ClusterDataStorageMasterNode;

import com.mongodb.client.*;
import com.mongodb.util.JSON;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;
import org.json.simple.JSONObject;

import javax.print.Doc;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.*;
import java.util.Iterator;

@SpringBootApplication(exclude= MongoAutoConfiguration.class)
public class ClusterDataStorageMasterNodeApplication {

	public static String mongoDBLink = "mongodb+srv://master:masterpassword@clusterdatastorage-zjtvv.mongodb.net/<dbname>?retryWrites=true&w=majority";

	public static void main(String[] args) {
		// start an async task to check all nodes to see if they are running every 5 sec. If a node is not running access the database to see what files are being hosted and copy files from backup nodes to another node. Remove all files from the down node.

		new Thread(() -> {
			while (true) {
					MongoClient mongoClient = MongoClients.create(ClusterDataStorageMasterNodeApplication.mongoDBLink);
					MongoDatabase database = mongoClient.getDatabase("ClusterDataStorage");

					// Retrieving a collection
					MongoCollection<Document> nodeCollection = database.getCollection("Nodes");
					FindIterable<Document> nodeIterDoc = nodeCollection.find();

					// Getting the iterator
					Iterator it = nodeIterDoc.iterator();
					while (it.hasNext()) {
						Document doc = (Document) it.next();
						String ip = (String) doc.get("ip");
						String files = (String) doc.get("files");
                        System.out.println("Doc: " + doc);
                        System.out.println("Files: " + files);
						try{
							URL api = new URL("http://"+ ip + "/checkIfUp");

							URLConnection apiConnection = api.openConnection();
							apiConnection.getInputStream();
							System.out.println("Node is active!");
						}catch (ConnectException e) {
							System.out.println("Node is no longer active!");

							// set a new node for the files in this node

							if(files != ""){
								String[] filesNeededToBeMoved = files.split(",");
								for (String fileNeededToBeMoved : filesNeededToBeMoved) {
									String fileNeededToBeMovedId = fileNeededToBeMoved.split("\\.")[0];
									String fileNeededToBeMovedDeletionCode = "";
									if (fileNeededToBeMoved.split("\\.").length == 2) {
										fileNeededToBeMovedDeletionCode = fileNeededToBeMoved.split("\\.")[1];
									}


									String nodeThatHasOriginal = "";
									String nodeThatHasOriginalDeletionCode = "";
									int sizeInBytes = 0;

									// find the ip of the original node

									Bson fileCondition = new Document("$eq", Integer.valueOf(fileNeededToBeMovedId));
									Bson fileFilter = new Document("id", fileCondition);

									MongoCollection<Document> fileCollection = database.getCollection("Files");

									System.out.println("File ID: " + fileNeededToBeMovedId);

									FindIterable<Document> fileIterDoc = fileCollection.find(fileFilter);
									Iterator fileIt = fileIterDoc.iterator();

									String main = "";
									String backup = "";
									boolean mainFailed = true;

									while (fileIt.hasNext()) {
										Document file = (Document) fileIt.next();
										System.out.println(file);
										main = file.get("main").toString();
										backup = file.get("backup").toString();
										sizeInBytes = Integer.valueOf(file.get("size").toString());

										if (ip.equals(main)) {
											nodeThatHasOriginal = backup;
											mainFailed = true;
										} else {
											nodeThatHasOriginal = main;
											mainFailed = false;
										}
										System.out.println("Backup: " + backup);
										System.out.println("Main: " + main);
									}
									System.out.println("Backup: " + backup);
									System.out.println("Main: " + main);
									// get the deletion code for the original file.

									Bson originalNodeCondition = new Document("$eq", nodeThatHasOriginal);
									Bson originalNodeFilter = new Document("ip", originalNodeCondition);
									System.out.println(nodeThatHasOriginal);
									FindIterable<Document> originalNodeIterDoc = nodeCollection.find(originalNodeFilter);
									Iterator originalNodeIt = originalNodeIterDoc.iterator();
									System.out.println("GYUFGYTUFYTRDRTDtr");
									while (originalNodeIt.hasNext()) {
										Document originalNode = (Document) originalNodeIt.next();
										System.out.println("Gyuftyfty");
										String originalNodeFiles = originalNode.get("files").toString();
										String[] orginalNodeFilesArray = originalNodeFiles.split(",");
										for (String originalNodeFile : orginalNodeFilesArray) {
											if (originalNodeFile.split("\\.")[0].equals(fileNeededToBeMovedId)) {
												nodeThatHasOriginalDeletionCode = originalNodeFile.split("\\.")[1];
											}
										}
									}

									try {
										System.out.println(nodeThatHasOriginal);
										JSONObject base64JSON = ClusterDataStorageMasterNodeApplication.download(nodeThatHasOriginal, fileNeededToBeMovedId, nodeThatHasOriginalDeletionCode);
										String base64String = base64JSON.get("Base64").toString();
										String[] newNodeIp = ClusterDataStorageMasterNodeController.getNodes(1, sizeInBytes);

										while (newNodeIp[0] == (mainFailed ? main : backup)) {
											newNodeIp = ClusterDataStorageMasterNodeController.getNodes(1, sizeInBytes);
										}


										// Update the file document
										fileCondition = new Document("$eq", Integer.valueOf(fileNeededToBeMovedId));
										fileFilter = new Document("id", fileCondition);

										System.out.println("File ID: " + fileNeededToBeMovedId);

										fileIterDoc = fileCollection.find(fileFilter);
										fileIt = fileIterDoc.iterator();
										while (fileIt.hasNext()) {
											Document oldFile = (Document) fileIt.next();

											Document newFile = new Document();
											newFile.put("id", Integer.valueOf(oldFile.get("id").toString()));
											newFile.put(mainFailed ? "main" : "backup", newNodeIp[0]);
											newFile.put(mainFailed ? "backup" : "main", oldFile.get(mainFailed ? "backup" : "main"));
											newFile.put("size", sizeInBytes);

											fileCollection.deleteOne(oldFile);

											System.out.println(newFile);
											fileCollection.insertOne(newFile);
										}


										// Upload the file to the new node
										ClusterDataStorageMasterNodeApplication.upload(newNodeIp[0], Integer.valueOf(fileNeededToBeMovedId), base64String, fileNeededToBeMovedDeletionCode, sizeInBytes);


									} catch (IOException ioException) {
										ioException.printStackTrace();
									} catch (ParseException parseException) {
										parseException.printStackTrace();
									}
								}


							}

							Bson condition = new Document("$eq",ip);
							Bson filter = new Document("ip", condition);
							try {
								nodeCollection.deleteOne(filter);
								System.out.println("Deleted inactive node");
							}catch (Exception k){
								k.printStackTrace();
							}


						}catch (Exception e){
							System.out.println("Node is active!");
						}






					}

				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				mongoClient.close();
			}

		}).start();





		SpringApplication.run(ClusterDataStorageMasterNodeApplication.class, args);
	}


	public static JSONObject download(String ip, String id, String deletionCode) throws IOException, ParseException {
		String masterNodeIp = "http://" + ip + "/downloadFile?fileID=" + id + "&deletionCode=" + deletionCode;

		URL master = new URL(masterNodeIp);

		org.json.simple.JSONObject json = new org.json.simple.JSONObject();
		JSONParser parser = new JSONParser();

		URLConnection masterConnection = master.openConnection();
		BufferedReader in = new BufferedReader(
				new InputStreamReader(
						masterConnection.getInputStream()));
		String inputLine;

		while ((inputLine = in.readLine()) != null)
			json = (org.json.simple.JSONObject) parser.parse(inputLine);
		in.close();

		return json;
	}

	public static void upload(String ip, int id, String bytes, String deletionCode, int size) throws IOException, ParseException {
		String masterNodeIp = "http://" + ip + "/uploadFile?fileID=" + id + "&bytes=" + bytes.replaceAll(" ", "+") + "&sizeInBytes=" + size + "&deletionCode=" + deletionCode;

		System.out.println(masterNodeIp);

		URL master = new URL(masterNodeIp);
		//-----------------
		HttpURLConnection httpCon = (HttpURLConnection) master.openConnection();
		httpCon.setDoOutput(true);
		httpCon.setRequestMethod("POST");
		OutputStreamWriter out = new OutputStreamWriter(
				httpCon.getOutputStream());
		System.out.println(httpCon.getResponseCode());
		System.out.println(httpCon.getResponseMessage());
		out.close();
	}

}
