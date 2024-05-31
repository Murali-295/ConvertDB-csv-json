package com.mk.controller;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvValidationException;
import org.bson.Document;
import org.json.JSONObject;
import org.springframework.web.bind.annotation.*;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@RestController
public class CsvController {


    MongoClient mongoClient = MongoClients.create();
    MongoDatabase mongoDatabase=mongoClient.getDatabase("movies_csv");
    MongoCollection<Document> mongoCollection=mongoDatabase.getCollection("movies_csv");

    @GetMapping("/get")
    public String getData() throws IOException, CsvValidationException {
        CSVReader csvReader=new CSVReader(new FileReader("MOVIES.csv"));
        File file = new File("movies.json");
        HashMap<String, Object> map = new HashMap<>();

       String[] header= csvReader.readNext();
       String[] data;
        Writer writer = new FileWriter(file);

        while ((data= csvReader.readNext())!=null){
              Document document=new Document();

              for (int i=0;i<data.length;i++){
                  document.put(header[i],data[i]);
                  }
            mongoCollection.insertOne(document);
              writer.append(document.toJson());

       }
        writer.close();
        csvReader.close();
       return "Data stored...";
    }

    @PostMapping("/saveMovie")
    public String addMovie(@RequestBody Map<String,String> document) throws IOException {
        Document document1=new Document(document);
        mongoCollection.insertOne(document1);
        return "Movie added successfully...";
    }

    @GetMapping("/convertJson")
    public Map<String, Object> convertDBToJson() throws IOException {
        Map<String,Object> map=new HashMap<>();
        File file = new File("db_movies.json");
        FileWriter fileWriter=new FileWriter(file);

        JSONObject jsonObject=new JSONObject();
        for(Document document:mongoCollection.find()){
            document.remove("_id");
            HashMap<String,Object> hashMap=new HashMap<>();
            for (String string:document.keySet()){
            hashMap.put(string,document.get(string));
            }
            map.put(document.get("title").toString(),hashMap);
        }
        jsonObject.put("movie",map);
        Document document=new Document(jsonObject.toMap());
        fileWriter.append(document.toJson());
        return jsonObject.toMap();
    }
    @GetMapping("/DBToCsv")
    public String convertDBToCsv() throws IOException {
        File file=new File("db_movies.csv");
        CSVWriter csvWriter=new CSVWriter(new FileWriter(file));

        Document document=mongoCollection.find().first();
        document.remove("_id");
        JSONObject jsonObject=new JSONObject(document.toJson());
        System.out.println(jsonObject.keySet());
        ArrayList<String> list=new ArrayList<>();
        for (String string:jsonObject.keySet()){
            list.add(string);
        }
        String [] header=list.toArray(new String[0]);
        csvWriter.writeNext(header);

        for (Document document1:mongoCollection.find()){
            document.remove("_id");
            JSONObject jsonObject1=new JSONObject(document.toJson());
            ArrayList<String> list1=new ArrayList<>();
            for (String string:jsonObject1.keySet()){
                list1.add(jsonObject1.get(string).toString());
            }
            String [] data=list1.toArray(new String[0]);
            csvWriter.writeNext(data);
        }
        return "Data converted from db to csv...";
    }
}
