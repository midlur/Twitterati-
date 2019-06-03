/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import kafka.utils.ShutdownableThread;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class Consumer extends ShutdownableThread {
    private final KafkaConsumer<Integer, String> consumer;
    private final String topic;
    private final DB database;

    public Consumer(String topic, DB database) {
        super("KafkaConsumerExample", false);
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoConsumer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        this.database = database;
        consumer = new KafkaConsumer<>(props);
        this.topic = topic;
    }

    @Override
    public void doWork() {
        consumer.subscribe(Collections.singletonList(this.topic));
        ConsumerRecords<Integer, String> records = consumer.poll(2);
        for (ConsumerRecord<Integer, String> record : records) {
            System.out.println("in here");
            System.out.println(record.value() + " this is it");
            String value = record.value();
             String[] strArr = value.trim().split("!@#%&");
             String tweet = strArr[0];
             String user = strArr[1];
             System.out.println("Hi");
            DBCollection table = database.getCollection("tweetdatabase");
            System.out.println("After getting db");
            BasicDBObject document = new BasicDBObject();
            document.put("_id",record.key());
            document.put("tweet",tweet);
            document.put("username",user);


            String mention = "";
            String hashtag = "";
           List<String> mentions = new ArrayList<>();
           List<String> hashtags = new ArrayList<>();
            System.out.println("Received message: " + record.key() + ", " + tweet);
            System.out.println(user + " username");
            String[] arr = tweet.trim().split(" ");
            for(int i=0;i<arr.length;i++){
                if(arr[i].startsWith("@")){
                    String newMention = arr[i];
                    if(!newMention.equals(mention)){
                        mention = newMention;
                        mentions.add(mention);
                    }
                }
                if(arr[i].startsWith("#")){
                    String newHashtag = arr[i];
                    if(!newHashtag.equals(hashtag)){
                        hashtag = newHashtag;
                        hashtags.add(hashtag);
                    }
                }
            }
            if(!mentions.isEmpty())
                System.out.println(mentions.toString() + " These are mentions for key : " + record.key() );
            if(!hashtags.isEmpty())
                System.out.println(hashtags.toString() + " These are hashtags for key : " + record.key() );
            document.put("Mentions",mentions);
            document.put("Hashtags",hashtags);
            table.insert(document);
        }





    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public boolean isInterruptible() {
        return false;
    }
}
