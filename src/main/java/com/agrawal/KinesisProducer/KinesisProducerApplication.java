package com.agrawal.KinesisProducer;


import com.agrawal.KinesisProducer.aws.AwsKinesisClient;
import com.agrawal.KinesisProducer.model.Order;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

public class KinesisProducerApplication {

	List<String> productList = new ArrayList<>();
	Random random = new Random();

	public static void main(String[] args) {

		KinesisProducerApplication app = new KinesisProducerApplication();
		app.populateProductList();

		//1. getClient
		AmazonKinesis kinesisClient = AwsKinesisClient.getKinesisClient();

		//2. PutRecordRequest
		List<PutRecordsRequestEntry> recordsRequestList = app.getRecordsRequestList();
		PutRecordsRequest recordsRequest = new PutRecordsRequest();
		recordsRequest.setStreamName("");
		recordsRequest.setRecords(recordsRequestList);

		//3. putRecord or putRecords - 500 records in 1 batch
		PutRecordsResult result = kinesisClient.putRecords(recordsRequest);
		System.out.println(result); // failed record = 0 means successful

		//4. retry mechanism
		Integer failedRecordCount = result.getFailedRecordCount();

		List<PutRecordsResultEntry> records = result.getRecords();

		for(PutRecordsResultEntry recordsResultEntry : records) {
			if(recordsResultEntry.getErrorCode() != null ){
				//there is some problem
			}
		}
	}

	private List<PutRecordsRequestEntry> getRecordsRequestList(){
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		List<PutRecordsRequestEntry> putRecordsRequestEntries = new ArrayList<>();

		for(Order order: getOrderList()){
			PutRecordsRequestEntry requestEntry = new PutRecordsRequestEntry();
			requestEntry.setData(ByteBuffer.wrap(gson.toJson(order).getBytes()));
			requestEntry.setPartitionKey(UUID.randomUUID().toString());
			putRecordsRequestEntries.add(requestEntry);
		}

		return putRecordsRequestEntries;
	}

	private void populateProductList(){
		productList.add("Shirt1");
		productList.add("Shirt2");
		productList.add("Shirt3");
		productList.add("Shirt4");
		productList.add("Shirt5");
		productList.add("Shirt6");
		productList.add("Shirt7");
	}

	private List<Order> getOrderList(){
		List<Order> orders = new ArrayList<>();

		for (int i=0;i<500;i++){
			Order order = new Order();
			order.setOrderId(random.nextInt());
			order.setProduct(productList.get(random.nextInt(productList.size())));
			order.setQuantity(random.nextInt(20));
			orders.add(order);
		}

		return orders;
	}
}
