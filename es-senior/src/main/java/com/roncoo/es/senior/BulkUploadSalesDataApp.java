package com.roncoo.es.senior;

import java.net.InetAddress;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

/**
 * 业务场景：有一个汽车销售公司，拥有很多家4S店，这些4S店的数据
        都会在一段时间内陆续传递过来，汽车的销售数据，现在希望能够在内
        存中缓存比如1000条销售数据，然后一次性批量上传到es中去
 * @author 41241
 *
 */
public class BulkUploadSalesDataApp  {

	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception{
		Settings settings =Settings.builder()
				.put("cluster.name","elasticsearch")
				.build();
		@SuppressWarnings("resource")
		TransportClient client=new PreBuiltTransportClient(settings)
				.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"),9300));
		BulkRequestBuilder builRequestBuilder =client.prepareBulk();
		
		IndexRequestBuilder indexRequestBuilder=client.prepareIndex("car_shop","sales","3")
				.setSource(XContentFactory.jsonBuilder()
						.startObject()
						.field("brand", "奔驰")
						.field("name", "奔驰C200")
						.field("price", 350000)
						.field("produce_date", "2017-01-20")
						.field("sale_price", 320000)
						.field("sale_date", "2017-01-25")
					.endObject());
		builRequestBuilder.add(indexRequestBuilder);
		UpdateRequestBuilder updateRequestBuilder=client.prepareUpdate("car_shop","sales","1")
				.setDoc(XContentFactory.jsonBuilder()
						.startObject()
						.field("sale_price", 290000)
						.endObject());
		builRequestBuilder.add(updateRequestBuilder);
		DeleteRequestBuilder deleteRequestBuilder=client.prepareDelete("car_shop","sales","2");
		builRequestBuilder.add(deleteRequestBuilder);
		BulkResponse response=builRequestBuilder.get();
		for(BulkItemResponse item:response.getItems()) {
			System.out.println("version:"+item.getVersion());
			
		}
		client.close();

	}

}
