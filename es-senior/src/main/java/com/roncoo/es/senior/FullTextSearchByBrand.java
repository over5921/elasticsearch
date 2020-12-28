package com.roncoo.es.senior;

import java.net.InetAddress;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

/**
 * 全文搜索，精准搜索，前缀搜索
 * @author 41241
 *
 */
public class FullTextSearchByBrand {

	public static void main(String[] args) throws Exception{
		
		Settings settings =Settings.builder()
				.put("cluster.name","elasticsearch")
				.build();
		
		TransportClient client=new PreBuiltTransportClient(settings)
				.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"),9300));
		
		SearchResponse searchResponse=client.prepareSearch("car_shop")
				.setTypes("cars")
				.setQuery(QueryBuilders.matchQuery("brand", "宝马"))
				.get();
		for(SearchHit searchHit:searchResponse.getHits().getHits()) {
			System.out.println(searchHit.getSourceAsString());
		}
		System.out.println("====================================================");
		
		searchResponse=client.prepareSearch("car_shop")
				.setTypes("cars")
				.setQuery(QueryBuilders.multiMatchQuery("宝马", "brand","name"))
				.get();
		for(SearchHit searchHit:searchResponse.getHits().getHits()) {
			System.out.println(searchHit.getSourceAsString());
		}
		System.out.println("====================================================");
		
		searchResponse=client.prepareSearch("car_shop")
				.setTypes("cars")
				.setQuery(QueryBuilders.termQuery("name.raw", "宝马318"))
				.get();
		for(SearchHit searchHit:searchResponse.getHits().getHits()) {
			System.out.println(searchHit.getSourceAsString());
		}
		System.out.println("====================================================");
		
		searchResponse=client.prepareSearch("car_shop")
				.setTypes("cars")
				.setQuery(QueryBuilders.prefixQuery("name", "宝"))
				.get();
		for(SearchHit searchHit:searchResponse.getHits().getHits()) {
			System.out.println(searchHit.getSourceAsString());
		}
		System.out.println("====================================================");
		client.close();

	}

}
