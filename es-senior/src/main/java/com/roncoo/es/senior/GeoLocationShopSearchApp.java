package com.roncoo.es.senior;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

/**
 * 比如我们有很多的4s店，然后呢给了用户一个app，在某个地方的时候，可以根据当前的地理位置搜索一下，自己附近的4s店
 * @author 41241
 * 第一个需求：搜索两个坐标点组成的一个区域
 * 第二个需求：指定一个区域，由三个坐标点，组成，比如上海大厦，东方明珠塔，上海火车站
 * 第三个需求：搜索距离当前位置在200公里内的4s店
 *
 */
public class GeoLocationShopSearchApp {

	@SuppressWarnings({ "unchecked", "resource" })
	public static void main(String[] args) throws Exception {
		
		Settings settings = Settings.builder()
				.put("cluster.name", "elasticsearch")
				.build();
		
		TransportClient client = new PreBuiltTransportClient(settings)
				.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
		
		// 第一个需求：搜索两个坐标点组成的一个区域
		SearchResponse searchResponse=client.prepareSearch("car_shop")
				.setTypes("shops")
				.setQuery(QueryBuilders.geoBoundingBoxQuery("pin.location")
						.setCorners(40.73, -74.1, 40.01, -71.12))
				.get();
		for(SearchHit searchHit : searchResponse.getHits().getHits()) {
			System.out.println(searchHit.getSourceAsString());  
		}
		
		System.out.println("====================================================");
		
		//指定一个区域，由三个坐标点，组成，比如上海大厦，东方明珠塔，上海火车站
		List<GeoPoint> points = new ArrayList<GeoPoint>();             
		points.add(new GeoPoint(40.73, -74.1));
		points.add(new GeoPoint(40.01, -71.12));
		points.add(new GeoPoint(50.56, -90.58));

		searchResponse = client.prepareSearch("car_shop")
				.setTypes("shops")
				.setQuery(QueryBuilders.geoPolygonQuery("pin.location", points))  
				.get();
		
		for(SearchHit searchHit : searchResponse.getHits().getHits()) {
			System.out.println(searchHit.getSourceAsString());  
		}
		
		System.out.println("====================================================");
		
		//第三个需求：搜索距离当前位置在200公里内的4s店
		searchResponse = client.prepareSearch("car_shop")
				.setTypes("shops")
				.setQuery(QueryBuilders.geoDistanceQuery("pin.location")
						.point(40, -70)
						.distance(200, DistanceUnit.KILOMETERS))  
				.get();
		
		for(SearchHit searchHit : searchResponse.getHits().getHits()) {
			System.out.println(searchHit.getSourceAsString());  
		}
		
		client.close();
	}

}
