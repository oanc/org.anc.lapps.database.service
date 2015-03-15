package org.anc.lapps.database;

import org.junit.*;
import org.lappsgrid.api.WebService;
import org.lappsgrid.metadata.ServiceMetadata;
import org.lappsgrid.serialization.Data;
import org.lappsgrid.serialization.Serializer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.lappsgrid.discriminator.Discriminators.Uri;

/**
 * @author Keith Suderman
 */
public class DatabaseTests
{
	@Ignore
	public void testQuery()
	{
		WebService service = new DatabaseService();
		Map<String,String> params = new HashMap<>();
		params.put("username", "langrid");
		params.put("password", "langrid");

		Data<Map<String,String>> request = new Data<>(Uri.JSON, params);
		String result = service.execute(request.asJson());

		Data<?> data = Serializer.parse(result, Data.class);
		assertNotNull(data);
		System.out.println(data.getDiscriminator());
		System.out.println(data.getPayload().toString());
		assertEquals(data.getPayload().toString(), data.getDiscriminator(), Uri.JSON);
		Object payload = data.getPayload();
		System.out.println("Payload is a " + payload.getClass().getCanonicalName());
		List<Map<String,String>> list = (List) payload;
		for (Map<String,String> map : list)
		{
//			String s = String.format("%s:%s, %s, %s, %s", )
			System.out.print(map.get("gridid"));
			System.out.print(":");
			System.out.print(map.get("id"));
			System.out.print(", ");
			System.out.print(map.get("name"));
			System.out.print(", ");
			System.out.print(map.get("license"));
			System.out.print(", ");
			System.out.println(map.get("description"));
		}
	}

	@Test
	public void testMetadata()
	{
		WebService service = new DatabaseService();
		String json = service.getMetadata();
		assertNotNull(json);
		System.out.println(json);
		Data<ServiceMetadata> data = Serializer.parse(json, Data.class);
		assertEquals(Uri.META, data.getDiscriminator());
		ServiceMetadata metadata = new ServiceMetadata((Map)data.getPayload());
		assertEquals("http://www.anc.org", metadata.getVendor());
		assertEquals(Version.getVersion(), metadata.getVersion());
	}
}
