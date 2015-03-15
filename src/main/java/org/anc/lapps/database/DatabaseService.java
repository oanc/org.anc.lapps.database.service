package org.anc.lapps.database;

import org.lappsgrid.api.WebService;
import org.lappsgrid.core.DataFactory;
//import org.lappsgrid.experimental.annotations.ServiceMetadata;
import org.lappsgrid.metadata.IOSpecification;
import org.lappsgrid.metadata.ServiceMetadata;
import org.lappsgrid.serialization.Serializer;
import org.lappsgrid.serialization.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.lappsgrid.discriminator.Discriminators.Uri;

/**
 * @author Keith Suderman
 */
//@ServiceMetadata(
//		  vendor = "http://www.anc.org",
//		  license = "Apache 2.0",
//		  produces_encoding = "UTF-8",
//		  produces_format = "json"
//)
public class DatabaseService implements WebService
{
	private static final String DATABASE_URL = "jdbc:postgresql://localhost/langrid";
	private final Logger logger = LoggerFactory.getLogger(DatabaseService.class);
	private String metadata;

	public DatabaseService()
	{
		loadMetadata();
		try
		{
			Class.forName("org.postgresql.Driver");
		}
		catch (ClassNotFoundException e)
		{
			logger.error("Unable to load the database driver.", e);
		}
	}

	private void loadMetadata()
	{
//		String path = "/metadata/" + this.getClass().getName() + ".json";
//		InputStream inputStream = this.getClass().getResourceAsStream(path);
//		if (inputStream == null)
//		{
//			metadata = DataFactory.error("Unable to load metadata");
//		}

		ServiceMetadata metadata = new ServiceMetadata();
		metadata.setAllow(Uri.ANY);
		metadata.setVendor("http://www.anc.org");
		metadata.setVersion(Version.getVersion());
		metadata.setDescription("Returns a list of services installed to this node.");
		metadata.setLicense(Uri.APACHE2);
		IOSpecification produces = metadata.getProduces();
		List<String> formats = new ArrayList<>();
		formats.add(Uri.JSON);
		produces.setFormat(formats);
		produces.setEncoding("UTF-8");
		this.metadata = new Data<ServiceMetadata>(Uri.META, metadata).asPrettyJson();
	}

	@Override
	public String getMetadata()
	{
		return metadata;
	}


	public String execute(String input)
	{
		logger.debug("Request received.");
		Map<String, String> params = null;
		Data data = null;
		try
		{
			data = Serializer.parse(input, Data.class);
		}
		catch (Exception e)
		{
			logger.error("Invalid request", e);
			return DataFactory.error("INVALID JSON: unable to parse input.");
		}

		Object payload = data.getPayload();
		logger.debug("Payload is a {}", payload.getClass().getName());
		params = (Map) payload;
		String username = params.get("username");
		if (username == null)
		{
			logger.error("Request did not contain a username");
			return DataFactory.error("No username provided.");
		}
		String password = params.get("password");
		if (password == null)
		{
			logger.error("Request did not contain a password");
			return DataFactory.error("No password provided.");
		}

		Connection connection = null;
		Statement statement = null;
		List<Map> list = new ArrayList<>();
		try
		{
			connection = DriverManager.getConnection(DATABASE_URL, username, password);
			statement = connection.createStatement();
			ResultSet result = statement.executeQuery("select * from service");
			logger.debug("Query executed.");
			while (result.next())
			{
				String id = result.getString("SERVICEID");
				logger.debug("Processing row for {}", id);
				Map<String,String> row = new HashMap<>();
				row.put("id", id);
				row.put("gridid", result.getString("GRIDID"));
				row.put("name", result.getString("SERVICENAME"));
				row.put("description", result.getString("SERVICEDESCRIPTION"));
				row.put("license", result.getString("LICENSEINFO"));
				list.add(row);
			}
		}
		catch (SQLException e)
		{
			logger.error("Unable to access the database.", e);
			return DataFactory.error("Unable to access the database", e);
		}
		finally
		{
			if (statement != null) try {
				statement.close();
			}
			catch (SQLException ignored) { }
			if (connection != null) try
			{
				connection.close();
			}
			catch (SQLException ignored) { }
		}

		logger.debug("Returning result list.");
		return new Data<List<Map>>(Uri.JSON, list).asJson();
	}
}
