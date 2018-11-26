
import java.util.ArrayList;
import java.util.List;

import elasticsearch.Elasticsearch;
import org.json.JSONObject;
import org.json.JSONException;

public class ElasticsearchTest {
	public static void main(String args[]) {
		Elasticsearch es = new Elasticsearch("localhost:9200");
		List<JSONObject> actions = new ArrayList<>();
		JSONObject body = new JSONObject();
		JSONObject action = new JSONObject();
		try {
			body.put("_index", "test");
			body.put("_type", "test");
			body.put("_id", "1");
			action.put("index", body);
			actions.add(action);
			JSONObject data = new JSONObject();
			data.put("value", "1");
			actions.add(data);
			body = new JSONObject();
			body = new JSONObject();
			action = new JSONObject();
			body.put("_index", "test");
			body.put("_type", "test");
			body.put("_id", "2");
			action.put("index", body);
			actions.add(action);
			data = new JSONObject();
			data.put("value", "2");
			actions.add(data);
		} catch (JSONException e) {
			e.printStackTrace();
		}
		es.bulk(null, null, actions);
	}
}