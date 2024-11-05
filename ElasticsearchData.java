import org.elasticsearch.action.count.CountRequest;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class ElasticsearchDataAnalytics {

    private static RestHighLevelClient client;

    public static void main(String[] args) throws IOException {
        client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 8989, "http")));

        String v_nameCollection = "Hash_YourName";
        String v_phoneCollection = "Hash_YourPhoneLastFourDigits";

        createCollection(v_nameCollection);
        createCollection(v_phoneCollection);

        getEmpCount(v_nameCollection);

        indexData(v_nameCollection, "Department");
        indexData(v_phoneCollection, "Gender");

        getEmpCount(v_nameCollection);

        delEmpById(v_nameCollection, "E02003");

        getEmpCount(v_nameCollection);

        searchByColumn(v_nameCollection, "Department", "IT");
        searchByColumn(v_nameCollection, "Gender", "Male");
        searchByColumn(v_phoneCollection, "Department", "IT");

        getDepFacet(v_nameCollection);
        getDepFacet(v_phoneCollection);

        client.close();
    }

    public static void createCollection(String p_collection_name) throws IOException {
        CreateIndexRequest request = new CreateIndexRequest(p_collection_name);
        CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
    }

    public static void indexData(String p_collection_name, String p_exclude_column) throws IOException {
        List<String> lines = Files.readAllLines(Paths.get("employee_data.csv"));
        String header = lines.get(0);
        String[] columns = header.split(",");

        int excludeIndex = -1;
        for (int i = 0; i < columns.length; i++) {
            if (columns[i].equalsIgnoreCase(p_exclude_column)) {
                excludeIndex = i;
                break;
            }
        }

        for (int i = 1; i < lines.size(); i++) {
            String[] values = lines.get(i).split(",");
            StringBuilder json = new StringBuilder("{");
            for (int j = 0; j < values.length; j++) {
                if (j != excludeIndex) {
                    json.append("\"").append(columns[j]).append("\":\"").append(values[j]).append("\",");
                }
            }
            json.deleteCharAt(json.length() - 1).append("}");
            IndexRequest request = new IndexRequest(p_collection_name).id(String.valueOf(i)).source(json.toString(), XContentType.JSON);
            IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
        }
    }

    public static void searchByColumn(String p_collection_name, String p_column_name, String p_column_value) throws IOException {
        SearchRequest searchRequest = new SearchRequest(p_collection_name);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery(p_column_name, p_column_value));
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        searchResponse.getHits().forEach(hit -> System.out.println(hit.getSourceAsString()));
    }

    public static void getEmpCount(String p_collection_name) throws IOException {
        CountRequest countRequest = new CountRequest(p_collection_name);
        countRequest.query(QueryBuilders.matchAllQuery());

        CountResponse countResponse = client.count(countRequest, RequestOptions.DEFAULT);
        long count = countResponse.getCount();
        System.out.println("Total Employees: " + count);
    }

    public static void delEmpById(String p_collection_name, String p_employee_id) throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest(p_collection_name, p_employee_id);
        DeleteResponse deleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT);
    }

    public static void getDepFacet(String p_collection_name) throws IOException {
        SearchRequest searchRequest = new SearchRequest(p_collection_name);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.aggregation(AggregationBuilders.terms("departments").field("Department.keyword"));
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        Terms terms = searchResponse.getAggregations().get("departments");
        terms.getBuckets().forEach(bucket -> System.out.println("Department: " + bucket.getKeyAsString() + ", Count: " + bucket.getDocCount()));
    }
}
