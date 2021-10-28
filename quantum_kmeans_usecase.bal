import ballerina/http;
import ballerina/io;
import ballerina/lang.runtime;

function createQHanaRequest() returns http:Request{
    // This function is used to create an HTTP Request with the required configurations 
    http:Request request = new;
    error? contentType = request.setContentType("application/x-www-form-urlencoded");
    request.setHeader("Authorization", "Basic Y2hvcmVvX3VzZXI6cXcqMmFxXm5ndHQl");
    request.setHeader("Accept", "application/json");
    return request;
}

function pollForResult(string pollingUrl) returns json{
    // This function is used to poll the endpoint until the task is completed
    json json_payload = {};

    do {
	http:Client pollEndpoint = check new (pollingUrl);
        string status = "";
	    while (status != "SUCCESS"){
			http:Response pollResponse = check pollEndpoint->get("");
			json_payload = check pollResponse.getJsonPayload();

			string|error? temp_status = json_payload.status.ensureType();
			if temp_status is string {
				status = temp_status;
				if (status == "FAILIURE"){
					io:println("task failed");
					break;
				}
				runtime:sleep(5000);
			}
	   }
    } on fail var e {
    	io:println(e);
    }
    
    return json_payload;
}

function searchForOutput(json json_payload, string searchTerm) returns string{
    // This function is used to extract the results url from the json payload
    string resultsUrl = "";

    json[]|error? outputs = json_payload.outputs.ensureType();
    if outputs is json[] {
        json inner_output = outputs[0];
        string|error? output_name = inner_output.name.ensureType();
        if output_name is string{
            if (output_name == searchTerm){
                string|error? result = inner_output.href.ensureType();
                if result is string{
                    resultsUrl = result;
                }
            }else{
                io:println("Couldn't retrive the results url");
            }
        }
    }
    return resultsUrl;
}

function getResultsUrl(http:Response postResponse, string searchTerm) returns string{
    // This function is used to poll the tasks API and retrieve the results URL
    
    //REDIRECT_SEE_OTHER_303
    if (postResponse.statusCode != 303) {
        io:println("Error: status code incorrect not equal to 303");
        io:print("postResponse.statusCode");
        io:println(postResponse.statusCode);
    }else{
        do {
	        string pollingUrl = check postResponse.getHeader("Location");
            json json_payload = pollForResult(pollingUrl);

            if (json_payload != {}){
                resultsUrl = searchForOutput(json_payload, searchTerm);
            }
        } on fail var e {
        	io:println(e);
        }
    }
    return resultsUrl;
}

// This service takes in the dataset URL as the input and creates a Quantum KMeans plot. 
// This service returns the result files URLs to the user.
service / on new http:Listener(8090) {
    resource function post .(http:Request user_request) returns http:Response|error? {
        
        // Get user the dataset URL from the user request
        json input_payload = checkpanic user_request.getJsonPayload();
        string attributeSimilaritiesUrl = check input_payload.attributeSimilaritiesUrl;

        // Create an HTTP client for the QHana backend
        http:Client httpEndpoint = check new ("http://choreo-qhana.eastus2.cloudapp.azure.com");


        // Create an HTTP request with the required configuration
        http:Request simDistTransformerRequest = createQHanaRequest();
        // Create the required payload
        string attributes = "dominanteFarbe\ndominanterZustand\ndominanteCharaktereigenschaft\ndominanterAlterseindruck\ngenre";
        string simDistTransformerPayload = "attributeSimilaritiesUrl=" + attributeSimilaritiesUrl + "&attributes=" + attributes + "&transformer=linear_inverse";
        simDistTransformerRequest.setPayload(simDistTransformerPayload);
        // Invoke the simDistTransformer API
        http:Response simDistTransformerPostResponse = check httpEndpoint->post("/plugins/sim-to-dist-transformers@v0-1-0/process/", simDistTransformerRequest);
        // Get the results file URL
        string attributeDistanceUrl = getResultsUrl(simDistTransformerPostResponse, "attr_dist.zip");


        // Create an HTTP request with the required configuration
        http:Request distanceAggregatorRequest = createQHanaRequest();
        // Create the required payload
        string distanceAggregatorPayload = "attributeDistancesUrl=" + attributeDistanceUrl + "&aggregator=mean";
        distanceAggregatorRequest.setPayload(distanceAggregatorPayload);
        // Invoke distance aggregator API
        http:Response distAggregatorPostResponse = check httpEndpoint->post("/plugins/distance-aggregator@v0-1-0/process/", distanceAggregatorRequest);
        // Get the results file URL
        string entityDistanceUrl = getResultsUrl(distAggregatorPostResponse, "entity_distances.json");
        

        // Create an HTTP request with the required configuration
        http:Request mdsRequest = createQHanaRequest();
        // Create the required payload
        string mdsPayload = "entityDistancesUrl=" + entityDistanceUrl + "&dimensions=2&metric=metric_mds&nInit=4&maxIter=300";
        mdsRequest.setPayload(mdsPayload);
        // Invoke mds API
        http:Response mdsPostResponse = check httpEndpoint->post("/plugins/mds@v0-1-0/process/", mdsRequest);
        // Get the results file URL
        string mdsEntityPointsUrl = getResultsUrl(mdsPostResponse, "entity_points.json");
        

        // Create an HTTP request with the required configuration
        http:Request quantumKMeansRequest = createQHanaRequest();
        // Create the required payload
        string quantumKMeansPayload = "entityPointsUrl=" + mdsEntityPointsUrl + "&clustersCnt=2&variant=negative_rotation&backend=aer_statevector_simulator&ibmqToken=&customBackend=";
        quantumKMeansRequest.setPayload(quantumKMeansPayload);
        // Invoke quantumKMeans API
        http:Response quantumKMeansPostResponse = check httpEndpoint->post("/plugins/quantum-k-means@v0-1-0/process/", quantumKMeansRequest);
        // Get the results file URL
        string clustersUrl = getResultsUrl(quantumKMeansPostResponse, "clusters.json");   


        // Create an HTTP request with the required configuration
        http:Request visualizationRequest = createQHanaRequest();
        // Create the required payload
        string visualizationPayload = "entityPointsUrl=" + mdsEntityPointsUrl + "&clustersUrl=" + clustersUrl;
        visualizationRequest.setPayload(visualizationPayload);
        // Invoke the visualization API
        http:Response visualizationPostResponse = check httpEndpoint->post("/plugins/visualization@v0-1-0/process/", visualizationRequest);
        // Get the results file URL
        string plotsUrl = getResultsUrl(visualizationPostResponse, "plot.html"); 

        // Create a response with all the results URLs 
        http:Response user_response = new;
        json output_json = {attribute_distances_url: attributeDistanceUrl, entity_distance_url: entityDistanceUrl,
                            mds_entity_points_url: mdsEntityPointsUrl, clusters_url: clustersUrl, plots_url: plotsUrl
                            };
        user_response.setJsonPayload(output_json);
        return user_response;
    }
}
