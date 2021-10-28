import ballerina/http;
import ballerina/lang.runtime;
import ballerina/log;

isolated function createQHanaRequest() returns http:Request{
    // This function is used to create an HTTP Request with the required configurations 
    http:Request request = new;
    error? contentType = request.setContentType("application/x-www-form-urlencoded");
    request.setHeader("Authorization", "Basic Y2hvcmVvX3VzZXI6cXcqMmFxXm5ndHQl");
    request.setHeader("Accept", "application/json");
    return request;
}

isolated function pollForResult(string pollingUrl) returns json|error{
    // This function is used to poll the endpoint until the task is completed
    http:Client pollEndpoint = check new (pollingUrl);

    json json_payload = {};
    string status = "";
    while (status != "SUCCESS"){
        http:Response pollResponse = check pollEndpoint->get("");
        json_payload = check pollResponse.getJsonPayload();

        status = check json_payload.status.ensureType();
        if (status == "FAILIURE"){
            log:printError("task failed");
            break;
        }
        runtime:sleep(1);
    }

    
    return json_payload;
}

isolated function searchForOutput(json json_payload, string searchTerm) returns string|error{
    // This function is used to extract the results url from the json payload
    string resultsUrl = "";

    json[] outputs = check json_payload.outputs.ensureType();
    json inner_output = outputs[0];
    string output_name = check inner_output.name.ensureType();

    if (output_name == searchTerm){
        string result = check inner_output.href.ensureType();
        resultsUrl = result;
    }else{
        log:printError("Couldn't retrive the results url");
    }


    return resultsUrl;
}

isolated function getResultsUrl(http:Response postResponse, string searchTerm) returns string|error{
    // This function is used to poll the tasks API and retrieve the results URL
    string resultsUrl = "";

    //REDIRECT_SEE_OTHER_303
    if (postResponse.statusCode != 303) {
        string errorMsg = "Error: status code " + postResponse.statusCode.toBalString() + "not equal to 303";
        log:printError(errorMsg);
    }else{
        string pollingUrl = check postResponse.getHeader("Location");
        json json_payload = check pollForResult(pollingUrl);

        if (json_payload != {}){
            resultsUrl = check searchForOutput(json_payload, searchTerm);
        }
    }
    return resultsUrl;
}

// This service takes in the dataset URL as the input and creates a Quantum KMeans plot. 
// This service returns the result files URLs to the user.
service / on new http:Listener(8090) {
    resource function post .(http:Request user_request) returns http:Response|error {
        
        // Host URL
        string QHANA_HOST_URL = "http://choreo-qhana.eastus2.cloudapp.azure.com";
        // Plugin URLs
        string URL_SIM_TO_DIST = "/plugins/sim-to-dist-transformers@v0-1-0/process/";
        string URL_AGGREGATION = "/plugins/distance-aggregator@v0-1-0/process/";
        string URL_MDS = "/plugins/mds@v0-1-0/process/";
        string URL_KMEANS = "/plugins/quantum-k-means@v0-1-0/process/";
        string URL_VISUALIZATION = "/plugins/visualization@v0-1-0/process/";
        // Output filenames used in search
        string RESPONSE_ATTRIBUTE_DISTANCE_FILE = "attr_dist.zip";
        string RESPONSE_ENTITY_DISTANCE_JSON = "entity_distances.json";
        string RESPONSE_ENTITY_POINTS_JSON = "entity_points.json";
        string RESPONSE_CLUSTERS_JSON = "clusters.json";
        string RESPONSE_PLOT = "plot.html";

        // Get user the dataset URL from the user request
        json input_payload = check user_request.getJsonPayload();
        string attributeSimilaritiesUrl = check input_payload.attributeSimilaritiesUrl;

        // Create an HTTP client for the QHana backend
        http:Client httpEndpoint = check new (QHANA_HOST_URL);


        // Create an HTTP request with the required configuration
        http:Request simDistTransformerRequest = createQHanaRequest();
        // Create the required payload
        string attributes = "dominanteFarbe\ndominanterZustand\ndominanteCharaktereigenschaft\ndominanterAlterseindruck\ngenre";
        string simDistTransformerPayload = "attributeSimilaritiesUrl=" + attributeSimilaritiesUrl + "&attributes=" + attributes + "&transformer=linear_inverse";
        simDistTransformerRequest.setPayload(simDistTransformerPayload);
        // Invoke the simDistTransformer API
        http:Response simDistTransformerPostResponse = check httpEndpoint->post(URL_SIM_TO_DIST, simDistTransformerRequest);
        // Get the results file URL
        string attributeDistanceUrl = check getResultsUrl(simDistTransformerPostResponse, RESPONSE_ATTRIBUTE_DISTANCE_FILE);


        // Create an HTTP request with the required configuration
        http:Request distanceAggregatorRequest = createQHanaRequest();
        // Create the required payload
        string distanceAggregatorPayload = "attributeDistancesUrl=" + attributeDistanceUrl + "&aggregator=mean";
        distanceAggregatorRequest.setPayload(distanceAggregatorPayload);
        // Invoke distance aggregator API
        http:Response distAggregatorPostResponse = check httpEndpoint->post(URL_AGGREGATION, distanceAggregatorRequest);
        // Get the results file URL
        string entityDistanceUrl = check getResultsUrl(distAggregatorPostResponse, RESPONSE_ENTITY_DISTANCE_JSON);
        

        // Create an HTTP request with the required configuration
        http:Request mdsRequest = createQHanaRequest();
        // Create the required payload
        string mdsPayload = "entityDistancesUrl=" + entityDistanceUrl + "&dimensions=2&metric=metric_mds&nInit=4&maxIter=300";
        mdsRequest.setPayload(mdsPayload);
        // Invoke mds API
        http:Response mdsPostResponse = check httpEndpoint->post(URL_MDS, mdsRequest);
        // Get the results file URL
        string mdsEntityPointsUrl = check getResultsUrl(mdsPostResponse, RESPONSE_ENTITY_POINTS_JSON);
        

        // Create an HTTP request with the required configuration
        http:Request quantumKMeansRequest = createQHanaRequest();
        // Create the required payload
        string quantumKMeansPayload = "entityPointsUrl=" + mdsEntityPointsUrl + "&clustersCnt=2&variant=negative_rotation&backend=aer_statevector_simulator&ibmqToken=&customBackend=";
        quantumKMeansRequest.setPayload(quantumKMeansPayload);
        // Invoke quantumKMeans API
        http:Response quantumKMeansPostResponse = check httpEndpoint->post(URL_KMEANS, quantumKMeansRequest);
        // Get the results file URL
        string clustersUrl = check getResultsUrl(quantumKMeansPostResponse, RESPONSE_CLUSTERS_JSON);   


        // Create an HTTP request with the required configuration
        http:Request visualizationRequest = createQHanaRequest();
        // Create the required payload
        string visualizationPayload = "entityPointsUrl=" + mdsEntityPointsUrl + "&clustersUrl=" + clustersUrl;
        visualizationRequest.setPayload(visualizationPayload);
        // Invoke the visualization API
        http:Response visualizationPostResponse = check httpEndpoint->post(URL_VISUALIZATION, visualizationRequest);
        // Get the results file URL
        string plotsUrl = check getResultsUrl(visualizationPostResponse, RESPONSE_PLOT); 

        // Create a response with all the results URLs 
        http:Response user_response = new;
        json output_json = {attribute_distances_url: attributeDistanceUrl, entity_distance_url: entityDistanceUrl,
                            mds_entity_points_url: mdsEntityPointsUrl, clusters_url: clustersUrl, plots_url: plotsUrl
                            };
        user_response.setJsonPayload(output_json);
        return user_response;
    }
}
