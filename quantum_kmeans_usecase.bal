import ballerina/http;
import ballerina/lang.runtime;
import ballerina/log;

// Host URL
const string QHANA_HOST_URL = "http://choreo-qhana.eastus2.cloudapp.azure.com";
// Plugin URLs
const string URL_SIM_TO_DIST = "/plugins/sim-to-dist-transformers@v0-1-0/process/";
const string URL_AGGREGATION = "/plugins/distance-aggregator@v0-1-0/process/";
const string URL_MDS = "/plugins/mds@v0-1-0/process/";
const string URL_KMEANS = "/plugins/quantum-k-means@v0-1-0/process/";
const string URL_VISUALIZATION = "/plugins/visualization@v0-1-0/process/";
// Output filenames used in search
const string RESPONSE_ATTRIBUTE_DISTANCE_FILE = "attr_dist.zip";
const string RESPONSE_ENTITY_DISTANCE_JSON = "entity_distances.json";
const string RESPONSE_ENTITY_POINTS_JSON = "entity_points.json";
const string RESPONSE_CLUSTERS_JSON = "clusters.json";
const string RESPONSE_PLOT = "plot.html";
// Parameter value
const string PARAMETER_DISTANCE_ATTRIBUTES = "dominanteFarbe\ndominanterZustand\ndominanteCharaktereigenschaft\ndominanterAlterseindruck\ngenre";

# Create an HTTP Request with the required configurations 
#
# + return - Constructed http:Request with the configurations or error if the function fails
isolated function createQHanaRequest() returns http:Request|error {
    http:Request request = new;
    check request.setContentType("application/x-www-form-urlencoded");
    request.setHeader("Authorization", "Basic Y2hvcmVvX3VzZXI6cXcqMmFxXm5ndHQl");
    request.setHeader("Accept", "application/json");
    return request;
}

# Poll the endpoint until the task is completed
#
# + pollingUrl - URL used for polling
# + return - JSON payload after polling the Tasks API or error if the function fails
isolated function pollForResult(string pollingUrl) returns json|error {
    http:Client pollEndpoint = check new (pollingUrl);

    json json_payload = {};
    string status = "";
    while (status != "SUCCESS") {
        http:Response pollResponse = check pollEndpoint->get("");
        json_payload = check pollResponse.getJsonPayload();

        status = check json_payload.status.ensureType();
        if (status == "FAILIURE") {
            log:printError("task failed");
            break;
        }
        // Poll every 5 seconds
        runtime:sleep(5);
    }
    return json_payload;
}

# Extract the results url from the json payload
#
# + json_payload - JSON payload returned after polling the Tasks API
# + searchTerm - filename that needs to be searched in the results
# + return - URL of the results file or error if the function fails
isolated function searchForOutput(json json_payload, string searchTerm) returns string|error {
    string resultsUrl = "";

    json[] outputs = check json_payload.outputs.ensureType();
    json inner_output = outputs[0];
    string output_name = check inner_output.name.ensureType();

    if (output_name == searchTerm) {
        resultsUrl = check inner_output.href.ensureType();
    } else {
        log:printError("Couldn't retrive the results url");
    }
    return resultsUrl;
}

# Decide and poll the Tasks API and extract the results url from the json payload
#
# + postResponse - Response that is obtained when the QHana Plugin APIs are called
# + searchTerm - filename that needs to be searched in the results
# + return - URL of the results file or empty string if status code is not redirect or error if the function fails
isolated function getResultsUrl(http:Response postResponse, string searchTerm) returns string|error {
    string resultsUrl = "";

    //REDIRECT_SEE_OTHER_303
    if (postResponse.statusCode != 303) {
        string errorMsg = "Error: status code " + postResponse.statusCode.toBalString() + "not equal to 303";
        log:printError(errorMsg);
    } else {
        string pollingUrl = check postResponse.getHeader("Location");
        json json_payload = check pollForResult(pollingUrl);

        if (json_payload != {}) {
            resultsUrl = check searchForOutput(json_payload, searchTerm);
        }
    }
    return resultsUrl;
}

# Invoke the QHana APIs and provide the results url
#
# + qhanaClient - HTTP client used to connect to the QHana Plugin Runner
# + pluginURL - URL of the QHana plugin to invoke
# + payload - Payload that needs to be sent to the QHana plugin
# + resultsFileName - filename that needs to be searched in the results
# + return - URL of the results file or error if the function fails
isolated function invokeQHanaPlugin(http:Client qhanaClient, string pluginURL, string payload, string resultsFileName) returns string|error {
    // Create an HTTP request with the required configuration
    http:Request pluginRequest = check createQHanaRequest();
    // Set the necessary payload
    pluginRequest.setPayload(payload);
    // Invoke the Plugin API
    http:Response pluginResponse = check qhanaClient->post(pluginURL, pluginRequest);
    // Get the results file URL
    return check getResultsUrl(pluginResponse, resultsFileName);
}

service / on new http:Listener(8090) {
    # Invoke QHana APIs with the user's dataset to provide the results url for the Quantum KMeans workflow
    #
    # + input_payload - User payload containing the URL of the input dataset
    # + return - HTTP response containing the URLs of all the results files or error if the service invocation fails
    resource function post .(@http:Payload json input_payload) returns http:Response|error {

        // Get the dataset URL from the user request
        string attributeSimilaritiesUrl = check input_payload.attributeSimilaritiesUrl;

        // Create an HTTP client for the QHana backend
        http:Client qhanaClient = check new (QHANA_HOST_URL);

        // Invoke Sim to Dist Transformer API
        string simDistTransformerPayload = "attributeSimilaritiesUrl=" + attributeSimilaritiesUrl + "&attributes=" + PARAMETER_DISTANCE_ATTRIBUTES + "&transformer=linear_inverse";
        string attributeDistanceUrl = check invokeQHanaPlugin(qhanaClient, URL_SIM_TO_DIST, simDistTransformerPayload, RESPONSE_ATTRIBUTE_DISTANCE_FILE);

        // Inovke Distance Aggregator API
        string distanceAggregatorPayload = "attributeDistancesUrl=" + attributeDistanceUrl + "&aggregator=mean";
        string entityDistanceUrl = check invokeQHanaPlugin(qhanaClient, URL_AGGREGATION, distanceAggregatorPayload, RESPONSE_ENTITY_DISTANCE_JSON);

        // Invoke MDS API
        string mdsPayload = "entityDistancesUrl=" + entityDistanceUrl + "&dimensions=2&metric=metric_mds&nInit=4&maxIter=300";
        string mdsEntityPointsUrl = check invokeQHanaPlugin(qhanaClient, URL_MDS, mdsPayload, RESPONSE_ENTITY_POINTS_JSON);

        // Invoke Quantum KMeans API
        string quantumKMeansPayload = "entityPointsUrl=" + mdsEntityPointsUrl + "&clustersCnt=2&variant=negative_rotation&backend=aer_statevector_simulator&ibmqToken=&customBackend=";
        string clustersUrl = check invokeQHanaPlugin(qhanaClient, URL_KMEANS, quantumKMeansPayload, RESPONSE_CLUSTERS_JSON);

        // Invoke Visualization API
        string visualizationPayload = "entityPointsUrl=" + mdsEntityPointsUrl + "&clustersUrl=" + clustersUrl;
        string plotsUrl = check invokeQHanaPlugin(qhanaClient, URL_VISUALIZATION, visualizationPayload, RESPONSE_PLOT);

        // Create a response with all the results URLs 
        http:Response user_response = new;
        json output_json = {
            attribute_distances_url: attributeDistanceUrl,
            entity_distance_url: entityDistanceUrl,
            mds_entity_points_url: mdsEntityPointsUrl,
            clusters_url: clustersUrl,
            plots_url: plotsUrl
        };
        user_response.setJsonPayload(output_json);
        return user_response;
    }
}
