package adapter;

import at.ac.tuwien.dsg.smartcom.Communication;
import at.ac.tuwien.dsg.smartcom.SmartCom;
import at.ac.tuwien.dsg.smartcom.SmartComBuilder;
import at.ac.tuwien.dsg.smartcom.callback.exception.NoSuchCollectiveException;
import at.ac.tuwien.dsg.smartcom.exception.CommunicationException;
import at.ac.tuwien.dsg.smartcom.model.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import com.pusher.client.connection.*;
import org.json.*;
import org.apache.log4j.Logger;
import org.apache.log4j.LogManager;
import spark.Request;
import spark.Response;

import static spark.Spark.*;

/**
 * @author Shaked Hindi
 */
public class IncentiveAdapter{
    private static SmartCom smartCom;
    private static Communication communication;
    private static com.pusher.rest.Pusher pusher;
    private static com.pusher.client.Pusher pusherclient;
    private static PeerManager peerManager;
    private static ConcurrentHashMap<Identifier,LinkedList<Identifier>> invalidatedPeers;
    private static final Logger logger = LogManager.getLogger(IncentiveAdapter.class);

    public static void main(String[] args) throws CommunicationException, IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

        //set up
        System.out.println("Initializing server...");
        init();
        System.out.println("Server is running at http://localhost:8083.");

        //wait for close
        System.out.println("Press enter to exit.");
        reader.readLine();

        //tear down
        smartCom.tearDownSmartCom();
        pusherclient.disconnect();
        stop();
        System.out.println("Shutdown complete");
    }

    /**
     *
     * @throws CommunicationException
     */
    private static void init() throws CommunicationException {
        //initialize pusher server
        pusher = new com.pusher.rest.Pusher("231267", "bf548749c8760edbe3b6", "6545a7b9465cde9fab73");
        pusher.setEncrypted(true);

        //initialize pusher client
        pusherclient = new com.pusher.client.Pusher("bf548749c8760edbe3b6");

        pusherclient.connect(new ConnectionEventListener() {
            @Override
            public void onConnectionStateChange(ConnectionStateChange change) {
                ConnectionState state = change.getCurrentState();
                if(state == ConnectionState.CONNECTED || state == ConnectionState.DISCONNECTED)
                    logger.info("Pusher client is " + state);
            }

            @Override
            public void onError(String message, String code, Exception e) {
                logger.error("There is a connection problem!");
            }
        }, ConnectionState.ALL);

        pusherclient.subscribe("adapter").bind("intervention", (chan, event, data) -> {
            logger.info("Received intervention request (data: " + data + ")");
            sendIntervention(data);
        });

        //initialize server
        port(8083);
        get("/*", (request, response) -> badRequest(response));
        put("/*", (request, response) -> badRequest(response));
        delete("/*", (request, response) -> badRequest(response));
        options("/*", (request, response) -> badRequest(response));
        post("/invalidate/:id", (request, response) -> {
            try {
                if (Integer.parseInt(request.params(":id")) < 1)
                    return badRequest(response);
                invalidatePeers(request);
                return successfulRequest(response);
            } catch (NumberFormatException e){
                return badRequest(response);
            }
        });
        post("/*", (request, response) -> {
            if (request.splat().length == 1 && request.splat()[0].equals("reminder")){
                handleReminderRequest(request);
                return successfulRequest(response);
            }
            return badRequest(response);
        });

        //connect to PeerManager
        peerManager = PeerManager.instance();

        //create SmartCom instance
        smartCom = new SmartComBuilder(peerManager, peerManager, peerManager).create();

        //get communication API
        communication = smartCom.getCommunication();

        //init invalidated peers map
        invalidatedPeers = new ConcurrentHashMap<>();
    }

    private static String badRequest(Response response){
        response.status(400);
        return "<h1>The server supports only POST request with a '/reminder' postfix or a '/invalidate/{collective_id}' postfix.</h1>";
    }

    private static String successfulRequest(Response response){
        response.status(200);
        return "<h1>Success</h1>";
    }

    /**
     * Gets a POST request, parses its content, creates a Classification Object
     * and send it to the Incentive Server
     * @param request the POST request
     */
    private static void handleReminderRequest(Request request){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");

        String content = request.body();
        String formattedContent = content.replaceAll("\\r\\n|\\r|\\n|\\s|\\u00A0|\\u2007|\\u202F","");
        logger.info("Reminder received: '" + formattedContent + "'.");

        JSONObject jContent = new JSONObject(formattedContent);

        String collective = jContent.getJSONObject("recipient").getString("id");
        String city = jContent.getJSONObject("location").getString("city_name");
        String country = jContent.getJSONObject("location").getString("country_name");
        String incentive_text = jContent.getString("incentive_text");
        String created = sdf.format(new Date(jContent.getLong("incentive_timestamp")));

        //create a list for the excluded experts
        invalidatedPeers.put(Identifier.collective(collective), new LinkedList<>());

        //send message to incentive server
        Classification classification = new Classification(collective, city, country, incentive_text, created);
        logger.info("Sent to IS: '" + classification.toString() + "'.");
        pusher.trigger("ouroboros", "classification", classification);
    }

    /**
     * Gets POST request with a collective and a list of peers and
     * adds them to the corresponding invalidated peers list
     * @param request the POST request
     */
    private static void invalidatePeers(Request request) {
        String collective = request.params(":id");
        String experts = request.body();
        Identifier collectiveId = Identifier.collective(collective);
        List<Identifier> invalidated = invalidatedPeers.get(collectiveId);

        if(invalidated == null) {
            logger.error("null list (id: " + collectiveId.getId() + ").");
            return;
        }

        //parse json array and add experts to collective invalidated list
        JSONArray peers = new JSONArray(experts);
        Identifier peerId;
        for(int i = 0; i < peers.length(); i++){
            peerId = Identifier.peer(peers.getJSONObject(i).getString("id"));
            invalidated.add(peerId);
            logger.info("Peer " + peerId + " was invalidated from collective " + collective);
        }
    }

    /**
     * Takes a String represented by JSON, creates a Message Object ans sends it to the peers
     * @param msg the JSON String
     */
    private static void sendIntervention(String msg) {
        JSONObject jsonObject = new JSONObject(msg);
        List<Identifier> shouldBeReminded;
        Message m;

        // get collective peers, exclude ones that has answered and free memory
        Identifier collectiveId = Identifier.collective(jsonObject.getString("collective_id"));
        try {
            CollectiveInfo collectiveInfo = peerManager.getCollectiveInfo(collectiveId);
            shouldBeReminded = collectiveInfo.getPeers();
            shouldBeReminded.removeAll(invalidatedPeers.get(collectiveId));
            invalidatedPeers.remove(collectiveId);
        } catch (NoSuchCollectiveException e) {
            logger.error("NoSuchCollectiveException (id: " + collectiveId.getId() + ").");
            return;
        }

        // build message template
        String conversation = jsonObject.getString("intervention_type");
        String intervention = jsonObject.getString("intervention_text");
        Message.MessageBuilder builder =
                new Message.MessageBuilder()
                        .setType("APP_ID") //TODO: fix type field
                        .setSubtype("reminder")
                        .setSenderId(Identifier.component("IS"))
                        .setConversationId(conversation)
                        .setContent(intervention);

        // send message for each experts that should be reminded
        try{
            for (Identifier id : shouldBeReminded){
                builder.setReceiverId(id);
                m = builder.create();
                communication.send(m);
            }
        } catch (CommunicationException ce){
            logger.error("CommunicationException (id: " + collectiveId.getId() + ").");
        }
        logger.info("Reminder was sent to the peers of collective " + collectiveId.getId() + " that didn't respond.");
    }
}
