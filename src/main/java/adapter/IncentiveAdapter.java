package adapter;

import at.ac.tuwien.dsg.smartcom.Communication;
import at.ac.tuwien.dsg.smartcom.SmartCom;
import at.ac.tuwien.dsg.smartcom.SmartComBuilder;
import at.ac.tuwien.dsg.smartcom.callback.exception.NoSuchCollectiveException;
import at.ac.tuwien.dsg.smartcom.exception.CommunicationException;
import at.ac.tuwien.dsg.smartcom.model.CollectiveInfo;
import at.ac.tuwien.dsg.smartcom.model.Identifier;
import at.ac.tuwien.dsg.smartcom.model.Message;

import com.pusher.client.connection.ConnectionEventListener;
import com.pusher.client.connection.ConnectionState;
import com.pusher.client.connection.ConnectionStateChange;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import com.pusher.client.Pusher;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

/**
 * @author Shaked Hindi
 */
public class IncentiveAdapter{
    private static SmartCom smartCom;
    private static Communication communication;
    private static Pusher pusherclient;
    private static PeerManager peerManager;
    private static final Logger logger = LogManager.getLogger(IncentiveAdapter.class);

    public static void main(String[] args) throws CommunicationException, IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

        //set up
        System.out.println("Initializing server...");
        init();

        //wait for close
        System.out.println("Press enter to exit.");
        reader.readLine();

        //tear down
        smartCom.tearDownSmartCom();
        pusherclient.disconnect();
        System.out.println("Shutdown complete");
    }

    /**
     * initiales the adapter components
     * @throws CommunicationException
     */
    private static void init() throws CommunicationException {
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

        //connect to PeerManager
        peerManager = PeerManager.instance();

        //create SmartCom instance
        smartCom = new SmartComBuilder(peerManager, peerManager, peerManager).create();

        //get communication API
        communication = smartCom.getCommunication();
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
            JSONArray invalidated = jsonObject.getJSONArray("invalidated");
            Identifier peer;
            for (int i = 0; i < invalidated.length(); i++) {
                peer = Identifier.peer(invalidated.getString(i));
                shouldBeReminded.remove(peer);
                logger.info("Peer " + peer.getId() + " was invalidated from collective " + collectiveId.getId());
            }
        } catch (NoSuchCollectiveException e) {
            logger.error("NoSuchCollectiveException (id: " + collectiveId.getId() + ").");
            return;
        }

        // send message for each experts that should be reminded
        Message.MessageBuilder builder;
        String conversation = jsonObject.getString("intervention_type");
        String intervention = jsonObject.getString("intervention_text");
        try {
            for (Identifier id : shouldBeReminded){
                builder = new Message.MessageBuilder()
                                .setType("appid")
                                .setSubtype("reminder")
                                .setSenderId(Identifier.component("IS"))
                                .setConversationId(conversation)
                                .setContent(intervention)
                                .setReceiverId(id);
                m = builder.create();
                communication.send(m);
            }
        } catch (CommunicationException ce){
            logger.error("CommunicationException (id: " + collectiveId.getId() + ").");
        }
        logger.info("Reminder was sent to the peers of collective " + collectiveId.getId() + " that didn't respond.");
    }
}
