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
import com.pusher.client.Pusher;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

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

        // set up
        System.out.println("Initializing server...");
        init();

        // wait for close
        System.out.println("Press enter to exit.");
        reader.readLine();

        // tear down
        smartCom.tearDownSmartCom();
        pusherclient.disconnect();
        System.out.println("Shutdown complete");
    }

    /**
     * initiales the adapter components
     * @throws CommunicationException
     */
    private static void init() throws CommunicationException {
        // initialize pusher client
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

        // connect to PeerManager
        peerManager = PeerManager.instance();

        // create SmartCom instance
        smartCom = new SmartComBuilder(peerManager, peerManager, peerManager).create();

        // get communication API
        communication = smartCom.getCommunication();
    }

    /**
     * Takes a String represented by JSON, creates a Message Object ans sends it to the peers
     * @param msg the JSON String
     */
    private static void sendIntervention(String msg) {
        JSONObject msgJson = new JSONObject(msg);
        String type = msgJson.getString("intervention_type");
        String intervention = msgJson.getString("intervention_text");

        if (type.equals("reminder")) {
            // reminder for a collective
            String cid = msgJson.getString("collective_id");
            JSONArray invalidated = msgJson.getJSONArray("invalidated");
            sendReminderToCollective(cid, invalidated, intervention);
        } else {
            // incentive for a peer or an entire collective
            JSONObject recipient = msgJson.getJSONObject("recipient");
            String rtype = recipient.getString("type");
            String rid = recipient.getString("id");
            Identifier id = (rtype.equals("collective")) ? Identifier.collective(rid) : Identifier.peer(rid);
            sendIncentiveToPeerOrCollective(id, intervention);
        }
    }

    /**
     * Gets a recipient and a message, and sends it using the SmartCom Communication API
     * @param id the Identifier of the receiver (can represent either a Peer or a Collective)
     * @param intervention the intervention message
     */
    private static void sendIncentiveToPeerOrCollective(Identifier id, String intervention) {
        Message.MessageBuilder builder = new Message.MessageBuilder()
                .setType("appid")
                .setSubtype("incentive")
                .setSenderId(Identifier.component("IS"))
                .setConversationId("incentive")
                .setContent(intervention)
                .setReceiverId(id);
        try {
            communication.send(builder.create());
            logger.info("Incentive was sent to the peer/collective with id: " + id.getId());
        } catch (CommunicationException ce){
            logger.error("CommunicationException (id: " + id.getId() + ").");
        }
    }

    /**
     * Gets a collective, a list of peers and a message, and sends it
     * to the peers in the collective that are not in the invalidated list
     * of peers, using SmartCom Communication API
     * @param cid the id of the collective
     * @param invalidated JSONArray of invalidated peers
     * @param intervention the intervention message
     */
    private static void sendReminderToCollective(String cid, JSONArray invalidated, String intervention) {
        try {
            // get the peers of the collective
            Identifier collectiveId = Identifier.collective(cid);
            CollectiveInfo collectiveInfo = peerManager.getCollectiveInfo(collectiveId);
            List<Identifier> shouldBeReminded = collectiveInfo.getPeers();

            // invalidate peers
            Identifier peer;
            for (int i = 0; i < invalidated.length(); i++) {
                peer = Identifier.peer(invalidated.getString(i));
                shouldBeReminded.remove(peer);
                logger.info("Peer " + peer.getId() + " was invalidated from collective " + cid);
            }

            // send message for each peer that should be reminded
            Message.MessageBuilder builder;
            for (Identifier id : shouldBeReminded){
                builder = new Message.MessageBuilder()
                        .setType("appid")
                        .setSubtype("reminder")
                        .setSenderId(Identifier.component("IS"))
                        .setConversationId("reminder")
                        .setContent(intervention)
                        .setReceiverId(id);
                communication.send(builder.create());
            }
            logger.info("Reminder was sent to the peers of collective " + cid + " that didn't respond.");
        } catch (NoSuchCollectiveException e) {
            logger.error("NoSuchCollectiveException (id: " + cid + ").");
        } catch (CommunicationException ce){
            logger.error("CommunicationException (id: " + cid + ").");
        }
    }
}
