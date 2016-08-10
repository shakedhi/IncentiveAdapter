package adapter;

import at.ac.tuwien.dsg.smartcom.Communication;
import at.ac.tuwien.dsg.smartcom.SmartCom;
import at.ac.tuwien.dsg.smartcom.SmartComBuilder;
import at.ac.tuwien.dsg.smartcom.adapters.EmailInputAdapter;
import at.ac.tuwien.dsg.smartcom.callback.NotificationCallback;
import at.ac.tuwien.dsg.smartcom.callback.exception.NoSuchCollectiveException;
import at.ac.tuwien.dsg.smartcom.exception.CommunicationException;
import at.ac.tuwien.dsg.smartcom.model.*;
import at.ac.tuwien.dsg.smartcom.utils.PropertiesLoader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import com.pusher.client.channel.*;
import com.pusher.client.connection.*;
import org.apache.commons.lang3.StringEscapeUtils;
import org.json.*;

/**
 * @author Shaked Hindi
 */
public class IncentiveAdapter{
    private static SmartCom smartCom;
    private static Communication communication;
    private static com.pusher.rest.Pusher pusher;
    private static com.pusher.client.Pusher pusherclient;
    private static PeerManager peerManager;
    private static ConcurrentHashMap<Identifier,LinkedList<Identifier>> excludedExperts;

    public static void main(String[] args) throws CommunicationException, IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

        //set up
        System.out.println("Initializing server...");
        init();
        System.out.println("Server is running, waiting for requests via 'is.smartsociety@gmail.com'.");

        //wait for close
        System.out.println("Press enter to exit");
        reader.read();

        //tear down
        smartCom.tearDownSmartCom();
        pusherclient.disconnect();
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
                System.out.println("Pusher client is " + change.getCurrentState());
            }

            @Override
            public void onError(String message, String code, Exception e) {
                System.out.println("There was a problem connecting!");
            }
        }, ConnectionState.ALL);

        Channel channel = pusherclient.subscribe("adapter");

        channel.bind("intervention", new SubscriptionEventListener() {
            @Override
            public void onEvent(String channel, String event, String data) {
                System.out.println("Received intervention with data: " + data);
                sendIntervention(data);
            }
        });

        channel.bind("expert", new SubscriptionEventListener() {
            @Override
            public void onEvent(String channel, String event, String data) {
                System.out.println("Received expert with data: " + data);
                excludeExpert("6802", data); //TODO: fix hard coded collective id
            }
        });

        //connect to PeerManager
        peerManager = PeerManager.instance();

        //create SmartCom instance
        smartCom = new SmartComBuilder(peerManager, peerManager, peerManager).create();

        //get communication API
        communication = smartCom.getCommunication();

        //register the input handler (pulls every second)
        communication.addPullAdapter(
                new EmailInputAdapter("REMINDER",
                        PropertiesLoader.getProperty("EmailAdapter.properties", "hostIncoming"),
                        PropertiesLoader.getProperty("EmailAdapter.properties", "username"),
                        PropertiesLoader.getProperty("EmailAdapter.properties", "password"),
                        Integer.valueOf(PropertiesLoader.getProperty("EmailAdapter.properties", "portIncoming")),
                        true, "test", "test", true),
                1000);

        //register notification callback
        communication.registerNotificationCallback(new ReminderHandler());

        excludedExperts = new ConcurrentHashMap<>();
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
            shouldBeReminded.removeAll(excludedExperts.get(collectiveId));
            excludedExperts.remove(collectiveId);
            //TODO: remove shouldBeReminded = new LinkedList<>();
            //TODO: remove shouldBeReminded.add(Identifier.peer("shakedhi"));
        } catch (NoSuchCollectiveException e) {
            System.out.println("sendIntervention: NoSuchCollectiveException (id: " + collectiveId.getId() + ").");
            return;
        }

        // build message template
        String conversation = jsonObject.getString("intervention_type"); //TODO: correct conversation
        String intervention = jsonObject.getString("text_message"); //TODO: correct intervention
        Message.MessageBuilder builder =
                new Message.MessageBuilder()
                        .setType("APP_ID") //TODO: add app id as a type
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
            System.out.println("sendIntervention: CommunicationException (id: " + collectiveId.getId() + ").");
        }
    }

    /**
     * Gets a collective and a list of peers and adds them to the corresponding exluded experts list
     * @param collective the collective id
     * @param experts the JSON String
     */
    private static void excludeExpert(String collective, String experts) {
        Identifier collectiveId = Identifier.collective(collective);
        List<Identifier> excluded = excludedExperts.get(collectiveId);

        if(excluded == null) {
            System.out.println("excludeExpert: null list (id: " + collectiveId.getId() + ").");
            return;
        }

        //parse json array and add experts to collective excluded list
        JSONArray peers = new JSONArray(experts);
        Identifier expertPeerId;
        for(int i = 0; i < peers.length(); i++){
            expertPeerId = Identifier.peer(peers.getJSONObject(i).getString("id"));
            excluded.add(expertPeerId);
        }
    }


    /**
     *  Handle an incoming reminder request
     */
    private static class ReminderHandler implements NotificationCallback {
        @Override
        public void notify(Message message) {
            //parse request to classification
            Classification classification = parseReminderRequest(message);

            //send message to incentive server
            System.out.println("SENT TO IS: '" + classification.toString() + "'.");
            pusher.trigger("ouroboros", "classification", classification);
        }

        /**
         * Takes a Message Object and creates a Classification Object
         * @param msg the Message Object
         * @return the Classification Object
         */
        private Classification parseReminderRequest(Message msg){
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");

            String untaggedContent = msg.getContent().replaceAll("<.*?>","");
            untaggedContent = StringEscapeUtils.unescapeHtml4(untaggedContent).trim();
            untaggedContent = untaggedContent.replaceAll("\\r\\n|\\r|\\n|\\s|\\u00A0|\\u2007|\\u202F","");
            System.out.println("REMINDER: '" + untaggedContent + "'.");

            JSONObject content = new JSONObject(untaggedContent); //TODO: fix incentive_text parsing

            String collective = content.getJSONObject("recipient").getString("id");
            String city = "Hadera"; //TODO: fix geo when needed
            String country = "Israel";
            String incentive_text = content.getString("incentive_text");
            String created = sdf.format(new Date(content.getLong("incentive_timestamp")));

            excludedExperts.put(Identifier.collective(collective), new LinkedList<Identifier>());

            return new Classification(collective, city, country, incentive_text, created);
        }
    }

    /**
     * private class that parsed to JSON
     */
    private static class Classification {
        private String project;
        private String user_id;
        private Geographic geo;
        private String subjects;
        private String created_at;

        Classification(String user, String city, String country, String subj, String created){
            this.project = "smartcom";
            this.user_id = user;
            this.geo = new Geographic(city, country);
            this.subjects = subj;
            this.created_at = created;
        }

        @Override
        public String toString() {
            return "{ \"project\":\"" + project + "\", \"user_id\":\"" + user_id +
                    "\", \"geo\":\"" + geo.toString() + "\", \"subject\":\"" +
                    subjects + "\", \"created_at\":\"" + created_at + "\" }";
        }

        private class Geographic{
            private String city_name;
            private String country_name;

            Geographic(String city, String country){
                this.city_name = city;
                this.country_name = country;
            }

            @Override
            public String toString() {
                return "{ \"city_name:\"" + city_name + "\", \"country_name:\"" + country_name + "\" }";
            }
        }
    }

    /*
        Temporary demo peer

    private static Identifier createDemoPeer(DemoPeerManager peerManager){
        Identifier peerId = Identifier.peer("peer1");
        String email = "shakedhi@post.bgu.ac.il";

        PeerChannelAddress address = new PeerChannelAddress();
        address.setPeerId(peerId);
        address.setChannelType(Identifier.channelType("Email"));
        address.setContactParameters(Arrays.asList(email));

        PeerInfo info = new PeerInfo();
        info.setId(peerId);
        info.setDeliveryPolicy(DeliveryPolicy.Peer.AT_LEAST_ONE);
        info.setPrivacyPolicies(null);
        info.setAddresses(Arrays.asList(address));
        System.out.println(info.toString());
        //peerManager.addPeer(peerId, info, peerId.getId());

        return peerId;
    }*/
}
