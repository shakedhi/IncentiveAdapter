package adapter;

import at.ac.tuwien.dsg.smartcom.callback.CollectiveInfoCallback;
import at.ac.tuwien.dsg.smartcom.callback.PeerAuthenticationCallback;
import at.ac.tuwien.dsg.smartcom.callback.PeerInfoCallback;
import at.ac.tuwien.dsg.smartcom.callback.exception.NoSuchCollectiveException;
import at.ac.tuwien.dsg.smartcom.callback.exception.NoSuchPeerException;
import at.ac.tuwien.dsg.smartcom.callback.exception.PeerAuthenticationException;
import at.ac.tuwien.dsg.smartcom.model.*;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Shaked Hindi
 */
class PeerManager implements PeerAuthenticationCallback, PeerInfoCallback, CollectiveInfoCallback {
    private static final Logger logger = LogManager.getLogger(PeerManager.class);
    private static PeerManager peerManager;

    private PeerManager() {}

    static PeerManager instance() {
        if (peerManager == null)
            peerManager = new PeerManager();
        return peerManager;
    }

    private String getRequest(String requestType, String id){
        HttpClient httpClient = HttpClientBuilder.create().build();
        String pm_url = "http://elog.disi.unitn.it:8081/kos-smartsociety/smartsociety-peermanager/";
        try{
            // send get request
            HttpGet request = new HttpGet(pm_url + requestType + id);
            HttpResponse response = httpClient.execute(request);

            // check status code
            int status = response.getStatusLine().getStatusCode();
            if (status != 200)
                logger.error("PeerManager.request: status code "+ status);

            // read content
            InputStream content = response.getEntity().getContent();
            BufferedReader rd = new BufferedReader(new InputStreamReader(content));
            StringBuilder result = new StringBuilder();
            String line;
            while ((line = rd.readLine()) != null)
                result.append(line);

            logger.info(result.toString());
            return result.toString();
        } catch (IOException e){
            logger.error("IOException " + e.getMessage());
            return "{}";
        }
    }

    @Override
    public CollectiveInfo getCollectiveInfo(Identifier collective) throws NoSuchCollectiveException {
        String response = getRequest("collectives/", collective.getId());
        if(response.contains("Cannot find a collective"))
            throw new NoSuchCollectiveException();

        response = response.replaceAll("(\\t|\\r|\\n|\\s|\\u00A0|\\u2007|\\u202F)+"," ");
        JSONObject content = new JSONObject(response);

        JSONArray collectedUsers = content.getJSONArray("collectedUsers");
        List<Identifier> peers = new ArrayList<>();
        for(int i = 0; i < collectedUsers.length(); i++){
            peers.add(Identifier.peer(collectedUsers.getString(i)));
        }

        CollectiveInfo info = new CollectiveInfo();
        info.setId(collective);
        info.setPeers(peers);
        info.setDeliveryPolicy(DeliveryPolicy.Collective.TO_ANY);

        return info;
    }

    @Override
    public boolean authenticate(Identifier peerId, String password) throws PeerAuthenticationException {
        return false;
    }

    @Override
    public PeerInfo getPeerInfo(Identifier id) throws NoSuchPeerException {
        String response = getRequest("askProfile/", id.getId());
        if(response.equals("{}"))
            throw new NoSuchPeerException(id);

        response = response.replaceAll("(\\t|\\r|\\n|\\s|\\u00A0|\\u2007|\\u202F)+"," ");
        JSONObject content = new JSONObject(response);

        //TODO: replace DeliveryPolicy.Peer policy = DeliveryPolicy.Peer.values()[content.getInt("deliveryPolicy")];
        DeliveryPolicy.Peer policy = DeliveryPolicy.Peer.AT_LEAST_ONE;
        List<PeerChannelAddress> addresses = parseDeliveryAddress(id, content.getJSONArray("deliveryAddress"));

        PeerInfo info = new PeerInfo();
        info.setId(id);
        info.setDeliveryPolicy(policy);
        info.setPrivacyPolicies(null);
        info.setAddresses(addresses);

        return info;
    }

    private List<PeerChannelAddress> parseDeliveryAddress(Identifier id, JSONArray array){
        JSONObject json;
        PeerChannelAddress address;
        List<PeerChannelAddress> addresses = new ArrayList<>();

        for(int i = 0; i < array.length(); i++){
            json = array.getJSONObject(i);
            address = new PeerChannelAddress();
            address.setPeerId(id);
            address.setChannelType(Identifier.channelType(json.getString("channelType")));
            address.setContactParameters(parseContactParams(json.getJSONArray("contactParams")));
            addresses.add(address);
        }

        return addresses;
    }

    private List<String> parseContactParams(JSONArray array){
        List<String> params = new ArrayList<>();

        for(int i = 0; i < array.length(); i++){
            params.add(array.getString(i));
        }

        return params;
    }
}
