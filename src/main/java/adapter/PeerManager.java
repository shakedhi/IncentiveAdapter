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
public class PeerManager implements PeerAuthenticationCallback, PeerInfoCallback, CollectiveInfoCallback {
    private static PeerManager peerManager;
    private HttpClient httpClient;
    private final String PM_URL = "http://elog.disi.unitn.it:8081/kos-smartsociety/smartsociety-peermanager";
    private final String GET_COLLECTIVE = "/collectives/";
    private final String GET_PEER = "/askProfile/";

    private PeerManager() {
        httpClient = HttpClientBuilder.create().build();
    }

    public static PeerManager instance() {
        if (peerManager == null)
            peerManager = new PeerManager();
        return peerManager;
    }

    private String getRequest(String requestType, String id){
        try{
            // send get request
            HttpGet request = new HttpGet(PM_URL + requestType + id);
            HttpResponse response = httpClient.execute(request);

            // check status code
            int status = response.getStatusLine().getStatusCode();
            if (status != 200)
                System.out.println("PeerManager.request: status code "+ status);

            // read content
            InputStream content = response.getEntity().getContent();
            BufferedReader rd = new BufferedReader(new InputStreamReader(content));
            StringBuilder result = new StringBuilder();
            String line;
            while ((line = rd.readLine()) != null)
                result.append(line);

            System.out.println(result.toString());
            return result.toString();
        } catch (IOException e){
            System.out.println("PeerManager.request: IOException " + e.getMessage());
            return "{}";
        }
    }

    @Override
    public CollectiveInfo getCollectiveInfo(Identifier collective) throws NoSuchCollectiveException {
        String response = getRequest(GET_COLLECTIVE, collective.getId());
        if(response.contains("Cannot find a collective"))
            throw new NoSuchCollectiveException();

        response = response.replaceAll("\\r\\n|\\r|\\n|\\s|\\u00A0|\\u2007|\\u202F","");
        JSONObject content = new JSONObject(response);

        JSONArray collectedUsers = content.getJSONArray("collectedUsers");
        List<Identifier> peers = new ArrayList<>();
        for(int i = 0; i < collectedUsers.length(); i++){
            peers.add(Identifier.peer(collectedUsers.getString(i)));
        }

        CollectiveInfo info = new CollectiveInfo();
        info.setId(collective);
        info.setPeers(peers);
        info.setDeliveryPolicy(DeliveryPolicy.Collective.TO_ALL_MEMBERS);

        return info;
    }

    @Override
    public boolean authenticate(Identifier peerId, String password) throws PeerAuthenticationException {
        return false;
    }

    @Override
    public PeerInfo getPeerInfo(Identifier id) throws NoSuchPeerException {
        String response = getRequest(GET_PEER, id.getId());
        if(response.equals("{}"))
            throw new NoSuchPeerException(id);

        response = response.replaceAll("\\r\\n|\\r|\\n|\\s|\\u00A0|\\u2007|\\u202F","");
        JSONObject content = new JSONObject(response);

        //TODO: fix DeliveryPolicy.Peer policy = DeliveryPolicy.Peer.values()[content.getInt("deliveryPolicy")];
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

    /*
    public void addPeer(){
        try {
            HttpPost request = new HttpPost("http://elog.disi.unitn.it:8081/kos-smartsociety/smartsociety-peermanager/person_peer");
            request.addHeader(new BasicHeader(HTTP.CONTENT_TYPE, "application/json"));
            request.setEntity(new StringEntity("{\n    \"firstname\": \"Shaked\",\n    \"lastname\": \"Hindi\",\n    \"gender\": \"male\",\n    \"username\": \"shakedhi\",\n    \"password\": \"12345678\"\n}"));
            HttpResponse response = httpClient.execute(request);

            HttpPost request2 = new HttpPost("http://elog.disi.unitn.it:8081/kos-smartsociety/smartsociety-peermanager/subscribeAsk_sl?username=shakedhi&password=12345678");
            request2.addHeader(new BasicHeader(HTTP.CONTENT_TYPE, "application/json"));
            request2.setEntity(new StringEntity("{\n    \"available\" : true,\n    \"deliveryPolicy\" : 0,\n    \"deliveryAddresses\" : [\n        \n        {\n            \"channelType\" : \"Email\",\n            \"contactParams\" : [\"shakedhi@post.bgu.ac.il\"]\n        }\n    ]\n}"));
            HttpResponse response2 = httpClient.execute(request2);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (ClientProtocolException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }*/
}
