package adapter;

/**
 * @author Shaked Hindi
 */
public class Classification {
    private String project;
    private String user_id;
    private Geographic geo;
    private String subjects;
    private String created_at;

    Classification(String user, String city, String country, String subj, String created){
        this.project = "SmartSociety";
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
