package twitterManager;

/**
 * Used to determine if a tweet comes from a particular state
 * @author brett
 *
 */
public enum States {
	Alabama("Alabama", "AL"),
	Alaska("Alaska", "AK"),
	Arizona("Arizona", "AZ"),
	Arkansas("Arkansas", "AR"),
	California("California", "CA"),
	Colorado("Colorado", "CO"),
	Connecticut("Connecticut", "CT"),
	Delaware("Delaware", "DE"),
	Florida("Florida", "FL"),
	Georgia("Georgia", "GA"),
	Hawaii("Hawaii", "HI"),
	Idaho("Idaho", "ID"),
	Illinois("Illinois", "IL"),
	Indiana("Indiana", "IN"),
	Iowa("Iowa", "IA"),
	Kansas("Kansas", "KS"),
	Kentucky("Kentucky", "KY"),
	Louisiana("Louisiana", "LA"),
	Maine("Maine", "ME"),
	Maryland("Maryland", "MD"),
	Massachusetts("Massachusetts", "MA"),
	Michigan("Michigan", "MI"),
	Minnesota("Minnesota", "MN"),
	Mississippi("Mississippi", "MS"),
	Missouri("Missouri", "MO"),
	Montana("Montana", "MT"),
	Nebraska("Nebraska", "NE"),
	Nevada("Nevada", "NV"),
	NewHampshire("New Hampshire", "NH"),
	NewJersey("New Jersey", "NJ"),
	NewMexico("New Mexico", "NM"),
	NewYork("New York", "NY"),
	NorthCarolina("North Carolina", "NC"),
	NorthDakota("North Dakota", "ND"),
	Ohio("Ohio", "OH"),
	Oklahoma("Oklahoma", "OK"),
	Oregon("Oregon", "OR"),
	Pennsylvania("Pennsylvania", "PA"),
	RhodeIsland("Rhode Island", "RI"),
	SouthCarolina("South Carolina", "SC"),
	SouthDakota("South Dakota", "SD"),
	Tennessee("Tennessee", "TN"),
	Texas("Texas", "TX"),
	Utah("Utah", "UT"),
	Vermont("Vermont", "VT"),
	Virginia("Virginia", "VA"),
	Washington("Washington", "WA"),
	WestVirginia("West Virginia", "WV"),
	Wisconsin("Wisconsin", "WI"),
	Wyoming("Wyoming", "WY");
	
	private String state ;
	private String stateAbbr ;
	
	/**
	 * Constructor of above states
	 * @param state
	 * @param stateAbbr
	 */
	States(String state, String stateAbbr) {
        this.state = state;
        this.stateAbbr = stateAbbr;
    }

	/**
	 * Check if the input looks like it has a state in it
	 * @param input
	 * @return The state identified, null otherwise
	 */
    public static String IsState(String input) 
    {
    	String result = null;
    	
    	//cycle through all the states
    	for(States aState : States.values()) 
    	{
    		//check if state contains either the full state name or abbreviation 
    		//Potentially a lot of false positives here...
    		if(input.contains(aState.state) || input.contains(aState.stateAbbr)) {
    			result = aState.stateAbbr;
    			break;
    		}
    	}
    	return result;
    }
}
