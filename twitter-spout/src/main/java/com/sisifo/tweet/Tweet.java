package com.sisifo.tweet;


public class Tweet {

	/**
	 * campos que van directamente a bbdd
	 */
	private String created_at;
	private Integer favorite_count;
	private Long id;
	private Long in_reply_to_status_id;
	private Long in_reply_to_user_id;
	private Integer retweet_count;
	private Boolean retweeted;
	private Long retweeted_id;
	private String text;
	private Boolean truncated;
	
	/**
	 * relaciones y colecciones
	 */
	private TweetPlace place;
	private TweetUser user;	
	private JsonTweetEntity entities;

	public Tweet() {
		super();
	}
	
	public String getPlace_full_name() {
		if (place == null) {
			return null;
		}
		return place.getFull_name();
	}
	
	public Long getUser_id() {
		return user.getId();
	}
	

	
	
	// métodos generados
	
	public String getCreated_at() {
		return created_at;
	}

	public void setCreated_at(String created_at) {
		this.created_at = created_at;
	}

	public Integer getFavorite_count() {
		return favorite_count;
	}

	public void setFavorite_count(Integer favorite_count) {
		this.favorite_count = favorite_count;
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public Long getIn_reply_to_status_id() {
		return in_reply_to_status_id;
	}

	public void setIn_reply_to_status_id(Long in_reply_to_status_id) {
		this.in_reply_to_status_id = in_reply_to_status_id;
	}

	public Long getIn_reply_to_user_id() {
		return in_reply_to_user_id;
	}

	public void setIn_reply_to_user_id(Long in_reply_to_user_id) {
		this.in_reply_to_user_id = in_reply_to_user_id;
	}

	public Integer getRetweet_count() {
		return retweet_count;
	}

	public void setRetweet_count(Integer retweet_count) {
		this.retweet_count = retweet_count;
	}

	public Boolean getRetweeted() {
		return retweeted;
	}

	public void setRetweeted(Boolean retweeted) {
		this.retweeted = retweeted;
	}

	public Long getRetweeted_id() {
		return retweeted_id;
	}

	public void setRetweeted_id(Long retweeted_id) {
		this.retweeted_id = retweeted_id;
	}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}

	public Boolean getTruncated() {
		return truncated;
	}

	public void setTruncated(Boolean truncated) {
		this.truncated = truncated;
	}

	public TweetPlace getPlace() {
		return place;
	}

	public void setPlace(TweetPlace place) {
		this.place = place;
	}

	public TweetUser getUser() {
		return user;
	}

	public void setUser(TweetUser user) {
		this.user = user;
	}

	public JsonTweetEntity getEntities() {
		return entities;
	}

	public void setEntities(JsonTweetEntity entities) {
		this.entities = entities;
	}

	
}
