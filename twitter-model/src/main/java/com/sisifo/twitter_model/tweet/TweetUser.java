package com.sisifo.twitter_model.tweet;

import javax.xml.bind.annotation.XmlRootElement;

import com.google.gson.annotations.SerializedName;

@XmlRootElement
public class TweetUser {

	/**
	 * campos que van directamente a bbdd
	 */
	private boolean contributors_enabled;
	private String created_at;
	private String description;
	private int favourites_count;
	private int followers_count;
	private int friends_count;
	private Long id;
	private boolean is_translator;
	private Integer listed_count;
	private String location;
	private String name;
	@SerializedName("protected") private boolean protectedFlag;
	private String screen_name;
	private Integer statuses_count;
	private String url;
	private boolean verified;
	private boolean withheld;

	// colecciones
	private JsonTweetEntity entities;
	
	public TweetUser() {
		super();
	}

	
	// métodos generados

	public boolean isContributors_enabled() {
		return contributors_enabled;
	}

	public void setContributors_enabled(boolean contributors_enabled) {
		this.contributors_enabled = contributors_enabled;
	}

	public String getCreated_at() {
		return created_at;
	}

	public void setCreated_at(String created_at) {
		this.created_at = created_at;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public int getFavourites_count() {
		return favourites_count;
	}

	public void setFavourites_count(int favourites_count) {
		this.favourites_count = favourites_count;
	}

	public int getFollowers_count() {
		return followers_count;
	}

	public void setFollowers_count(int followers_count) {
		this.followers_count = followers_count;
	}

	public int getFriends_count() {
		return friends_count;
	}

	public void setFriends_count(int friends_count) {
		this.friends_count = friends_count;
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public boolean isIs_translator() {
		return is_translator;
	}

	public void setIs_translator(boolean is_translator) {
		this.is_translator = is_translator;
	}

	public Integer getListed_count() {
		return listed_count;
	}

	public void setListed_count(Integer listed_count) {
		this.listed_count = listed_count;
	}

	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public boolean isProtectedFlag() {
		return protectedFlag;
	}

	public void setProtectedFlag(boolean protectedFlag) {
		this.protectedFlag = protectedFlag;
	}

	public String getScreen_name() {
		return screen_name;
	}

	public void setScreen_name(String screen_name) {
		this.screen_name = screen_name;
	}

	public Integer getStatuses_count() {
		return statuses_count;
	}

	public void setStatuses_count(Integer statuses_count) {
		this.statuses_count = statuses_count;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public boolean isVerified() {
		return verified;
	}

	public void setVerified(boolean verified) {
		this.verified = verified;
	}

	public boolean isWithheld() {
		return withheld;
	}

	public void setWithheld(boolean withheld) {
		this.withheld = withheld;
	}

	public JsonTweetEntity getEntities() {
		return entities;
	}

	public void setEntities(JsonTweetEntity entities) {
		this.entities = entities;
	}


}
