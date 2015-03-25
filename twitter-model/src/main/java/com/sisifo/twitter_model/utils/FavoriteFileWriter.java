package com.sisifo.twitter_model.utils;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import com.sisifo.twitter_model.tweet.Tweet;

public class FavoriteFileWriter {

	private CSVFormat csvFormat;
	
	private static final String FAV_TWEET_FILE_DEFAULT_NAME = "favorite_tweet";
	// see also @TweetWriterUtils
	private FileWriter favTweetFileWriter;
	private CSVPrinter favTweetCsvPrinter;

	private static final String FAVORITE_FILE_DEFAULT_NAME = "favorite";
	private static final Object[] FAVORITE_HEADER = {"user_id",
		"favorite_tweet_id"};
	private FileWriter favoriteFileWriter;
	private CSVPrinter favoriteCsvPrinter;


	public FavoriteFileWriter() {
		this(null);
	}
	
	public FavoriteFileWriter(String query) {
		super();
		csvFormat = WriterUtils.getCSVFormat();
		createFiles(query);
	}

	private void createFiles(String fileSuffix) {
		try {
			String fileName = WriterUtils.addSuffixAndExtension(FAVORITE_FILE_DEFAULT_NAME, fileSuffix);
			favoriteFileWriter = new FileWriter(fileName);
			favoriteCsvPrinter = new CSVPrinter(favoriteFileWriter, csvFormat);
	    	favoriteCsvPrinter.printRecord(FAVORITE_HEADER);
	    	
	    	fileName = WriterUtils.addSuffixAndExtension(FAV_TWEET_FILE_DEFAULT_NAME, fileSuffix);
			favTweetFileWriter = new FileWriter(fileName);
			favTweetCsvPrinter = new CSVPrinter(favTweetFileWriter, csvFormat);
	    	favTweetCsvPrinter.printRecord(FAVORITE_HEADER);
	    	
		} catch (IOException e) {
			throw new RuntimeException(e);
		}		
	}
	

	public void writeToFile(Long userId, Tweet[] tweets) {
		if (tweets == null) {
			return;
		}
		try { 
			List<List<Object>> favoriteRecord = new ArrayList<List<Object>>();
			for (Tweet tweet : tweets) {
				List<Object> record = new ArrayList<Object>();
				record.add(userId);
				record.add(tweet.getId());
				favoriteRecord.add(record);
				favTweetCsvPrinter.printRecord(TweetWriterUtils.getTweetRecord(tweet));
			}
			favoriteCsvPrinter.printRecords(favoriteRecord);
		} catch (IOException e) {
			close();
			throw new RuntimeException(e);
		}
	}


	public void close() {
		flush();
        try {
        	favoriteFileWriter.close();
            favoriteCsvPrinter.close();
        	favTweetFileWriter.close();
            favTweetCsvPrinter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
	}


	public void flush() {
        try {
        	favoriteFileWriter.flush();
        	favTweetFileWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
	}

}
