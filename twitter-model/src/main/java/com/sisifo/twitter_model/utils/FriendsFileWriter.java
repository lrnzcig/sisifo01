package com.sisifo.twitter_model.utils;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.QuoteMode;

public class FriendsFileWriter {


	private CSVFormat csvFormat;
	
	private static final String FRIENDS_FILE_DEFAULT_NAME = "friends";
	private static final Object[] FRIENDS_HEADER = {"user_id",
		"followed_user_id"};
	private FileWriter friendsFileWriter;
	private CSVPrinter friendsCsvPrinter;


	public FriendsFileWriter() {
		this(null);
	}
	
	public FriendsFileWriter(String query) {
		super();
		csvFormat = CSVFormat.DEFAULT.withDelimiter(';').withQuote('\'').withQuoteMode(QuoteMode.NON_NUMERIC);
		createFiles(query);
	}

	private void createFiles(String fileSuffix) {
		try {
			String fileName = addSuffixAndExtension(FRIENDS_FILE_DEFAULT_NAME, fileSuffix);
			friendsFileWriter = new FileWriter(fileName);
			friendsCsvPrinter = new CSVPrinter(friendsFileWriter, csvFormat);
	    	friendsCsvPrinter.printRecord(FRIENDS_HEADER);
	    	
		} catch (IOException e) {
			throw new RuntimeException(e);
		}		
	}
	
	private String addSuffixAndExtension(String defaultName,
			String fileSuffix) {
		String fileName = defaultName;
		if (fileSuffix != null) {
			fileName += fileSuffix.replaceAll("[^a-zA-Z0-9]+","_");
		}
		return fileName + ".csv";
	}


	public void writeToFile(Long userId, Set<Long> ids) {
		try {
			List<List<Object>> output = new ArrayList<List<Object>>();
			for (Long followedId : ids) {
				List<Object> record = new ArrayList<Object>();
				record.add(userId);
				record.add(followedId);
				output.add(record);
			}
			friendsCsvPrinter.printRecords(output);
		} catch (IOException e) {
			close();
			throw new RuntimeException(e);
		}
	}


	public void close() {
		flush();
        try {
        	friendsFileWriter.close();
            friendsCsvPrinter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
	}


	public void flush() {
        try {
        	friendsFileWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
	}
	
	

}
