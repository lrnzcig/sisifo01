package com.sisifo.twitter_model.utils;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.QuoteMode;

public class WriterUtils {

	/**
	 * creates file name using the query and adding the .csv suffix
	 * 
	 * @param defaultName
	 * @param fileSuffix
	 * @return
	 */
	public static String addSuffixAndExtension(String defaultName,
			String fileSuffix) {
		String fileName = defaultName;
		if (fileSuffix != null) {
			fileName += fileSuffix.replaceAll("[^a-zA-Z0-9]+","_");
		}
		return fileName + ".csv";
	}

	public static CSVFormat getCSVFormat() {
		return CSVFormat.DEFAULT.withDelimiter(';').withQuote('\'').withQuoteMode(QuoteMode.NON_NUMERIC);
	}
}
